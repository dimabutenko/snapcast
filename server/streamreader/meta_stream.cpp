/***
    This file is part of snapcast
    Copyright (C) 2014-2025  Johannes Pohl

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
***/

// prototype/interface header file
#include "meta_stream.hpp"

// local headers
#include "common/aixlog.hpp"
#include "common/snap_exception.hpp"
#include "common/utils/string_utils.hpp"



#include <algorithm>
using namespace std;

namespace streamreader
{

static constexpr auto LOG_TAG = "MetaStream";
static constexpr double kDuckingVolume = 0.2; // Volume when ducked

MetaStream::MetaStream(PcmStream::Listener* pcmListener, const std::vector<std::shared_ptr<PcmStream>>& streams, boost::asio::io_context& ioc,
                       const ServerSettings& server_settings, const StreamUri& uri, PcmStream::Source source)
    : PcmStream(pcmListener, ioc, server_settings, uri, source)
{
    auto path_components = utils::string::split(uri.path, '/');
    for (const auto& component : path_components)
    {
        if (component.empty())
            continue;

        bool found = false;
        for (const auto& stream : streams)
        {
            if (stream->getName() == component)
            {
                streams_.push_back(stream);
                stream->addListener(this);

                auto state = make_unique<StreamState>();
                state->stream = stream;
                // Resampler will be initialized when format is known/changed
                state->resampler = make_unique<Resampler>(stream->getSampleFormat(), sampleFormat_);
                stream_states_[stream.get()] = std::move(state);

                found = true;
                break;
            }
        }
        if (!found)
            throw SnapException("Unknown stream: \"" + component + "\"");
    }

    if (streams_.empty())
        throw SnapException("Meta stream '" + getName() + "' must contain at least one stream");
}


MetaStream::~MetaStream()
{
    stop(); // NOLINT
}


void MetaStream::start()
{
    LOG(DEBUG, LOG_TAG) << "Start, sampleformat: " << sampleFormat_.toString() << "\n";
    PcmStream::start();
}


void MetaStream::stop()
{
    active_ = false;
}


void MetaStream::onPropertiesChanged(const PcmStream* pcmStream, const Properties& properties)
{
    LOG(DEBUG, LOG_TAG) << "onPropertiesChanged: " << pcmStream->getName() << "\n";
    std::lock_guard<std::recursive_mutex> lock(active_mutex_);
    
    // We only expose properties of the primary stream, or maybe the first one?
    // For now, let's keep the logic simple: if it's the "master", update properties.
    // Ideally MetaStream properties are aggregation.
    
    // Check if this stream is the current master
    bool is_master = false;
    for (const auto& stream : streams_)
    {
        if (stream->getState() == ReaderState::kPlaying)
        {
            if (stream.get() == pcmStream)
                is_master = true;
            break;
        }
    }

    if (is_master)
        setProperties(properties);
}


void MetaStream::onStateChanged(const PcmStream* pcmStream, ReaderState state)
{
    LOG(DEBUG, LOG_TAG) << "onStateChanged: " << pcmStream->getName() << ", state: " << state << "\n";
    std::lock_guard<std::recursive_mutex> lock(active_mutex_);

    if (stream_states_.find(pcmStream) != stream_states_.end())
    {
        stream_states_[pcmStream]->active = (state == ReaderState::kPlaying);
    }
    
    checkState();
}

void MetaStream::checkState()
{
    // Determine overall state
    ReaderState new_state = ReaderState::kIdle;
    for (const auto& stream : streams_)
    {
        if (stream->getState() == ReaderState::kPlaying)
        {
            new_state = ReaderState::kPlaying;
            break;
        }
    }
    
    if (new_state != state_)
        setState(new_state);
}


void MetaStream::onChunkRead(const PcmStream* pcmStream, const msg::PcmChunk& chunk)
{
    std::lock_guard<std::recursive_mutex> lock(active_mutex_);
    
    auto it = stream_states_.find(pcmStream);
    if (it == stream_states_.end())
        return;

    StreamState& state = *it->second;

    if (state.resampler && state.resampler->resamplingNeeded())
    {
        auto resampled_chunk = state.resampler->resample(chunk);
        if (resampled_chunk)
             state.buffer.push_back(*resampled_chunk);
    }
    else
    {
        state.buffer.push_back(chunk);
    }

    mixChunks();
}

void MetaStream::mixChunks()
{
    // Find Master Stream (Highest Priority Playing Stream)
    StreamState* master_state = nullptr;
    const PcmStream* master_stream = nullptr;

    for (const auto& stream : streams_)
    {
        auto it = stream_states_.find(stream.get());
        if (it != stream_states_.end() && it->second->active)
        {
            master_state = it->second.get();
            master_stream = stream.get();
            break;
        }
    }

    if (!master_state)
    {
        // No active stream, clear buffers to avoid overflow? or just return
        return;
    }

    // Process all chunks available in Master
    while (!master_state->buffer.empty())
    {
        auto& master_chunk = master_state->buffer.front();
        
        // Prepare output chunk (copy of master)
        // Apply ducking/volume to master if needed (usually 1.0)
        double master_vol = getDuckingVolume(master_stream);
        
        // Modify master chunk volume in place? or copy?
        // PcmChunk owns vector<char>.
        // We need to mix in implementation.
        // Assuming 16-bit PCM for now (standard for Snapcast internal)
        // But sampleFormat_ can be anything.
        
        // MIXING implementation is complex without helper.
        // For Proof of Concept / Task:
        // We will just perform summation for int16.
        // TODO: Handle other formats.

        if (sampleFormat_.bits() != 16) 
        {
             // Fallback: Just forward master if not 16 bit (mixing not impl)
             chunkRead(master_chunk); // Raw forward
             master_state->buffer.pop_front();
             continue;
        }

        // Mix other streams
        // We need to iterate others
        
        // Create a working buffer from master
        // Ideally we shouldn't modify the buffer in deque if we want to keep it "clean" but we are popping it.
        
        // Apply volume to master
        int16_t* pcm_out = reinterpret_cast<int16_t*>(master_chunk.payload);
        size_t frame_count = master_chunk.getFrameCount();
        size_t channels = sampleFormat_.channels();
        size_t sample_count = frame_count * channels;

        if (master_vol < 0.99)
        {
            for (size_t i = 0; i < sample_count; ++i)
                pcm_out[i] = static_cast<int16_t>(pcm_out[i] * master_vol);
        }

        for (const auto& stream : streams_)
        {
            if (stream.get() == master_stream) continue;
            
            auto it = stream_states_.find(stream.get());
            if (it == stream_states_.end() || !it->second->active) continue;
            
            StreamState& other = *it->second;
            if (other.buffer.empty()) continue; // drift/underrun
            
            auto& other_chunk = other.buffer.front();
            // Assuming chunks aligned by duration/size because of resampler.
            // But they might not match exactly.
            // Take min length?
            
            int16_t* pcm_in = reinterpret_cast<int16_t*>(other_chunk.payload);
            size_t other_count = other_chunk.getFrameCount() * channels; // assume same channels
            size_t mix_count = std::min(sample_count, other_count);
            
            double other_vol = getDuckingVolume(stream.get());
            
            for (size_t i = 0; i < mix_count; ++i)
            {
                int32_t mixed = pcm_out[i] + static_cast<int32_t>(pcm_in[i] * other_vol);
                // Hard clipping
                if (mixed > 32767) mixed = 32767;
                if (mixed < -32768) mixed = -32768;
                pcm_out[i] = static_cast<int16_t>(mixed);
            }
            
            // Pop from other buffer? 
            // Only if we consumed it. simple 1:1 consumption for now.
            other.buffer.pop_front();
        }

        chunkRead(master_chunk);
        master_state->buffer.pop_front();
    }
}

double MetaStream::getDuckingVolume(const PcmStream* stream)
{
    // Iterate streams. If we find an active stream BEFORE 'stream', then 'stream' is ducked.
    for (const auto& s : streams_)
    {
        if (s.get() == stream) return 1.0; // Reached self, no higher prio active
        
        if (s->getState() == ReaderState::kPlaying)
        {
            return kDuckingVolume; // Found higher prio active
        }
    }
    return 1.0;
}


void MetaStream::onChunkEncoded(const PcmStream* pcmStream, std::shared_ptr<msg::PcmChunk> chunk, double duration)
{
    std::ignore = pcmStream;
    std::ignore = chunk;
    std::ignore = duration;
}


void MetaStream::onResync(const PcmStream* pcmStream, double ms)
{
    LOG(DEBUG, LOG_TAG) << "onResync: " << pcmStream->getName() << ", duration: " << ms << " ms\n";
    std::lock_guard<std::recursive_mutex> lock(active_mutex_);
    
    // Propagate resync if it comes from master?
    bool is_master = false;
    for (const auto& stream : streams_) {
        if (stream->getState() == ReaderState::kPlaying) {
            if (stream.get() == pcmStream) is_master = true;
            break;
        }
    }

    if (is_master)
        resync(std::chrono::nanoseconds(static_cast<int64_t>(ms * 1000000)));
}



// Setter for properties
void MetaStream::setShuffle(bool shuffle, ResultHandler&& handler)
{
    std::lock_guard<std::recursive_mutex> lock(mutex_); // PcmStream mutex
    // Forward to all or master?
    // Current impl forwards to active. Forward to Master.
    for (auto& s : streams_) s->setShuffle(shuffle, nullptr);
    handler(snapcast::ErrorCode());
}

void MetaStream::setLoopStatus(LoopStatus status, ResultHandler&& handler)
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    for (auto& s : streams_) s->setLoopStatus(status, nullptr);
    handler(snapcast::ErrorCode());
}

void MetaStream::setVolume(uint16_t volume, ResultHandler&& handler)
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    for (auto& s : streams_) s->setVolume(volume, nullptr);
    handler(snapcast::ErrorCode());
}

void MetaStream::setMute(bool mute, ResultHandler&& handler)
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    for (auto& s : streams_) s->setMute(mute, nullptr);
    handler(snapcast::ErrorCode());
}

void MetaStream::setRate(float rate, ResultHandler&& handler)
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    for (auto& s : streams_) s->setRate(rate, nullptr);
    handler(snapcast::ErrorCode());
}


// Control commands
void MetaStream::setPosition(std::chrono::milliseconds position, ResultHandler&& handler)
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    // ambiguous for meta stream. send to all?
    for (auto& s : streams_) s->setPosition(position, nullptr);
     handler(snapcast::ErrorCode());
}

void MetaStream::seek(std::chrono::milliseconds offset, ResultHandler&& handler)
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    for (auto& s : streams_) s->seek(offset, nullptr);
    handler(snapcast::ErrorCode());
}

void MetaStream::next(ResultHandler&& handler)
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    // typically next track. Send to master?
     for (auto& s : streams_) s->next(nullptr);
     handler(snapcast::ErrorCode());
}

void MetaStream::previous(ResultHandler&& handler)
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
     for (auto& s : streams_) s->previous(nullptr);
     handler(snapcast::ErrorCode());
}

void MetaStream::pause(ResultHandler&& handler)
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
     for (auto& s : streams_) s->pause(nullptr);
     handler(snapcast::ErrorCode());
}

void MetaStream::playPause(ResultHandler&& handler)
{
    LOG(DEBUG, LOG_TAG) << "PlayPause\n";
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    // Broadcast
    for (auto& s : streams_) s->playPause(nullptr);
    handler(snapcast::ErrorCode());
}

void MetaStream::stop(ResultHandler&& handler)
{
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    for (auto& s : streams_) s->stop(nullptr);
    handler(snapcast::ErrorCode());
}

void MetaStream::play(ResultHandler&& handler)
{
    LOG(DEBUG, LOG_TAG) << "Play\n";
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    for (auto& s : streams_) s->play(nullptr);
    handler(snapcast::ErrorCode());
}


} // namespace streamreader
