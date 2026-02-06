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
static constexpr double kDuckingVolume = 0.25; // Volume when ducked

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
    LOG(INFO, LOG_TAG) << "Start, sampleformat: " << sampleFormat_.toString() << "\n";
    tvEncodedChunk_ = std::chrono::steady_clock::now();
    first_ = true;
    PcmStream::start();
    
    // Sync initial state
    std::lock_guard<std::recursive_mutex> lock(active_mutex_);
    for (const auto& stream : streams_)
    {
        if (stream_states_.find(stream.get()) != stream_states_.end())
        {
             ReaderState state = stream->getState();
             stream_states_[stream.get()]->active = (state == ReaderState::kPlaying);
             LOG(INFO, LOG_TAG) << "Initial state for " << stream->getName() << ": " << state << "\n";
        }
    }
    checkState();
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
    LOG(INFO, LOG_TAG) << "onStateChanged: " << pcmStream->getName() << ", state: " << state << "\n";
    std::lock_guard<std::recursive_mutex> lock(active_mutex_);

    auto it = stream_states_.find(pcmStream);
    if (it != stream_states_.end())
    {
        it->second->active = (state == ReaderState::kPlaying);
        LOG(INFO, LOG_TAG) << "Set active state for " << pcmStream->getName() << " to " << it->second->active << "\n";
    }
    else
    {
        LOG(WARNING, LOG_TAG) << "onStateChanged for unknown stream: " << pcmStream->getName() << "\n";
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
    {
        if (new_state == ReaderState::kPlaying)
            first_ = true;
        setState(new_state);
    }
}

void MetaStream::onChunkRead(const PcmStream* pcmStream, const msg::PcmChunk& chunk)
{
    std::lock_guard<std::recursive_mutex> lock(active_mutex_);
    LOG(INFO, LOG_TAG) << "onChunkRead from " << pcmStream->getName() << ", frames: " << chunk.getFrameCount() << "\n";
    
    auto it = stream_states_.find(pcmStream);
    if (it == stream_states_.end())
    {
        LOG(WARNING, LOG_TAG) << "Received chunk from unknown stream: " << pcmStream->getName() << "\n";
        return;
    }

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
    // Synchronization Logic:
    // To prevent "Double Speed" and buffer bloat, we lock the output rate to a SINGLE "Clock Master".
    // We choose the LOWEST Priority active stream (last in list, e.g. Music) as the Master.
    // We ONLY output when this Master has data. High priority streams are mixed in.
    
    StreamState* master_state = nullptr;
    const PcmStream* master_stream = nullptr;

    // Iterate in reverse to find lowest priority active stream
    for (auto it = streams_.rbegin(); it != streams_.rend(); ++it)
    {
        auto state_it = stream_states_.find(it->get());
        if (state_it != stream_states_.end() && state_it->second->active)
        {
            master_state = state_it->second.get();
            master_stream = it->get();
            break; // Found the lowest priority active stream. Stop.
        }
    }

    if (!master_state)
    {
        // No active streams.
        return;
    }

    // Critical Sync Logic:
    // If the Clock Master has no data, we MUST WAIT.
    // Do NOT fallback to another stream (that would double-clock).
    if (master_state->buffer.empty())
    {
        return;
    }
    
    if (master_state->buffer.size() > 0)
    {
        if (first_)
        {
            first_ = false;
            tvEncodedChunk_ = std::chrono::steady_clock::now();
            LOG(INFO, LOG_TAG) << "First chunk mixed, resetting timestamp to now\n";
        }
    }

    // Process all chunks available in Master
    while (!master_state->buffer.empty())
    {
        auto& master_chunk = master_state->buffer.front();
        
        if (sampleFormat_.bits() != 16) 
        {
             // Fallback for non-16-bit: Just forward master
             chunkRead(master_chunk); 
             master_state->buffer.pop_front();
             continue;
        }

        // Prepare output chunk
        int16_t* pcm_out = reinterpret_cast<int16_t*>(master_chunk.payload);
        size_t frame_count = master_chunk.getFrameCount();
        size_t channels = sampleFormat_.channels();
        size_t sample_count = frame_count * channels;

        // --- Fading Logic Start ---
        // Calculate Target Volume for Master (Music)
        // Check if we should be ducked by any active, non-silent high priority stream
        double target_vol = getDuckingVolume(master_stream); // Returns 0.5 if needed, 1.0 otherwise

        // Fade Speed: 1.0 seconds to change 0.75 volume (1.0 -> 0.25).
        // Chunk is 20ms. 1.0s / 0.02s = 50 chunks.
        // Step = 0.75 / 50 = 0.015.
        constexpr double kFadeStep = 0.015;

        // Update current volume
        double& current_vol = master_state->current_volume;
        if (current_vol > target_vol)
            current_vol = std::max(target_vol, current_vol - kFadeStep);
        else if (current_vol < target_vol)
            current_vol = std::min(target_vol, current_vol + kFadeStep);

        // Apply volume if not full
        if (current_vol < 0.99)
        {
            for (size_t i = 0; i < sample_count; ++i)
                pcm_out[i] = static_cast<int16_t>(pcm_out[i] * current_vol);
        }
        // --- Fading Logic End ---

        // Mix in other streams
        for (const auto& stream : streams_)
        {
            // Skip master
            if (stream.get() == master_state->stream.get()) continue;
            
            auto it = stream_states_.find(stream.get());
            if (it == stream_states_.end() || !it->second->active) continue;
            
            StreamState& other = *it->second;
            if (other.buffer.empty()) continue; 
            
            auto& other_chunk = other.buffer.front();
            int16_t* pcm_in = reinterpret_cast<int16_t*>(other_chunk.payload);
            size_t other_count = other_chunk.getFrameCount() * channels; // assume same channels
            size_t mix_count = std::min(sample_count, other_count);
            
            // Full volume mix for others (Notification)
            for (size_t i = 0; i < mix_count; ++i)
            {
                int32_t mixed = pcm_out[i] + static_cast<int32_t>(pcm_in[i]); 
                if (mixed > 32767) mixed = 32767;
                if (mixed < -32768) mixed = -32768;
                pcm_out[i] = static_cast<int16_t>(mixed);
            }
            
            // Consume from other buffer
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
        
        // Only duck if the higher priority stream is Playing AND has buffered data AND is not Silent
        auto it = stream_states_.find(s.get());
        if (s->getState() == ReaderState::kPlaying && it != stream_states_.end() && !it->second->buffer.empty())
        {
             // Check silence
             const auto& chunk = it->second->buffer.front();
             bool is_silent = true;
             const uint64_t* ptr = reinterpret_cast<const uint64_t*>(chunk.payload);
             size_t len = chunk.payloadSize / 8;
             for (size_t i = 0; i < len; ++i) {
                 if (ptr[i] != 0) {
                     is_silent = false;
                     break;
                 }
             }

             if (!is_silent)
                return kDuckingVolume; // Found higher prio active WITH data and NOT silent
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
