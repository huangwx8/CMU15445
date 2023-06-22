//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) : frame_infos_(num_pages, ClockReplacerFrameInfo()) {}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) 
{
    mutex_.lock();

    if (size_ == 0) 
    {
        mutex_.unlock();
        return false;
    }

    if (FindVictim(frame_id))
    {
        frame_infos_[*frame_id].free = false;
        size_--;
        mutex_.unlock();
        return true;
    }
    else 
    {
        mutex_.unlock();
        return false;
    }
}

void ClockReplacer::Pin(frame_id_t frame_id) 
{
    mutex_.lock();

    if (!frame_infos_[frame_id].free)
    {
        mutex_.unlock();
        return;
    }

    frame_infos_[frame_id].free = false;
    size_--;

    mutex_.unlock();
}

void ClockReplacer::Unpin(frame_id_t frame_id) 
{
    mutex_.lock();

    if (frame_infos_[frame_id].free)
    {
        mutex_.unlock();
        return;
    }

    frame_infos_[frame_id].free = true;
    frame_infos_[frame_id].ref = true;
    size_++;

    mutex_.unlock();
}

size_t ClockReplacer::Size() 
{
    return size_;
}

bool ClockReplacer::FindVictim(frame_id_t *frame_id) 
{
    for (;;)
    {
        if (frame_infos_[clock_hand_].free)
        {
            if (frame_infos_[clock_hand_].ref)
            {
                frame_infos_[clock_hand_].ref = false;
            }
            else 
            {
                *frame_id = clock_hand_;
                Step();
                return true;
            }
        }

        Step();
    }

    return false;
}

void ClockReplacer::Step() 
{
    clock_hand_ = (clock_hand_ + 1) % (frame_infos_.size());
}

}  // namespace bustub
