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

#include <cstddef>
#include <mutex>
#include <shared_mutex>

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  for (size_t i{0}; i < num_pages; i++) {
    frames_.emplace_back(0, 0);
  }
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  if (this->Size() == 0) {
    return false;
  }

  std::lock_guard<std::shared_mutex> lock(mutex_);
  while (true) {
    auto &[contains, ref] = frames_[clock_hand_];
    if (contains == 1) {
      if (ref == 1) {
        ref = 0;
      } else {
        contains = 0;
        *frame_id = clock_hand_;
        return true;
      }
    }
    clock_hand_ = (clock_hand_ + 1) % static_cast<int>(frames_.size());
  }
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  auto &[contains, ref] = frames_[frame_id];
  contains = 0;
  ref = 0;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  auto &[contains, ref] = frames_[frame_id];
  contains = 1;
  ref = 1;
}

size_t ClockReplacer::Size() {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  size_t out{0};
  for (auto &[contains, ref] : frames_) {
    out += contains;
  }
  return out;
}
}  // namespace bustub
