//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <algorithm>
#include <latch>
#include <list>
#include <locale>
#include <mutex>
#include <unordered_map>

#include "common/config.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  std::lock_guard<std::shared_mutex> lock(latch_);
  Page *page;
  auto it_page = page_table_.find(page_id);

  // 1.1    If P exists, pin it and return it immediately.
  if (it_page != page_table_.end()) {
    page = &pages_[it_page->second];
    if (page->pin_count_++ == 0) {
      replacer_->Pin(page_id);
    }
    return page;
  }

  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  frame_id_t frame_id = this->GetVictimFrameId();
  if (frame_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  // 2.     If R is dirty, write it back to the disk.
  page = &pages_[frame_id];
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
  }

  // 3.     Delete R from the page table and insert P.
  page_table_.erase(page->GetPageId());
  page_table_[page_id] = frame_id;

  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  disk_manager_->ReadPage(page_id, page->data_);
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  return nullptr;
}

auto BufferPoolManager::GetVictimFrameId() -> frame_id_t {
  frame_id_t frame_id;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Victim(&frame_id)) {
    return INVALID_PAGE_ID;
  }

  return frame_id;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::shared_mutex> lock(latch_);
  auto it_page = page_table_.find(page_id);

  if (it_page == page_table_.end()) {
    return false;
  }

  Page *page = &pages_[it_page->second];
  if (page->pin_count_ <= 0) {
    return false;
  }

  page->is_dirty_ |= is_dirty;
  if (--page->pin_count_ == 0) {
    replacer_->Pin(it_page->second);
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  auto it_page = page_table_.find(page_id);
  if (it_page == page_table_.end()) {
    return false;
  }

  std::lock_guard<std::shared_mutex> lock(latch_);
  Page *page = &pages_[it_page->second];

  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }

  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  std::lock_guard<std::shared_mutex> lock(latch_);
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  if (free_list_.empty() && replacer_->Size() == 0) {
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }

  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t frame_id = GetVictimFrameId();
  if (frame_id == INVALID_PAGE_ID) {
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  Page* page = &pages_[frame_id];
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
  }

  *page_id = disk_manager_->AllocatePage();
  page_table_.erase(page->page_id_);
  page_table_[*page_id] = frame_id;
  
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = true;
  
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  std::lock_guard<std::shared_mutex> lock(latch_);
  // 1.   Search the page table for the requested page (P).
  auto it_page = page_table_.find(page_id);
  // 1.   If P does not exist, return true.
  if (it_page == page_table_.end()){
    return true;
  }
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  Page *page = &pages_[it_page->second];
  if (page->pin_count_ != 0) {
    return false;
  }
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  disk_manager_->DeallocatePage(page_id);
  page_table_.erase(page_id);
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  free_list_.emplace_back(it_page->second);
  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (auto it_page : page_table_) {
    FlushPageImpl(it_page.first);
  }
}

}  // namespace bustub
