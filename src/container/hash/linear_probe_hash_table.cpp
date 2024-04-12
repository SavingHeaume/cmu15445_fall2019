//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/hash/linear_probe_hash_table.h"

#include <cstddef>
#include <iostream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/hash_table_header_page.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {

/* 根据用户指定的 num_buckets （也就是 slot 的数量）分配 Page，
 * 最后一个 Page 的 slot 数量可能少于前面的 Page。
 * 还将每个 HashTableBlockPage 对应的 page_id 保存到 page_ids_ 成员里面了
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      hash_fn_(std::move(hash_fn)),
      num_buckets_(num_buckets),
      num_pages_((num_buckets - 1) / BLOCK_ARRAY_SIZE + 1),
      last_block_array_size_(num_buckets - (num_pages_ - 1) * BLOCK_ARRAY_SIZE) {
  auto *page = buffer_pool_manager_->NewPage(&header_page_id_);
  page->WLatch();

  auto *head = reinterpret_cast<HashTableHeaderPage *>(page->GetData());
  InitHeaderPage(head);

  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::InitHeaderPage(HashTableHeaderPage *header_page) {
  header_page->SetPageId(header_page_id_);
  header_page->SetSize(num_buckets_);

  page_ids_.clear();
  for (size_t i{0}; i < num_pages_; i++) {
    page_id_t page_id;

    buffer_pool_manager_->NewPage(&page_id);
    buffer_pool_manager_->UnpinPage(page_id, false);

    header_page->AddBlockPageId(page_id);
    page_ids_.emplace_back(page_id);
  }
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();

  // 根据关键字获取槽索引、块页索引和桶索引
  auto [slot_index, block_index, bucket_index] = this->GetIndex(key);

  // 获取包含关键字的块页面
  auto raw_block_page = buffer_pool_manager_->FetchPage(page_ids_[block_index]);
  raw_block_page->RLatch();
  auto block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(raw_block_page->GetData());

  // 线性探测
  while (block_page->IsOccupied(bucket_index)) {
    if (block_page->IsReadable(bucket_index) && !comparator_(key, block_page->KeyAt(bucket_index))) {
      result->emplace_back(block_page->ValueAt(bucket_index));
    }

    StepForward(bucket_index, block_index, raw_block_page, block_page, LockType::READ);

    // 如果返回到原始位置，则中断循环
    if (block_index * BLOCK_ARRAY_SIZE + bucket_index == slot_index) {
      break;
    }
  }

  raw_block_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(raw_block_page->GetPageId(), false);
  table_latch_.RUnlock();

  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetIndex(const KeyType &key) -> std::tuple<size_t, size_t, slot_offset_t> {
  size_t slot_index = this->hash_fn_.GetHash(key) % num_buckets_;
  size_t block_index = slot_index / BLOCK_ARRAY_SIZE;
  slot_offset_t bucket_index = slot_index % BLOCK_ARRAY_SIZE;

  return {slot_index, block_index, bucket_index};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::StepForward(slot_offset_t &bucket_index, size_t &block_index, Page *&raw_block_page,
                                  HASH_TABLE_BLOCK_TYPE *&block_page, LockType lockType) {
  if (++bucket_index != this->GetBlockArraySize(block_index)) {
    return;
  }

  if (lockType == LockType::READ) {
    raw_block_page->RUnlatch();
  } else {
    raw_block_page->WUnlatch();
  }
  buffer_pool_manager_->UnpinPage(page_ids_[block_index], false);

  bucket_index = 0;
  block_index = (block_index + 1) / num_pages_;

  raw_block_page = buffer_pool_manager_->FetchPage(page_ids_[block_index]);
  if (lockType == LockType::READ) {
    raw_block_page->RLatch();
  } else {
    raw_block_page->WLatch();
  }

  block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(raw_block_page);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto flag = InsertImpl(transaction, key, value);
  table_latch_.RUnlock();
  return flag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::InsertImpl(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto [slot_index, block_index, bucket_index] = GetIndex(key);

  auto raw_block_page = buffer_pool_manager_->FetchPage(page_ids_[block_index]);
  raw_block_page->WLatch();
  auto block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(raw_block_page);

  bool flag = true;
  while (!block_page->Insert(bucket_index, key, value)) {
    if (block_page->IsReadable(bucket_index) && IsMash(block_page, bucket_index, key, value)) {
      flag = false;
      break;
    }

    StepForward(bucket_index, block_index, raw_block_page, block_page, LockType::WRITE);

    // 如果回到初始位置，resize
    if (block_index * BLOCK_ARRAY_SIZE + bucket_index == slot_index) {
      raw_block_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_ids_[block_index], false);

      table_latch_.RUnlock();
      Resize(num_buckets_);
      table_latch_.RLock();

      std::tie(slot_index, block_index, bucket_index) = GetIndex(key);

      raw_block_page = buffer_pool_manager_->FetchPage(page_ids_[block_index]);
      raw_block_page->WLatch();
      block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(raw_block_page);
    }
  }

  raw_block_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(raw_block_page->GetPageId(), flag);

  return flag;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  auto [slot_index, block_index, bucket_index] = GetIndex(key);

  auto *raw_block_page = buffer_pool_manager_->FetchPage(page_ids_[block_index]);
  raw_block_page->WLatch();
  auto block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(raw_block_page);

  bool flag = false;
  while (block_page->IsOccupied(bucket_index)) {
    if (IsMash(block_page, bucket_index, key, value)) {
      if (block_page->IsReadable(bucket_index)) {
        block_page->Remove(bucket_index);
        flag = true;
      } else {
        flag = false;
      }
      break;
    }

    StepForward(bucket_index, block_index, raw_block_page, block_page, LockType::WRITE);
    
    if (block_index * BLOCK_ARRAY_SIZE + bucket_index == slot_index) {
      break;
    }
  }

  raw_block_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(raw_block_page->GetPageId(), flag);
  table_latch_.RUnlock();

  return flag;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  table_latch_.WLock();
  num_buckets_ = 2 * initial_size;
  num_pages_ = (num_buckets_ - 1) / BLOCK_ARRAY_SIZE + 1;
  last_block_array_size_ = num_buckets_ - (num_pages_ - 1) * BLOCK_ARRAY_SIZE;

  auto old_head_page_id = header_page_id_;
  std::vector<page_id_t> old_page_ids(page_ids_);

  auto *raw_header_page = buffer_pool_manager_->NewPage(&header_page_id_);
  raw_header_page->WLatch();
  InitHeaderPage(reinterpret_cast<HashTableHeaderPage *>(raw_header_page));

  for (size_t block_index{0}; block_index < num_pages_; ++block_index) {
    auto old_page_id = old_page_ids[block_index];
    auto *raw_block_page = buffer_pool_manager_->FetchPage(old_page_id);
    raw_block_page->RLatch();
    auto block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(raw_block_page);

    for (slot_offset_t bucket_index{0}; bucket_index < GetBlockArraySize(block_index); ++bucket_index) {
      if (block_page->IsReadable(bucket_index)) {
        InsertImpl(nullptr, block_page->KeyAt(bucket_index), block_page->ValueAt(bucket_index));
      }
    }

    // delete old_page;
    raw_block_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(old_page_id, false);
    buffer_pool_manager_->DeletePage(old_page_id);
  }

  raw_header_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  buffer_pool_manager_->DeletePage(old_head_page_id);

  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  table_latch_.RLock();
  size_t out = num_buckets_;
  table_latch_.RUnlock();
  return out;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
