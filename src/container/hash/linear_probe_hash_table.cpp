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

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"


namespace bustub {

class ScopeRPageLatch
{
public:
  ScopeRPageLatch(BufferPoolManager* buffer_pool_manager, Page* page) {
    buffer_pool_manager_ = buffer_pool_manager;
    page_ = page;
    page_->RLatch();
  }

  ~ScopeRPageLatch() {
    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
  }

private:
  BufferPoolManager* buffer_pool_manager_ = nullptr;
  Page* page_;
};

class ScopeWPageLatch
{
public:
  ScopeWPageLatch(BufferPoolManager* buffer_pool_manager, Page* page) {
    buffer_pool_manager_ = buffer_pool_manager;
    page_ = page;
    page_->WLatch();
  }

  ~ScopeWPageLatch() {
    page_->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), true);
  }

private:
  BufferPoolManager* buffer_pool_manager_ = nullptr;
  Page* page_;
};

class ScopeRTableLatch
{
public:
  ScopeRTableLatch(ReaderWriterLatch* latch) {
    latch_ = latch;
    latch_->RLock();
  }

  ~ScopeRTableLatch() {
    latch_->RUnlock();
  }

private:
  ReaderWriterLatch* latch_;
};

class ScopeWTableLatch
{
public:
  ScopeWTableLatch(ReaderWriterLatch* latch) {
    latch_ = latch;
    latch_->WLock();
  }

  ~ScopeWTableLatch() {
    latch_->WUnlock();
  }

private:
  ReaderWriterLatch* latch_;
};

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
      // Allocate header page
      Page* header_page = buffer_pool_manager_->NewPage(&header_page_id_);
      HashTableHeaderPage* hash_table_header_page = reinterpret_cast<HashTableHeaderPage*>(header_page->GetData());
      assert(hash_table_header_page != nullptr);
      ScopeWPageLatch wh_latch(buffer_pool_manager_, header_page);
      
      hash_table_header_page->SetPageId(header_page_id_);
      hash_table_header_page->SetSize(num_buckets);
      size_ = num_buckets;
      
      // Allocate block pages
      size_t num_blocks = (num_buckets % BLOCK_ARRAY_SIZE == 0) ? num_buckets / BLOCK_ARRAY_SIZE : num_buckets / BLOCK_ARRAY_SIZE + 1;
      for (size_t index = 0; index < num_blocks; index++) {
        page_id_t block_page_id;
        Page* block_page = buffer_pool_manager_->NewPage(&block_page_id);
        assert(block_page != nullptr);
        ScopeRPageLatch rb_latch(buffer_pool_manager_, block_page);
        hash_table_header_page->AddBlockPageId(block_page_id);
      }
    }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  {
    ScopeRTableLatch r_table_latch(&table_latch_);

    Page* header_page = buffer_pool_manager_->FetchPage(header_page_id_);
    if (!header_page) {
      LOG_WARN("HASH_TABLE_TYPE::GetValue Fail to pin header page");
      return false;
    }
    HashTableHeaderPage* hash_table_header_page = reinterpret_cast<HashTableHeaderPage*>(header_page->GetData());
    ScopeRPageLatch rh_latch(buffer_pool_manager_, header_page);

    // Locate
    uint64_t hash_val = hash_fn_.GetHash(key);
    size_t num_buckets = hash_table_header_page->GetSize();
    size_t num_blocks = hash_table_header_page->NumBlocks();
    size_t block_index = (hash_val % num_buckets) / BLOCK_ARRAY_SIZE;
    size_t bucket_index = (hash_val % num_buckets) % BLOCK_ARRAY_SIZE;
    
    // Scan
    size_t cur_block_index = block_index;
    size_t cur_bucket_index = bucket_index;
    bool first = true;
    bool cycle = false;

    do {
      page_id_t cur_block_page_id = hash_table_header_page->GetBlockPageId(cur_block_index);
      Page* cur_block_page = buffer_pool_manager_->FetchPage(cur_block_page_id);
      if (!cur_block_page) {
        LOG_WARN("HASH_TABLE_TYPE::GetValue Fail to pin block page, index=%lu, page_id=%d", cur_block_index, cur_block_page_id);
        return false;
      }
      HashTableBlockPage<KeyType, ValueType, KeyComparator>* cur_hash_table_block_page = 
        reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(cur_block_page->GetData());
      ScopeRPageLatch rb_latch(buffer_pool_manager_, cur_block_page);

      size_t max_bucket_index = BLOCK_ARRAY_SIZE;
      if ((num_buckets % BLOCK_ARRAY_SIZE != 0) && (cur_block_index == num_blocks - 1)) {
        max_bucket_index = num_buckets % BLOCK_ARRAY_SIZE;
      }

      for (size_t i = cur_bucket_index; i < max_bucket_index; i++) {
        if (cur_block_index == block_index && i == bucket_index) {
          if (first) {
            first = false;
          } else {
            cycle = true;
            break;
          }
        }
        if (cur_hash_table_block_page->IsOccupied(i)) {
          if (cur_hash_table_block_page->IsReadable(i)) {
            KeyType cur_key = cur_hash_table_block_page->KeyAt(i);
            if (comparator_(key, cur_key) == 0) {
              result->push_back(cur_hash_table_block_page->ValueAt(i));
            }
          }
        } else {
          return true;
        }
      }

      cur_block_index = (cur_block_index + 1) % num_blocks;
      cur_bucket_index = 0;

    } while(!cycle);
  }

  return true;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  {
    ScopeRTableLatch r_table_latch(&table_latch_);

    Page* header_page = buffer_pool_manager_->FetchPage(header_page_id_);
    if (!header_page) {
      LOG_WARN("HASH_TABLE_TYPE::Insert Fail to pin header page");
      return false;
    }
    HashTableHeaderPage* hash_table_header_page = reinterpret_cast<HashTableHeaderPage*>(header_page->GetData());
    ScopeRPageLatch rh_latch(buffer_pool_manager_, header_page);

    // Locate
    uint64_t hash_val = hash_fn_.GetHash(key);
    size_t num_buckets = hash_table_header_page->GetSize();
    size_t num_blocks = hash_table_header_page->NumBlocks();

    size_t block_index = (hash_val % num_buckets) / BLOCK_ARRAY_SIZE;
    size_t bucket_index = (hash_val % num_buckets) % BLOCK_ARRAY_SIZE;

    // Scan
    size_t cur_block_index = block_index;
    size_t cur_bucket_index = bucket_index;
    bool first = true;
    bool cycle = false;

    do {
      page_id_t cur_block_page_id = hash_table_header_page->GetBlockPageId(cur_block_index);
      Page* cur_block_page = buffer_pool_manager_->FetchPage(cur_block_page_id);
      if (!cur_block_page) {
        LOG_WARN("HASH_TABLE_TYPE::Insert Fail to pin block page, index=%lu, page_id=%d", cur_block_index, cur_block_page_id);
        return false;
      }
      HashTableBlockPage<KeyType, ValueType, KeyComparator>* cur_hash_table_block_page = 
        reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(cur_block_page->GetData());
      ScopeWPageLatch wb_latch(buffer_pool_manager_, cur_block_page);

      size_t max_bucket_index = BLOCK_ARRAY_SIZE;
      if ((num_buckets % BLOCK_ARRAY_SIZE != 0) && (cur_block_index == num_blocks - 1)) {
        max_bucket_index = num_buckets % BLOCK_ARRAY_SIZE;
      }

      for (size_t i = cur_bucket_index; i < max_bucket_index; i++) {
        if (cur_block_index == block_index && i == bucket_index) {
          if (first) {
            first = false;
          } else {
            cycle = true;
            break;
          }
        }
        if (cur_hash_table_block_page->IsReadable(i)) {
          KeyType cur_key = cur_hash_table_block_page->KeyAt(i);
          ValueType cur_value = cur_hash_table_block_page->ValueAt(i);
          if ((comparator_(key, cur_key) == 0) && (value == cur_value)) {
            return false;
          }
        } else {
          cur_hash_table_block_page->Insert(i, key, value);
          return true;
        }
      }

      cur_block_index = (cur_block_index + 1) % num_blocks;
      cur_bucket_index = 0;

    } while(!cycle);
  }

  Resize(size_ * 2);

  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  {
    ScopeRTableLatch r_table_latch(&table_latch_);

    Page* header_page = buffer_pool_manager_->FetchPage(header_page_id_);
    if (!header_page) {
      LOG_WARN("HASH_TABLE_TYPE::Remove Fail to pin header page");
      return false;
    }
    HashTableHeaderPage* hash_table_header_page = reinterpret_cast<HashTableHeaderPage*>(header_page->GetData());
    ScopeRPageLatch rh_latch(buffer_pool_manager_, header_page);

    // Locate
    uint64_t hash_val = hash_fn_.GetHash(key);
    size_t num_buckets = hash_table_header_page->GetSize();
    size_t num_blocks = hash_table_header_page->NumBlocks();
    size_t block_index = (hash_val % num_buckets) / BLOCK_ARRAY_SIZE;
    size_t bucket_index = (hash_val % num_buckets) % BLOCK_ARRAY_SIZE;
    
    // Scan
    size_t cur_block_index = block_index;
    size_t cur_bucket_index = bucket_index;
    bool first = true;
    bool cycle = false;

    do {
      page_id_t cur_block_page_id = hash_table_header_page->GetBlockPageId(cur_block_index);
      Page* cur_block_page = buffer_pool_manager_->FetchPage(cur_block_page_id);
      if (!cur_block_page) {
        LOG_WARN("HASH_TABLE_TYPE::Remove Fail to pin block page, index=%lu, page_id=%d", cur_block_index, cur_block_page_id);
        return false;
      }
      HashTableBlockPage<KeyType, ValueType, KeyComparator>* cur_hash_table_block_page = 
        reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(cur_block_page->GetData());
      ScopeWPageLatch block_page_latch(buffer_pool_manager_, cur_block_page);

      size_t max_bucket_index = BLOCK_ARRAY_SIZE;
      if ((num_buckets % BLOCK_ARRAY_SIZE != 0) && (cur_block_index == num_blocks - 1)) {
        max_bucket_index = num_buckets % BLOCK_ARRAY_SIZE;
      }

      for (size_t i = cur_bucket_index; i < max_bucket_index; i++) {
        if (cur_block_index == block_index && i == bucket_index) {
          if (first) {
            first = false;
          } else {
            cycle = true;
            break;
          }
        }
        if (cur_hash_table_block_page->IsOccupied(i)) {
          if (cur_hash_table_block_page->IsReadable(i)) {
            KeyType cur_key = cur_hash_table_block_page->KeyAt(i);
            ValueType cur_value = cur_hash_table_block_page->ValueAt(i);
            if ((comparator_(key, cur_key) == 0) && (value == cur_value)) {
              cur_hash_table_block_page->Remove(i);
              return true;
            }
          }
        } else {
          return false;
        }
      }

      cur_block_index = (cur_block_index + 1) % num_blocks;
      cur_bucket_index = 0;

    } while(!cycle);
  }

  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  {
    ScopeWTableLatch w_table_latch(&table_latch_);

    if (initial_size <= size_) {
      return;
    }
    
    Page* header_page = buffer_pool_manager_->FetchPage(header_page_id_);
    if (!header_page) {
      LOG_WARN("HASH_TABLE_TYPE::Resize Fail to pin header page of old table");
      return;
    }
    HashTableHeaderPage* hash_table_header_page = reinterpret_cast<HashTableHeaderPage*>(header_page->GetData());

    page_id_t new_header_page_id;
    Page* new_header_page = buffer_pool_manager_->NewPage(&new_header_page_id);
    if (!new_header_page) {
      LOG_WARN("HASH_TABLE_TYPE::Resize Fail to allocate a new header page for new table");
      return;
    }
    HashTableHeaderPage* new_hash_table_header_page = reinterpret_cast<HashTableHeaderPage*>(new_header_page->GetData());
    ScopeWPageLatch wnh_latch(buffer_pool_manager_, new_header_page);
    
    new_hash_table_header_page->SetPageId(new_header_page_id);
    new_hash_table_header_page->SetSize(initial_size);
    
    size_t num_blocks = (initial_size / BLOCK_ARRAY_SIZE) + 1;
    for (size_t index = 0; index < num_blocks; index++) {
      page_id_t block_page_id;
      Page* block_page = buffer_pool_manager_->NewPage(&block_page_id);
      assert(block_page != nullptr);
      ScopeRPageLatch rb_latch(buffer_pool_manager_, block_page);
      new_hash_table_header_page->AddBlockPageId(block_page_id);
    }

    auto InsertToNew = [&](const KeyType &key, const ValueType &value) {
      // Locate
      uint64_t hash_val = hash_fn_.GetHash(key);
      size_t num_buckets = new_hash_table_header_page->GetSize();
      size_t num_blocks = new_hash_table_header_page->NumBlocks();

      size_t block_index = (hash_val % num_buckets) / BLOCK_ARRAY_SIZE;
      size_t bucket_index = (hash_val % num_buckets) % BLOCK_ARRAY_SIZE;
      
      // Scan
      size_t cur_block_index = block_index;
      size_t cur_bucket_index = bucket_index;
      bool first = true;
      bool cycle = false;

      do {
        page_id_t cur_block_page_id = new_hash_table_header_page->GetBlockPageId(cur_block_index);
        Page* cur_block_page = buffer_pool_manager_->FetchPage(cur_block_page_id);
        if (!cur_block_page) {
          LOG_WARN("HASH_TABLE_TYPE::Resize Fail to pin block page in new table, index=%lu, page_id=%d", cur_block_index, cur_block_page_id);
          return false;
        }
        HashTableBlockPage<KeyType, ValueType, KeyComparator>* cur_hash_table_block_page = 
          reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(cur_block_page->GetData());
        ScopeWPageLatch wb_latch(buffer_pool_manager_, cur_block_page);

        for (size_t i = cur_bucket_index; i < BLOCK_ARRAY_SIZE; i++) {
          if (cur_block_index == block_index && i == bucket_index) {
            if (first) {
              first = false;
            } else {
              cycle = true;
              break;
            }
          }
          if (cur_hash_table_block_page->IsReadable(i)) {
            KeyType cur_key = cur_hash_table_block_page->KeyAt(i);
            ValueType cur_value = cur_hash_table_block_page->ValueAt(i);
            if ((comparator_(key, cur_key) == 0) && (value == cur_value)) {
              return false;
            }
          } else {
            cur_hash_table_block_page->Insert(i, key, value);
            return true;
          }
        }

        cur_block_index = (cur_block_index + 1) % num_blocks;
        cur_bucket_index = 0;

      } while(!cycle);

      return false;
    };

    // Insertion for each existing pair
    for (size_t index = 0; index < hash_table_header_page->NumBlocks(); index++) {
      page_id_t block_page_id = hash_table_header_page->GetBlockPageId(index);
      Page* block_page = buffer_pool_manager_->FetchPage(block_page_id);
      if (!block_page) {
        LOG_WARN("HASH_TABLE_TYPE::Resize Fail to pin block page in old table, index=%lu, page_id=%d", index, block_page_id);
        return;
      }
      HashTableBlockPage<KeyType, ValueType, KeyComparator>* hash_table_block_page = 
        reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator>*>(block_page->GetData());
      ScopeRPageLatch rb_latch(buffer_pool_manager_, block_page);
      
      for (size_t bucket_index = 0; bucket_index < BLOCK_ARRAY_SIZE; bucket_index++) {
        if (hash_table_block_page->IsReadable(bucket_index)) {
          InsertToNew(hash_table_block_page->KeyAt(bucket_index), hash_table_block_page->ValueAt(bucket_index));
        }
      }
    }

    // Free old pages

    for (size_t index = 0; index < hash_table_header_page->NumBlocks(); index++) {
      page_id_t block_page_id = hash_table_header_page->GetBlockPageId(index);
      buffer_pool_manager_->DeletePage(block_page_id);
    }
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    buffer_pool_manager_->DeletePage(header_page_id_);

    // Use new pages
    header_page_id_ = new_header_page_id;
    size_ = initial_size;
  }
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  return size_;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
