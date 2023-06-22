//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_block_page.cpp
//
// Identification: src/storage/page/hash_table_block_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_block_page.h"
#include "storage/index/generic_key.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BLOCK_TYPE::KeyAt(slot_offset_t bucket_ind) const {
  return array_[bucket_ind].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BLOCK_TYPE::ValueAt(slot_offset_t bucket_ind) const {
  return array_[bucket_ind].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::Insert(slot_offset_t bucket_ind, const KeyType &key, const ValueType &value) {
  if (!IsReadable(bucket_ind)) {
    array_[bucket_ind] = std::make_pair(key, value);
    SetOccupied(bucket_ind, true);
    SetReadable(bucket_ind, true);
    return true;
  } else {
    return false;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::Remove(slot_offset_t bucket_ind) {
  SetReadable(bucket_ind, false);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsOccupied(slot_offset_t bucket_ind) const {
  return occupied_[bucket_ind / 8] & (0x1 << (bucket_ind % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsReadable(slot_offset_t bucket_ind) const {
  return readable_[bucket_ind / 8] & (0x1 << (bucket_ind % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::SetOccupied(slot_offset_t bucket_ind, bool occupied) {
  if (occupied) {
    occupied_[bucket_ind / 8] = occupied_[bucket_ind / 8] | (0x1 << (bucket_ind % 8));
  } else {
    occupied_[bucket_ind / 8] = occupied_[bucket_ind / 8] & (~(0x1 << (bucket_ind % 8)));
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::SetReadable(slot_offset_t bucket_ind, bool readable) {
  if (readable) {
    readable_[bucket_ind / 8] = readable_[bucket_ind / 8] | (0x1 << (bucket_ind % 8));
  } else {
    readable_[bucket_ind / 8] = readable_[bucket_ind / 8] & (~(0x1 << (bucket_ind % 8)));
  }
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBlockPage<int, int, IntComparator>;
template class HashTableBlockPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBlockPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBlockPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBlockPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBlockPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
