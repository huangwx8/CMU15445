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

#include <list>
#include <unordered_map>

#include "common/logger.h"

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

// 1.     Search the page table for the requested page (P).
// 1.1    If P exists, pin it and return it immediately.
// 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
//        Note that pages are always found from the free list first.
// 2.     If R is dirty, write it back to the disk.
// 3.     Delete R from the page table and insert P.
// 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) 
{
	frame_id_t frame_id = -1;

	Page* page = nullptr;

	bool locked = latch_.try_lock();

	auto it = page_table_.find(page_id);

	if (it != page_table_.end())
	{
		frame_id = it->second;

		page = &pages_[frame_id];

		page->WLatch();

		if (page->pin_count_ == 0) 
		{
			replacer_->Pin(frame_id);
		}

		page->pin_count_++;

		page->WUnlatch();
	}

	if (!page)
	{
		frame_id = GetUsableFrameImpl();

		if (frame_id != -1)
		{
			page_table_[page_id] = frame_id;

			page = &pages_[frame_id];

			page->WLatch();

			page->page_id_ = page_id;

			page->pin_count_ = 1;

			replacer_->Pin(frame_id);

			disk_manager_->ReadPage(page_id, page->data_);

			page->WUnlatch();
		}
	}

	if (locked)
	{
		latch_.unlock();
	}

	return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) 
{
	bool success = false;

	frame_id_t frame_id = -1;

	Page* page = nullptr;

	bool locked = latch_.try_lock();

	auto it = page_table_.find(page_id);

	if (it != page_table_.end())
	{
		frame_id = it->second;

		page = &pages_[frame_id];

		page->WLatch();

		if (page->pin_count_ > 0)
		{
			page->pin_count_--;

			if (page->pin_count_ == 0) 
			{
				replacer_->Unpin(frame_id);
			}

			if (is_dirty)
			{
				page->is_dirty_ = is_dirty;
			}

			success = true;
		}

		page->WUnlatch();
	}

	if (locked)
	{
		latch_.unlock();
	}

	return success;
}

// Make sure you call DiskManager::WritePage!
bool BufferPoolManager::FlushPageImpl(page_id_t page_id) 
{
	bool success = false;
	
	frame_id_t frame_id = -1;

	Page* page = nullptr;

	bool locked = latch_.try_lock();

	auto it = page_table_.find(page_id);

	if (it != page_table_.end()) 
	{
		frame_id = it->second;

		page = &pages_[frame_id];

		page->WLatch();

		disk_manager_->WritePage(page_id, page->data_);

		page->is_dirty_ = false;

		page->WUnlatch();

		success = true;
	}

	if (locked)
	{
		latch_.unlock();
	}

	return success;
}

// 0.   Make sure you call DiskManager::AllocatePage!
// 1.   If all the pages in the buffer pool are pinned, return nullptr.
// 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
// 3.   Update P's metadata, zero out memory and add P to the page table.
// 4.   Set the page ID output parameter. Return a pointer to P.
Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) 
{
	frame_id_t frame_id = -1;

	Page* page = nullptr;

	bool locked = latch_.try_lock();

	frame_id = GetUsableFrameImpl();

	if (frame_id != -1)
	{
		*page_id = disk_manager_->AllocatePage();

		page_table_[*page_id] = frame_id;

		page = &pages_[frame_id];

		page->WLatch();

		bzero(page->data_, PAGE_SIZE);

		disk_manager_->WritePage(*page_id, page->data_);

		page->page_id_ = *page_id;

		page->pin_count_ = 1;

		replacer_->Pin(frame_id);

		page->is_dirty_ = false;

		page->WUnlatch();
	}

	if (locked)
	{
		latch_.unlock();
	}

	return page;
}

// 0.   Make sure you call DiskManager::DeallocatePage!
// 1.   Search the page table for the requested page (P).
// 1.   If P does not exist, return true.
// 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
// 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
bool BufferPoolManager::DeletePageImpl(page_id_t page_id) 
{
	bool success = false;

	frame_id_t frame_id = -1;

	Page* page = nullptr;

	bool locked = latch_.try_lock();

	auto it = page_table_.find(page_id);

	if (it != page_table_.end()) 
	{
		frame_id = it->second;

		page = &pages_[frame_id];

		page->WLatch();

		if (page->pin_count_ == 0)
		{
			page_table_.erase(page_id);

			free_list_.push_back(frame_id);

			if (page->is_dirty_)
			{
				disk_manager_->WritePage(page_id, page->data_);
				page->is_dirty_ = false;
			}

			page->page_id_ = INVALID_PAGE_ID;

			page->pin_count_ = 0;

			success = true;
		}
		else 
		{
			success = false;
		}

		page->WUnlatch();
	}
	else 
	{
		success = true;
	}

	if (locked)
	{
		latch_.unlock();
	}

	return success;
}

// You can do it!
void BufferPoolManager::FlushAllPagesImpl() 
{
	frame_id_t frame_id = -1;

	Page* page = nullptr;

	latch_.lock();

	for (auto p : page_table_)
	{
		frame_id = p.second;

		page = &pages_[frame_id];

		page->WLatch();
		
		disk_manager_->WritePage(pages_->page_id_, page->data_);
		
		page->is_dirty_ = false;
		
		page->WUnlatch();
	}

	latch_.unlock();
}

frame_id_t BufferPoolManager::GetUsableFrameImpl() 
{
	frame_id_t frame_id = -1;

	if (latch_.try_lock())
	{
		LOG_ERROR("GetUsableFrameImpl latch_ is not locked by outer");
		latch_.unlock();
		return frame_id;
	}

	if (!free_list_.empty()) // from free list
	{
		frame_id = free_list_.front();
		free_list_.pop_front();
	}
	else if (replacer_->Victim(&frame_id)) // from replacer
	{
		Page* page = &pages_[frame_id];

		page->WLatch();

		if (page->pin_count_ > 0)
		{
			LOG_ERROR("GetUsableFrameImpl page is pinned");
		}

		page_table_.erase(page->page_id_);
		
		if (page->is_dirty_)
		{
			disk_manager_->WritePage(page->page_id_, page->data_);
			page->is_dirty_ = false;
		}

		page->WUnlatch();
	}

	return frame_id;
}

}  // namespace bustub
