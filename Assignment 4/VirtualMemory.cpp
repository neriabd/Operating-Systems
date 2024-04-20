#include "VirtualMemory.h"
#include "PhysicalMemory.h"

/*
 * Initialize the virtual memory.
 */
void VMinitialize ()
{
  for (uint64_t j = 0; j < PAGE_SIZE; ++j)
  {
    PMwrite (j, 0);
  }
}

word_t max_available_frame (word_t root_frame = 0, uint64_t depth = 0)
{
  if (depth == TABLES_DEPTH)
  {
    return root_frame;
  }
  word_t max_frame = root_frame;
  word_t child_val = 0;

  for (uint64_t i = 0; i < PAGE_SIZE; ++i)
  {
    PMread ((uint64_t) (root_frame) * PAGE_SIZE + i, &child_val);
    if (child_val != 0)
    {
      word_t cur_frame = max_available_frame (child_val, depth + 1);
      if (cur_frame > max_frame)
      {
        max_frame = cur_frame;
      }
    }
  }
  return max_frame;
}

word_t
empty_frame (word_t original_frame, word_t node_parent = 0, word_t cur_node = 0,
             uint64_t child_number = 0, uint64_t depth = 0)
{
  if (depth == TABLES_DEPTH || cur_node == original_frame)
  {
    return 0;
  }

  uint64_t childs_sum = 0;
  word_t child_val = 0;

  for (uint64_t i = 0; i < PAGE_SIZE; ++i)
  {
    child_val = 0;
    PMread ((uint64_t) (cur_node) * PAGE_SIZE + i, &child_val);
    childs_sum += child_val;
  }

  if (childs_sum == 0)
  {
    PMwrite ((uint64_t) (node_parent) * PAGE_SIZE + child_number, 0);
    return cur_node;
  }

  for (uint64_t i = 0; i < PAGE_SIZE; ++i)
  {
    PMread ((uint64_t) (cur_node) * PAGE_SIZE + i, &child_val);
    if (child_val != 0)
    {
      word_t cur_frame = empty_frame (original_frame, cur_node, child_val,
                                      i, depth + 1);
      if (cur_frame != 0)
      {
        return cur_frame;
      }
    }
  }
  return 0;
}

uint64_t min_distance (uint64_t new_page, uint64_t cur_address)
{
  uint64_t abs_diff = 0;
  if (new_page > cur_address)
  {
    abs_diff = new_page - cur_address;
  }
  else
  {
    abs_diff = cur_address - new_page;
  }
  uint64_t cyclic_distance = NUM_PAGES - abs_diff;
  uint64_t regular_distance = abs_diff;
  if (cyclic_distance < regular_distance)
  {
    return cyclic_distance;
  }
  return regular_distance;
}

typedef struct
{
    uint64_t max_distance;
    uint64_t evicted_page;
    word_t evicted_frame;
    word_t frame_parent;
    uint64_t child_number;
} min_index_distance;

min_index_distance
evict_page (uint64_t new_page, uint64_t temp_page = 0, word_t cur_node = 0,
            word_t node_parent = 0, uint64_t child_number = 0,
            uint64_t depth = 0)
{
  if (depth == TABLES_DEPTH)
  {
    return {min_distance (new_page, temp_page), temp_page, cur_node,
            node_parent, child_number};
  }

  min_index_distance final_data = {0, 0, 0, 0, 0};
  word_t child_frame = 0;

  for (int i = 0; i < PAGE_SIZE; ++i)
  {
    PMread (cur_node * PAGE_SIZE + i, &child_frame);
    if (child_frame != 0)
    {
      uint64_t new_address = (temp_page << OFFSET_WIDTH) + i;
      min_index_distance data = evict_page (new_page, new_address, child_frame,
                                            cur_node, i,
                                            depth + 1);
      if (data.max_distance > final_data.max_distance)
      {
        final_data = data;
      }
    }
  }
  return final_data;
}

void
find_frame (word_t cur_frame, word_t &new_frame, uint64_t page_number, bool &max_frames_found)
{
  new_frame = empty_frame (cur_frame);
  if (new_frame != 0)
  {
    return;
  }
  if (!max_frames_found)
  {
    new_frame = max_available_frame (max_frames_found) + 1;
    if (new_frame < NUM_FRAMES)
    {
      return;
    }
    max_frames_found = true;
  }
  min_index_distance evict_info = evict_page (page_number);
  PMevict ((uint64_t) evict_info.evicted_frame, evict_info.evicted_page);
  new_frame = evict_info.evicted_frame;
  PMwrite ((uint64_t) (evict_info.frame_parent) * PAGE_SIZE
           + evict_info.child_number, 0);
}

uint64_t find_physical_address (uint64_t virtualAddress)
{
  uint64_t mask = (1 << OFFSET_WIDTH) - 1;
  uint64_t offset = virtualAddress & mask;
  word_t parent_frame = 0;
  word_t child_frame = 0;
  uint64_t page_number = virtualAddress >> OFFSET_WIDTH;
  bool page_fault = false;
  bool max_frames_found = false;

  for (uint64_t i = TABLES_DEPTH; i > 0; --i)
  {
    uint64_t p = (virtualAddress >> (OFFSET_WIDTH * i)) & mask;
    PMread ((uint64_t) (parent_frame) * PAGE_SIZE + p, &child_frame);
    if (child_frame == 0)
    {
      page_fault = true;
      find_frame (parent_frame, child_frame, page_number, max_frames_found);
      if (i != 1)
      {
        for (uint64_t j = 0; j < PAGE_SIZE; ++j)
        {
          PMwrite ((uint64_t) (child_frame) * PAGE_SIZE + j, 0);
        }
      }
      PMwrite ((uint64_t) (parent_frame) * PAGE_SIZE + p, child_frame);
    }
    parent_frame = child_frame;
  }

  if (page_fault)
  {
    PMrestore ((uint64_t) parent_frame, page_number);
  }
  return (uint64_t) (parent_frame) * PAGE_SIZE + offset;
}

int VMread (uint64_t virtualAddress, word_t *value)
{
  if (virtualAddress >= VIRTUAL_MEMORY_SIZE)
  {
    return 0;
  }
  uint64_t physical_address = find_physical_address (virtualAddress);
  PMread (physical_address, value);
  return 1;
}

int VMwrite (uint64_t virtualAddress, word_t value)
{
  if (virtualAddress >= VIRTUAL_MEMORY_SIZE)
  {
    return 0;
  }
  uint64_t physical_address = find_physical_address (virtualAddress);
  PMwrite (physical_address, value);
  return 1;
}