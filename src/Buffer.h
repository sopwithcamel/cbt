// Copyright (C) 2012 Georgia Institute of Technology
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

// ---
// Author: Hrishikesh Amur

#ifndef SRC_BUFFER_H_
#define SRC_BUFFER_H_
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include "Config.h"

namespace cbt {
    class Node;

    class Buffer {
        friend class Node;
        friend class CompressTree;
        friend class Compressor;

        public:
#ifdef ENABLE_PAGING
          enum PageAction {
              NO_PAGE,
              PAGE_OUT,
              PAGE_IN
          };
#endif  // ENABLE_PAGING

          class List {
            public:
              enum ListState {
                  DECOMPRESSED,
                  COMPRESSED,
                  PAGED_OUT
              };
              List();
              ~List();
              /* allocates buffers */
              void allocate(bool isLarge);
              /* frees allocated buffers. Maintains counter info */
              void deallocate();
              /* set list to empty */
              void setEmpty();

              static uint64_t allocated_lists;

              uint32_t* hashes_;
              uint32_t* sizes_;
              char* data_;
              uint32_t num_;
              uint32_t size_;
              ListState state_;
              size_t c_hashlen_;
              size_t c_sizelen_;
              size_t c_datalen_;
          };

          Buffer();
          Buffer(const Buffer&);
          // clears all buffer state
          ~Buffer();
          // add a list and allocate memory
          List* addList(bool isLarge = false);
          void addList(List* l);
          /* clear the lists_ vector. This does not free space allocated
           * for the buffers but merely deletes the pointers. To avoid
           * memory leaks, this must be called after deallocate() */
          void delList(uint32_t ind);
          void clear();
          /* frees buffers in all the lists. This maintains all the count
           * information about each of the lists */
          void deallocate();
          /* returns true if the sum of all the list_s[i]->num_ is zero. This
           * can happen even if no memory is allocated to the buffers as all
           * buffers may be compressed */
          bool empty() const;
          uint32_t numElements() const;
          void setParent(Node* n);
#ifdef ENABLE_PAGING
          void setupPaging();
          void cleanupPaging();
#endif  // ENABLE_PAGING

          /* Sorting-related */
          void quicksort(uint32_t left, uint32_t right);
          void radixsort(uint32_t left, uint32_t right, uint32_t shift);
          void insertion_sort(uint32_t left, uint32_t right);
          bool sort();

          /* Merge the sorted sub-lists of the buffer */
          bool merge();

          /* Aggregation-related */
          bool aggregate(bool isSort);

          /* Compression-related */
          bool compress();
          bool decompress();
          void setCompressible(bool flag);

#ifdef ENABLE_PAGING
          /* Paging-related */
          bool pageOut();
          bool pageIn();
          void setPageable(bool flag);
          /* return value indicates whether the node needs to be added or
           * if it's already present in the queue */
          bool checkPageOut();
          bool checkPageIn();
          bool performPageAction();
          PageAction getPageAction();
#endif  // ENABLE_PAGING
          bool checkSortIntegrity(List* l);

          bool checkIntegrity();
          bool checkSortIntegrity();

        private:
          const Node* node_;
          /* buffer fragments */
          std::vector<List*> lists_;
          bool compressible_;
          // used during sort
          char** perm_;
          // used during merge
          List* aux_list_;

#ifdef ENABLE_PAGING
          /* Paging-related */
          bool pageable_;
          FILE* f_;
#endif  // ENABLE_PAGING
    };
}
#endif  // SRC_BUFFER_H_
