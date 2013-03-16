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

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <sstream>
#include "Buffer.h"
#include "CompressTree.h"
#include "compsort.h"
#include "rle.h"
#include "snappy.h"
#include "lz4.h"

namespace cbt {
    Buffer::List::List(bool fd_req, AllocType type) :
            state_(IN_MEMORY),
            hashes_(NULL),
            sizes_(NULL),
            data_(NULL),
            hash_offset_(0),
            size_offset_(0),
            data_offset_(0),
            num_(0),
            size_(0),
            fd_(-1) {

        if (type != NO_ALLOC) {
            uint32_t nel = cbt::MAX_ELS_PER_BUFFER;
            uint32_t buf = cbt::BUFFER_SIZE;
            if (type == LARGE_ALLOC) {
                nel *= 2;
                buf *= 2;
            }
            hashes_ = reinterpret_cast<uint32_t*>(malloc(sizeof(uint32_t) * nel));
            sizes_ = reinterpret_cast<uint32_t*>(malloc(sizeof(uint32_t) * nel));
            data_ = reinterpret_cast<char*>(malloc(buf));
        }

        if (fd_req) {
            do {
                stringstream ss;
                ss << "/mnt/hamur/cbt_data/" << rand() << ".buf";
                filename_ = ss.str();
                fd_ = open(filename_.c_str(), O_RDWR | O_CREAT | O_EXCL,
                        S_IRUSR | S_IWUSR);
            } while (fd_ < 0 && errno == EEXIST);
            if (fd_ < 0) {
                fprintf(stderr, "Error opening file: %s\n", strerror(errno));
                assert(false);
            }
        }
    }

    Buffer::List::~List() {
        free_buffers();
        if (fd_ > 0) {
            // decrement ref-count for fd_; if 0 then close and delete file
            close(fd_);
        }
    }

    void Buffer::List::free_buffers() {
        if (hashes_) {
            free(hashes_);
            hashes_ = NULL;
        }
        if (sizes_) {
            free(sizes_);
            sizes_ = NULL;
        }
        if (data_) {
            free(data_);
            data_ = NULL;
        }
    }

    void Buffer::List::setEmpty() {
        num_ = 0;
        size_ = 0;
        state_ = IN_MEMORY;
    }

    Buffer::Buffer() :
            pageable_(true), max_last_paos_(0) {
        pthread_spin_init(&lists_lock_, PTHREAD_PROCESS_PRIVATE);
    }

    Buffer::~Buffer() {
        pthread_spin_destroy(&lists_lock_);
        deallocate();
        if (max_last_paos_ > 0) {
            const Operations* o = node_->tree_->ops;
            for (uint32_t i = 0; i < max_last_paos_; ++i)
                o->destroyPAO(lastPAOs_[i]);
            lastPAOs_.clear();
        }
    }

    void Buffer::delList(uint32_t ind) {
        pthread_spin_lock(&lists_lock_);
        if (ind < lists_.size()) {
            lists_.erase(lists_.begin() + ind);
        }
        pthread_spin_unlock(&lists_lock_);
    }

    void Buffer::addList(Buffer::List* l) {
        pthread_spin_lock(&lists_lock_);
        lists_.push_back(l);
        pthread_spin_unlock(&lists_lock_);
    }

    void Buffer::clear() {
        pthread_spin_lock(&lists_lock_);
        lists_.clear();
        pthread_spin_unlock(&lists_lock_);
    }

    std::vector<Buffer::List*> Buffer::lists_copy() {
        pthread_spin_lock(&lists_lock_);
        std::vector<Buffer::List*> lc = lists_;
        pthread_spin_unlock(&lists_lock_);
        return lc;
    }

    void Buffer::deallocate() {
        std::vector<Buffer::List*> lists_c = lists_copy();
        for (uint32_t i = 0; i < lists_c.size(); ++i)
            delete lists_[i];
        lists_.clear();
    }

    bool Buffer::empty() {
        return (numElements() == 0);
    }

    uint32_t Buffer::numElements() {
        uint32_t num = 0;
        std::vector<Buffer::List*> lists_c = lists_copy();
        for (uint32_t i = 0; i < lists_c.size(); ++i)
            num += lists_c[i]->num_;
        return num;
    }

    uint32_t Buffer::size() {
        uint32_t siz = 0;
        std::vector<Buffer::List*> lists_c = lists_copy();
        for (uint32_t i = 0; i < lists_c.size(); ++i)
            siz += lists_c[i]->size_;
        return siz;
    }

    void Buffer::setParent(Node* n) {
        node_ = n;
    }

    void Buffer::convertOffsetsToSize() {
        for (uint32_t i = 0; i < lists_.size(); ++i) {
            Buffer::List* l = lists_[i];
            for (uint32_t j = 0; j < l->num_ - 1; ++j)
                l->sizes_[j] = l->sizes_[j + 1] - l->sizes_[j];
            l->sizes_[l->num_ - 1] = l->size_ - l->sizes_[l->num_ - 1];
        }
    }

    void Buffer::changeFileDescriptors() {
        for (uint32_t i = 0; i < lists_.size(); ++i) {
            close(lists_[i]->fd_);
            lists_[i]->fd_ = -1;

            // get new file descriptor
            do {
                stringstream ss;
                ss << "/mnt/hamur/cbt_data/" << rand() << ".buf";
                lists_[i]->filename_ = ss.str();
                lists_[i]->fd_ = open(lists_[i]->filename_.c_str(),
                        O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR);
            } while (lists_[i]->fd_ < 0 && errno == EEXIST);

            if (lists_[i]->fd_ < 0) {
                fprintf(stderr, "Error opening file: %s\n", strerror(errno));
                assert(false);
            }
        }
    }

    void Buffer::quicksort(uint32_t uleft, uint32_t uright) {
        int32_t i, j, stack_pointer = -1;
        int32_t left = uleft;
        int32_t right = uright;
        int32_t* rstack = new int32_t[128];
        uint32_t swap, temp;
        uint32_t sizs, sizt;
        char *pers, *pert;
        uint32_t* arr = lists_[0]->hashes_;
        uint32_t* siz = lists_[0]->sizes_;
        while (true) {
            if (right - left <= 7) {
                for (j = left + 1; j <= right; j++) {
                    swap = arr[j];
                    sizs = siz[j];
                    pers = perm_[j];
                    i = j - 1;
                    if (i < 0) {
                        fprintf(stderr, "Noo");
                        assert(false);
                    }
                    while (i >= left && (arr[i] > swap)) {
                        arr[i + 1] = arr[i];
                        siz[i + 1] = siz[i];
                        perm_[i + 1] = perm_[i];
                        i--;
                    }
                    arr[i + 1] = swap;
                    siz[i + 1] = sizs;
                    perm_[i + 1] = pers;
                }
                if (stack_pointer == -1) {
                    break;
                }
                right = rstack[stack_pointer--];
                left = rstack[stack_pointer--];
            } else {
                int median = (left + right) >> 1;
                i = left + 1;
                j = right;

                swap = arr[median];
                arr[median] = arr[i];
                arr[i] = swap;

                sizs = siz[median];
                siz[median] = siz[i];
                siz[i] = sizs;

                pers = perm_[median];
                perm_[median] = perm_[i];
                perm_[i] = pers;

                if (arr[left] > arr[right]) {
                    swap = arr[left];
                    arr[left] = arr[right];
                    arr[right] = swap;

                    sizs = siz[left];
                    siz[left] = siz[right];
                    siz[right] = sizs;

                    pers = perm_[left];
                    perm_[left] = perm_[right];
                    perm_[right] = pers;
                }
                if (arr[i] > arr[right]) {
                    swap = arr[i];
                    arr[i] = arr[right];
                    arr[right] = swap;

                    sizs = siz[i];
                    siz[i] = siz[right];
                    siz[right] = sizs;

                    pers = perm_[i];
                    perm_[i] = perm_[right];
                    perm_[right] = pers;
                }
                if (arr[left] > arr[i]) {
                    swap = arr[left];
                    arr[left] = arr[i];
                    arr[i] = swap;

                    sizs = siz[left];
                    siz[left] = siz[i];
                    siz[i] = sizs;

                    pers = perm_[left];
                    perm_[left] = perm_[i];
                    perm_[i] = pers;
                }
                temp = arr[i];
                sizt = siz[i];
                pert = perm_[i];
                while (true) {
                    while (arr[++i] < temp);
                    while (arr[--j] > temp);
                    if (j < i) {
                        break;
                    }
                    swap = arr[i];
                    arr[i] = arr[j];
                    arr[j] = swap;

                    sizs = siz[i];
                    siz[i] = siz[j];
                    siz[j] = sizs;

                    pers = perm_[i];
                    perm_[i] = perm_[j];
                    perm_[j] = pers;
                }
                arr[left + 1] = arr[j];
                siz[left + 1] = siz[j];
                perm_[left + 1] = perm_[j];
                arr[j] = temp;
                siz[j] = sizt;
                perm_[j] = pert;
                if (right - i + 1 >= j - left) {
                    rstack[++stack_pointer] = i;
                    rstack[++stack_pointer] = right;
                    right = j - 1;
                } else {
                    rstack[++stack_pointer] = left;
                    rstack[++stack_pointer] = j - 1;
                    left = i;
                }
            }
        }
        delete[] rstack;
    }

    void Buffer::insertion_sort(uint32_t uleft, uint32_t uright) {
        uint32_t x, y, temp;
        uint32_t size_temp;
        char* per_temp;
        uint32_t* array = lists_[0]->hashes_;
        uint32_t* sizes = lists_[0]->sizes_;
        for (x = uleft; x < uright; ++x) {
            for (y = x; y > uleft && array[y - 1] > array[y]; y--) {
                temp = array[y];
                array[y] = array[y-1];
                array[y-1] = temp;

                size_temp = sizes[y];
                sizes[y] = sizes[y-1];
                sizes[y-1] = size_temp;

                per_temp = perm_[y];
                perm_[y] = perm_[y-1];
                perm_[y-1] = per_temp;
            }
        }
    }

    void Buffer::radixsort(uint32_t uleft, uint32_t uright, uint32_t shift) {
        uint32_t x, y, value, temp;
        uint32_t last[256] = { 0 }, pointer[256];

        uint32_t* array = lists_[0]->hashes_;
        uint32_t* sizes = lists_[0]->sizes_;

        uint32_t size, size_temp;
        char *per, *per_temp;
        

        for (x = uleft; x < uright; ++x) {
            ++last[(array[x] >> shift) & 0xFF];
        }

        last[0] += uleft;
        pointer[0] = uleft;

        for (x = 1; x < 256; ++x) {
            pointer[x] = last[x - 1];
            last[x] += last[x - 1];
        }

        for (x = 0; x < 256; ++x) {
            while (pointer[x] != last[x]) {
                value = array[pointer[x]];
                size = sizes[pointer[x]];
                per = perm_[pointer[x]];
                
                y = (value >> shift) & 0xFF;
                while (x != y) {
                    temp = array[pointer[y]];
                    array[pointer[y]] = value;
                    value = temp;

                    size_temp = sizes[pointer[y]];
                    sizes[pointer[y]] = size;
                    size = size_temp;

                    per_temp = perm_[pointer[y]];
                    perm_[pointer[y]] = per;
                    per = per_temp;

                    pointer[y]++;
                    y = (value >> shift) & 0xFF;
                }
                array[pointer[x]] = value;
                sizes[pointer[x]] = size;
                perm_[pointer[x]] = per;
                pointer[x]++;
            }
        }

        if (shift > 0) {
            shift -= 8;
            for (x=0; x<256; ++x) {
                temp = x > 0 ? pointer[x] - pointer[x-1] : pointer[0] - uleft;
                if (temp > 64) {
                    radixsort(pointer[x] - temp, pointer[x], shift);
                } else if (temp > 1) {
                    // std::sort(array + (pointer[x] - temp), array + pointer[x]);
                    insertion_sort(pointer[x] - temp, pointer[x]);
                }
            }
        }
    }

    // Sorting-related
    bool Buffer::sort() {
        if (empty())
            return true;
        // initialize pointers to serialized PAOs
        uint32_t num = numElements();
        perm_ = reinterpret_cast<char**>(malloc(sizeof(char*) * num));
        uint32_t offset = 0;
        for (uint32_t i = 0; i < num; ++i) {
            perm_[i] = lists_[0]->data_ + offset;
            offset += lists_[0]->sizes_[i];
        }

        // sort elements
//        quicksort(0, num - 1);
        radixsort(0, num, 24);
        return true;
    }

    bool Buffer::merge() {

        if (empty())
            return true;

        // initialize pointers to serialized PAOs
        uint32_t num = numElements();
        perm_ = reinterpret_cast<char**>(malloc(sizeof(char*) * num));

        // aggregate expects permutation pointers
        if (lists_.size() == 1) {
            uint32_t offset = 0;
            for (uint32_t i = 0; i < num; ++i) {
                perm_[i] = lists_[0]->data_ + offset;
                offset += lists_[0]->sizes_[i];
            }
            return true;
        }

        if (numElements() < MAX_ELS_PER_BUFFER)
            aux_list_ = new Buffer::List(/*fd_req = */true);
        else
            aux_list_ = new Buffer::List(/*fd_req = */true,
                    /*AllocType = */List::LARGE_ALLOC);

        // Load each of the list heads into the priority queue
        // keep track of offsets for possible deserialization
        uint32_t nlists = lists_.size();
        uint32_t* heads = new uint32_t[nlists];
        uint32_t* indices = new uint32_t[nlists];
        uint32_t* offsets = new uint32_t[nlists]; 

#ifdef CT_NODE_DEBUG
        for (uint32_t i = 0; i < nlists; ++i)
            checkSortIntegrity(lists_[i]);
#endif  // CT_NODE_DEBUG

        uint32_t num_non_empty_lists = 0;
        for (uint32_t i = 0; i < nlists; ++i) {
            if (lists_[i]->num_ > 0) {
                heads[i] = lists_[i]->hashes_[0];
                indices[i] = 0;
                offsets[i] = 0;
                ++num_non_empty_lists;
            } else {
                heads[i] = 0xffffffff;
            }
        }

        while (num_non_empty_lists) {
            // find min
            uint32_t min = 0xffffffff;
            uint32_t min_index = nlists;
            for (uint32_t i = 0; i < nlists; ++i) {
                if (heads[i] < min) {
                    min = heads[i];
                    min_index = i;
                }
            }
            // copy hash values
            Buffer::List* min_list = lists_[min_index];

            aux_list_->hashes_[aux_list_->num_] = min;
            aux_list_->sizes_[aux_list_->num_] =
                    min_list->sizes_[indices[min_index]];
            perm_[aux_list_->num_] = min_list->data_ +
                    offsets[min_index];
            aux_list_->num_++;

            // update values
            offsets[min_index] += min_list->sizes_[indices[min_index]];
            ++indices[min_index];
            // check if end of list is reached
            if (indices[min_index] < min_list->num_) {
                // update head
                heads[min_index] = min_list->hashes_[indices[min_index]];
            } else {
                heads[min_index] = 0xffffffff;
                --num_non_empty_lists;
            }
        }
        checkSortIntegrity(aux_list_);
        return true;
    }

    bool Buffer::aggregate(bool isSort) {
        // initialize auxiliary buffer
        Buffer aux;
        Buffer::List* a = new List(/*fd_req = */true);
        aux.addList(a);

        // aggregate elements in buffer
        uint32_t lastIndex = 0;

        // set up the input list
        Buffer::List* l;
        if (isSort || lists_.size() == 1)
            l = lists_[0];
        else
            l = aux_list_;

        const Operations* o = node_->tree_->ops;

        // Check that PAOs are actually created
        PartialAgg *thisPAO;
        assert(1 == o->createPAO(NULL, &thisPAO));
        if (max_last_paos_ == 0) {
            PartialAgg* p;
            assert(1 == o->createPAO(NULL, &p));
            lastPAOs_.push_back(p);
            ++max_last_paos_;
        }
        uint32_t num_last_PAOs = 0;

        for (uint32_t i = 1; i < l->num_; ++i) {
            if (l->hashes_[i] == l->hashes_[lastIndex]) {
                // aggregate elements
                if (i == lastIndex + 1) {
                    if (!(o->deserialize(lastPAOs_[0],
                                    perm_[lastIndex],
                                    l->sizes_[lastIndex]))) {
                        fprintf(stderr, "Error at index %d\n", i);
                        assert(false);
                    }
                    num_last_PAOs = 1;
                }
                assert(o->deserialize(thisPAO, perm_[i],
                            l->sizes_[i]));
                // we have now found that the hash of the current PAO is the
                // same as the hash of the lastPAOs. Next we need to check if
                // the keys match...
                bool keys_match = false;
                for (uint32_t j = 0; j < num_last_PAOs; ++j) {
                    if (o->sameKey(thisPAO, lastPAOs_[j])) {
                        o->merge(lastPAOs_[j], thisPAO);
                        keys_match = true;
                        break;
                    }
                }
                // if the keys don't match we add the PAO to the list of PAOS
                // each of which have the same hash, but differing keys
                if (!keys_match) {
                    PartialAgg* t = thisPAO;
                    if (num_last_PAOs == max_last_paos_) {
                        PartialAgg* p;
                        assert(1 == o->createPAO(NULL, &p));
                        lastPAOs_.push_back(p);
                        ++max_last_paos_;
                    }
                    thisPAO = lastPAOs_[num_last_PAOs];
                    lastPAOs_[num_last_PAOs] = t;
                    ++num_last_PAOs;
                }
                continue;
            }

            if (i == lastIndex + 1) {
                a->hashes_[a->num_] = l->hashes_[lastIndex];
                // the size wouldn't have changed
                a->sizes_[a->num_] = l->sizes_[lastIndex];
                //                memset(a->data_ + a->size_, 0, l->sizes_[lastIndex]);
                memcpy(a->data_ + a->size_,
                        reinterpret_cast<void*>(perm_[lastIndex]),
                        l->sizes_[lastIndex]);
                a->size_ += l->sizes_[lastIndex];
                a->num_++;
            } else {
                for (uint32_t j = 0; j < num_last_PAOs; ++j) {
                    uint32_t buf_size = o->getSerializedSize(lastPAOs_[j]);
                    o->serialize(lastPAOs_[j], a->data_ + a->size_, buf_size);

                    a->hashes_[a->num_] = l->hashes_[lastIndex];
                    a->sizes_[a->num_] = buf_size;
                    a->size_ += buf_size;
                    a->num_++;
                }
                num_last_PAOs = 0;
            }
            lastIndex = i;
        }
        // copy the last PAO; TODO: Clean this up!
        // copy hash and size into auxBuffer_
        if (lastIndex == l->num_-1) {
            a->hashes_[a->num_] = l->hashes_[lastIndex];
            // the size wouldn't have changed
            a->sizes_[a->num_] = l->sizes_[lastIndex];
            // memset(a->data_ + a->size_, 0, l->sizes_[lastIndex]);
            memcpy(a->data_ + a->size_,
                    reinterpret_cast<void*>(perm_[lastIndex]),
                    l->sizes_[lastIndex]);
            a->size_ += l->sizes_[lastIndex];
            a->num_++;
        } else {
            for (uint32_t j = 0; j < num_last_PAOs; ++j) {
                uint32_t buf_size = o->getSerializedSize(lastPAOs_[j]);
                o->serialize(lastPAOs_[j], a->data_ + a->size_, buf_size);

                a->hashes_[a->num_] = l->hashes_[lastIndex];
                a->sizes_[a->num_] = buf_size;
                a->size_ += buf_size;
                a->num_++;
            }
            num_last_PAOs = 0;
        }
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d aggregated from %u to %u\n", node_->id_,
                numElements(), aux.numElements());
#endif

        // free pointer memory
        free(perm_);
        if (!isSort && lists_.size() > 1)
            aux_list_->free_buffers();

        o->destroyPAO(thisPAO);

        // clear buffer and shallow copy aux into buffer
        // aux is on stack and will be destroyed

        deallocate();
        lists_ = aux.lists_;
        aux.clear();
        return true;
    }

    // Compression-related    

    bool Buffer::page_out() {
#ifdef ENABLE_COMPRESSION
        if (empty())
            return true;

        if (!page())
            return true;

        std::vector<Buffer::List*> lists_c = lists_copy();
        for (uint32_t i = 0; i < lists_c.size(); ++i) {
            Buffer::List* l = lists_c[i];
            if (l->state_ == Buffer::List::PAGED_OUT)
                continue;

            size_t ret1, ret2, ret3;
            ret1 = write(l->fd_, l->hashes_, l->num_ * sizeof(uint32_t));
            ret2 = write(l->fd_, l->sizes_, l->num_ * sizeof(uint32_t));
            ret3 = write(l->fd_, l->data_, l->size_);
            if (ret1 != l->num_ * sizeof(uint32_t) ||
                    ret2 != l->num_ * sizeof(uint32_t) ||
                    ret3 != l->size_) {
#ifdef ENABLE_ASSERT_CHECKS
                fprintf(stderr, "Node %d page-out fail! Error: %s\n",
                        node_->id_, strerror(errno));
                fprintf(stderr,
                        "HL:%ld;RHL:%ld\nSL:%ld;RSL:%ld\nDL:%ld;RDL:%ld\n",
                        l->num_ * sizeof(uint32_t), ret1,
                        l->num_ * sizeof(uint32_t), ret2,
                        l->size_, ret3);
#endif  // ENABLE_ASSERT_CHECKS
                assert(false);
            }
            l->free_buffers();
            l->state_ = List::PAGED_OUT;
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "%d (%lu), ", i, lists_c[i]->num_);
#endif  // CT_NODE_DEBUG
        }
#endif  // ENABLE_COMPRESSION
        return true;
    }

    bool Buffer::page_in() {
#ifdef ENABLE_COMPRESSION
        if (empty())
            return true;

        // allocate memory for paged-in buffers
        Buffer paged_in;

        std::vector<Buffer::List*> lists_c = lists_copy();
        for (uint32_t i = 0; i < lists_c.size(); ++i) {
            Buffer::List* cl = lists_c[i];
            if (cl->state_ == Buffer::List::IN_MEMORY)
                continue;

            List* l = new List();
            paged_in.addList(l);

            size_t byt, ret, byt_to_read;

            // read in hashes
            assert(lseek(cl->fd_, cl->hash_offset_, SEEK_SET) ==
                    cl->hash_offset_);
            byt = 0;
            byt_to_read = cl->num_ * sizeof(uint32_t);
            while (byt < byt_to_read) {
                if ((ret = read(cl->fd_, l->hashes_, byt_to_read - byt)) <= 0)
                    break;
                byt += ret;
            }
            checkIO(byt, byt_to_read);

            // read in sizes
            assert(lseek(cl->fd_, cl->size_offset_, SEEK_SET) ==
                    cl->size_offset_);
            byt = 0;
            byt_to_read = cl->num_ * sizeof(uint32_t);
            while (byt < byt_to_read) {
                if ((ret = read(cl->fd_, l->sizes_, byt_to_read - byt)) <= 0)
                    break;
                byt += ret;
            }
            checkIO(byt, byt_to_read);

            // read in data
            assert(lseek(cl->fd_, cl->data_offset_, SEEK_SET) ==
                    cl->data_offset_);
            byt = 0;
            byt_to_read = cl->size_;
            while (byt < byt_to_read) {
                if ((ret = read(cl->fd_, l->data_, byt_to_read - byt)) <= 0)
                    break;
                byt += ret;
            }
            checkIO(byt, byt_to_read);

            cl->free_buffers();
            cl->hashes_ = l->hashes_;
            cl->sizes_ = l->sizes_;
            cl->data_ = l->data_;
            cl->state_ = List::IN_MEMORY;
        }
        // clear paged_in so lists won't be deallocated on return
        paged_in.clear();
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "paged_in node %d; n: %u\n",
                node_->id_, numElements());
#endif  // CT_NODE_DEBUG
#endif  // ENABLE_COMPRESSION
        return true;
    }

    void Buffer::set_pageable(bool flag) {
        // TODO Synchrnonization required
        pageable_ = flag;
    }

    bool Buffer::page() {
/*
        FILE *file = fopen("/proc/self/statm", "r");
        unsigned long vm = 0, res = 0;
        if (file) {
            fscanf (file, "%lu %lu", &vm, &res);
        }
        fclose(file);
        if (res << 2 > 31457280) {
            return true;
        }
        return false;
*/
        if (node_->level() > 0)
            return false;
        return true;
    }

    void Buffer::checkIO(size_t done, size_t req) {
        if (done != req) {
#ifdef ENABLE_ASSERT_CHECKS
            fprintf(stderr, "Node %d I/O fail! Error: %s\n",
                    node_->id_, strerror(errno));
            fprintf(stderr, "Requested: %ld; Performed: %ld\n",
                    req, done);
#endif  // ENABLE_ASSERT_CHECKS
            assert(false);
        }
    }

    bool Buffer::checkSortIntegrity(List* l) {
#ifdef ENABLE_INTEGRITY_CHECK
        for (uint32_t i = 0; i < l->num_; ++i) {
            assert(l->hashes_[i] > 0);
            assert(l->sizes_[i] > 0);
            if (i < l->num_ - 1) {
                if (l->hashes_[i] > l->hashes_[i+1])
                    assert(false);
                if (l->sizes_[i] != l->sizes_[i+1])
                    assert(false);
            }
        }
#endif
        return true;
    }
}
