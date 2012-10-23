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
#include <stdlib.h>
#include <unistd.h>
#include <sstream>
#include "Buffer.h"
#include "CompressTree.h"
// #include "compsort.h"
// #include "rle.h"
#include "snappy.h"

namespace cbt {
    uint64_t Buffer::List::allocated_lists = 0;

    Buffer::List::List() :
            hashes_(NULL),
            sizes_(NULL),
            data_(NULL),
            num_(0),
            size_(0),
            state_(IN),
            c_hashlen_(0),
            c_sizelen_(0),
            c_datalen_(0) {
    }

    Buffer::List::~List() {
        deallocate();
    }

    void Buffer::List::allocate(bool isLarge) {
        uint32_t nel = cbt::MAX_ELS_PER_BUFFER;
        uint32_t buf = cbt::BUFFER_SIZE;
        if (isLarge) {
            nel *= 2;
            buf *= 2;
        }
        hashes_ = reinterpret_cast<uint32_t*>(malloc(sizeof(uint32_t) * nel));
        sizes_ = reinterpret_cast<uint32_t*>(malloc(sizeof(uint32_t) * nel));
        data_ = reinterpret_cast<char*>(malloc(buf));
    }

    void Buffer::List::deallocate() {
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
        state_ = IN;
    }

    Buffer::Buffer() :
            kPagingEnabled(false),
            egressible_(true) {
    }

    Buffer::~Buffer() {
        deallocate();
    }

    Buffer::List* Buffer::addList(bool isLarge/* = false */) {
        List *l = new List();
        l->allocate(isLarge);
        lists_.push_back(l);
        return l;
    }

    void Buffer::delList(uint32_t ind) {
        if (ind < lists_.size()) {
            delete lists_[ind];
            lists_.erase(lists_.begin() + ind);
        }
    }

    void Buffer::addList(Buffer::List* l) {
        lists_.push_back(l);
    }

    void Buffer::clear() {
        lists_.clear();
    }

    void Buffer::deallocate() {
        for (uint32_t i = 0; i < lists_.size(); ++i)
            delete lists_[i];
        lists_.clear();
    }

    bool Buffer::empty() const {
        return (numElements() == 0);
    }

    uint32_t Buffer::numElements() const {
        uint32_t num = 0;
        for (uint32_t i = 0; i < lists_.size(); ++i)
            num += lists_[i]->num_;
        return num;
    }

    void Buffer::setParent(Node* n) {
        node_ = n;
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

        // quicksort elements
        quicksort(0, num - 1);
        return true;
    }

    // Compression-related    

    bool Buffer::egress() {
        if (!empty()) {
            // allocate memory for one list
            Buffer egressed;

            for (uint32_t i = 0; i < lists_.size(); ++i) {
                Buffer::List* in_list = lists_[i];
                if (in_list->state_ == Buffer::List::OUT)
                    continue;

                // Perform compression or paging out
                if (!kPagingEnabled) {
                    List* out_list = egressed.addList();
                    snappy::RawCompress((const char*)in_list->hashes_,
                            in_list->num_ * sizeof(uint32_t),
                            reinterpret_cast<char*>(out_list->hashes_),
                            &in_list->c_hashlen_);
                    snappy::RawCompress((const char*)in_list->sizes_,
                            in_list->num_ * sizeof(uint32_t),
                            reinterpret_cast<char*>(out_list->sizes_),
                            &in_list->c_sizelen_);
                    /*
                       compsort::compress(l->hashes_, l->num_,
                       cl->hashes_, (uint32_t&)l->c_hashlen_);
                       rle::encode(l->sizes_, l->num_, cl->sizes_,
                       (uint32_t&)l->c_sizelen_);
                     */
                    snappy::RawCompress(in_list->data_, in_list->size_,
                            out_list->data_,
                            &in_list->c_datalen_);
                    in_list->deallocate();
                    in_list->hashes_ = out_list->hashes_;
                    in_list->sizes_ = out_list->sizes_;
                    in_list->data_ = out_list->data_;
                    in_list->state_ = List::OUT;
#ifdef CT_NODE_DEBUG
                    fprintf(stderr, "compessed list %d in node %d\n",
                            i, node_->id_);
#endif  // CT_NODE_DEBUG
                } else { // Paging
                    size_t ret1, ret2, ret3;
                    ret1 = fwrite(in_list->hashes_, 1,
                            in_list->num_ * sizeof(uint32_t), f_);
                    ret2 = fwrite(in_list->sizes_, 1,
                            in_list->num_ * sizeof(uint32_t), f_);
                    ret3 = fwrite(in_list->data_, 1,
                            in_list->size_, f_);
                    if (ret1 != in_list->num_ * sizeof(uint32_t) ||
                            ret2 != in_list->num_ * sizeof(uint32_t) ||
                            ret3 != in_list->size_) {
                        assert(false);
#ifdef ENABLE_ASSERT_CHECKS
                        fprintf(stderr, "Node %d page-out fail! Error: %s\n",
                                node_->id_, strerror(errno));
                        fprintf(stderr,
                                "HL:%ld;RHL:%ld\nSL:%ld;RSL:%ld\nDL:%ld;RDL:%ld\n",
                                in_list->num_ * sizeof(uint32_t), ret1,
                                in_list->num_ * sizeof(uint32_t), ret2,
                                in_list->size_, ret3);
#endif  // ENABLE_ASSERT_CHECKS
                    }
                    in_list->deallocate();
                    in_list->state_ = List::OUT;
#ifdef CT_NODE_DEBUG
                    fprintf(stderr, "%d (%lu), ", i, lists_[i]->num_);
#endif  // CT_NODE_DEBUG
                }
            }
            // clear egressed list so lists won't be deallocated on return
            egressed.clear();
        }
        return true;
    }

    bool Buffer::ingress() {
        if (!empty()) {
            // allocate memory for ingressed buffers
            Buffer ingressed;

            // set file pointer to beginning of file
            if (kPagingEnabled)
                rewind(f_);

            for (uint32_t i = 0; i < lists_.size(); ++i) {
                Buffer::List* out_list = lists_[i];
                if (out_list->state_ == Buffer::List::IN)
                    continue;
                // latest added list
                Buffer::List* in_list = ingressed.addList();

                if (!kPagingEnabled) {
                    snappy::RawUncompress((const char*)out_list->hashes_,
                            out_list->c_hashlen_,
                            reinterpret_cast<char*>(in_list->hashes_));
                    snappy::RawUncompress((const char*)out_list->sizes_,
                            out_list->c_sizelen_,
                            reinterpret_cast<char*>(in_list->sizes_));
                    /*
                       uint32_t siz;
                       compsort::ingress(cl->hashes_, (uint32_t)cl->c_hashlen_,
                       l->hashes_, siz);
                       rle::decode(cl->sizes_, (uint32_t)cl->c_sizelen_,
                       l->sizes_, siz);
                     */
                    snappy::RawUncompress(out_list->data_,
                            out_list->c_datalen_,
                            in_list->data_);
                } else { // Paging
                    size_t ret1, ret2, ret3;
                    ret1 = fread(in_list->hashes_, 1,
                            out_list->num_ * sizeof(uint32_t), f_);
                    ret2 = fread(in_list->sizes_, 1,
                            out_list->num_ * sizeof(uint32_t), f_);
                    ret3 = fread(in_list->data_, 1,
                            out_list->size_, f_);
                    if (ret1 != out_list->num_ * sizeof(uint32_t) ||
                            ret2 != out_list->num_ * sizeof(uint32_t) ||
                            ret3 != out_list->size_) {
#ifdef ENABLE_ASSERT_CHECKS
                        fprintf(stderr, "Node %d page-in fail! Error: %s\n",
                                node_->id_, strerror(errno));
                        fprintf(stderr,
                                "HL:%ld;RHL:%ld\nSL:%ld;RSL:%ld\n\
                                DL:%ld;RDL:%ld\n",
                                out_list->num_ * sizeof(uint32_t), ret1,
                                out_list->num_ * sizeof(uint32_t), ret2,
                                out_list->size_, ret3);
#endif  // ENABLE_ASSERT_CHECKS
                        assert(false);
                    }
                }

                out_list->deallocate();
                out_list->hashes_ = in_list->hashes_;
                out_list->sizes_ = in_list->sizes_;
                out_list->data_ = in_list->data_;
                out_list->state_ = List::OUT;
                // clear ingressed so lists won't be deallocated on return
                ingressed.clear();
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "ingressed node %d; n: %u\n", node_->id_,
                        numElements());
#endif  // CT_NODE_DEBUG
            } 

            // set file pointer to beginning of file
            if (kPagingEnabled)
                rewind(f_);
        }
        return true;
    }

    void Buffer::setEgressible(bool flag) {
        // TODO Synchrnonization required
        egressible_ = flag;
    }

    void Buffer::setupPaging() {
        if (kPagingEnabled) {
            std::stringstream fileName;
            fileName << "/localfs/hamur/minni_data/";
            fileName << node_->id_;
            fileName << ".buf";
            f_ = fopen(fileName.str().c_str(), "w+");
            if (f_ == NULL) {
                fprintf(stderr, "Error opening file: %s\n", strerror(errno));
                assert(false);
            }
        }
    }

    void Buffer::cleanupPaging() {
        if (kPagingEnabled)
            fclose(f_);
    }
}
