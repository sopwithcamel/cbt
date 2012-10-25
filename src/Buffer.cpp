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
            egressible_(true),
            queueStatus_(NONE) {
        pthread_spin_init(&queueStatusLock_, PTHREAD_PROCESS_PRIVATE);
    }

    Buffer::~Buffer() {
        deallocate();
        pthread_spin_destroy(&queueStatusLock_);
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

    bool Buffer::full() const {
        return (numElements() > EMPTY_THRESHOLD);
    }

    bool Buffer::available_for_insertion() {
        // if status is EGRESS or NONE, then 
        return (getQueueStatus() >= EGRESS);
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

    Action Buffer::getQueueStatus() {
        pthread_spin_lock(&queueStatusLock_);
        Action ret = queueStatus_;
        pthread_spin_unlock(&queueStatusLock_);
        return ret;
    }

    void Buffer::setQueueStatus(const Action& act) {
        pthread_spin_lock(&queueStatusLock_);
        queueStatus_ = act;
        pthread_spin_unlock(&queueStatusLock_);
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

    bool Buffer::merge() {
        std::priority_queue<MergeElement,
                std::vector<MergeElement>,
                MergeComparator> queue;

        if (lists_.size() == 1 || empty())
            return true;

        // initialize aux buffer
        Buffer aux;
        List* a;
        if (numElements() < MAX_ELS_PER_BUFFER)
            a = aux.addList();
        else
            a = aux.addList(/*large buffer=*/true);

        // Load each of the list heads into the priority queue
        // keep track of offsets for possible deserialization
        for (uint32_t i = 0; i < lists_.size(); ++i) {
            if (lists_[i]->num_ > 0) {
                MergeElement* mge = new MergeElement(lists_[i]);
                queue.push(*mge);
            }
        }

        while (!queue.empty()) {
            MergeElement n = queue.top();
            queue.pop();

            // copy hash values
            a->hashes_[a->num_] = n.hash();
            uint32_t buf_size = n.size();
            a->sizes_[a->num_] = buf_size;
            // memset(a->data_ + a->size_, 0, buf_size);
            memmove(a->data_ + a->size_,
                    reinterpret_cast<void*>(n.data()), buf_size);
            a->size_ += buf_size;
            a->num_++;
/*
            if (a->num_ >= MAX_ELS_PER_BUFFER) {
                fprintf(stderr, "Num elements: %u\n", a->num_);
                assert(false);
            }
*/
            // increment n pointer and re-insert n into prioQ
            if (n.next())
                queue.push(n);
        }

        // clear buffer and copy over aux.
        // aux itself is on the stack and will be destroyed
        deallocate();
        lists_ = aux.lists_;
        aux.clear();

#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d merged; new size: %d\n", node_->id(),
                numElements());
#endif  // CT_NODE_DEBUG
        return true;
    }

    bool Buffer::aggregate(bool isRoot) {
        const Operations* ops = node_->tree_->ops();

        PartialAgg *lastPAO, *thisPAO;
        // Check that PAOs are actually created
        assert(1 == ops->createPAO(NULL, &lastPAO));
        assert(1 == ops->createPAO(NULL, &thisPAO));

        if (empty())
            return true;
        // initialize aux buffer
        Buffer aux;

        if (isRoot) { 
            List* a = aux.addList();

            // aggregate elements in buffer
            uint32_t lastIndex = 0;
            Buffer::List* l = lists_[0];
            for (uint32_t i = 1; i < l->num_; ++i) {
                if (l->hashes_[i] == l->hashes_[lastIndex]) {
                    // aggregate elements
                    if (i == lastIndex + 1) {
                        if (!(ops->deserialize(lastPAO, perm_[lastIndex],
                                        l->sizes_[lastIndex]))) {
                            fprintf(stderr, "Error at index %d\n", i);
                            assert(false);
                        }
                    }
                    assert(ops->deserialize(thisPAO, perm_[i], l->sizes_[i]));
                    if (ops->sameKey(thisPAO, lastPAO)) {
                        ops->merge(lastPAO, thisPAO);
                        continue;
                    }
                }
                // copy hash and size into auxBuffer_
                a->hashes_[a->num_] = l->hashes_[lastIndex];
                if (i == lastIndex + 1) {
                    // the size wouldn't have changed
                    a->sizes_[a->num_] = l->sizes_[lastIndex];
                    //                memset(a->data_ + a->size_, 0, l->sizes_[lastIndex]);
                    memmove(a->data_ + a->size_,
                            reinterpret_cast<void*>(perm_[lastIndex]),
                            l->sizes_[lastIndex]);
                    a->size_ += l->sizes_[lastIndex];
                } else {
                    uint32_t buf_size = ops->getSerializedSize(lastPAO);
                    ops->serialize(lastPAO, a->data_ + a->size_, buf_size);

                    a->sizes_[a->num_] = buf_size;
                    a->size_ += buf_size;
                }
                a->num_++;
                lastIndex = i;
            }
            // copy the last PAO; TODO: Clean this up!
            // copy hash and size into auxBuffer_
            if (lastIndex == l->num_-1) {
                a->hashes_[a->num_] = l->hashes_[lastIndex];
                // the size wouldn't have changed
                a->sizes_[a->num_] = l->sizes_[lastIndex];
                // memset(a->data_ + a->size_, 0, l->sizes_[lastIndex]);
                memmove(a->data_ + a->size_,
                        reinterpret_cast<void*>(perm_[lastIndex]),
                        l->sizes_[lastIndex]);
                a->size_ += l->sizes_[lastIndex];
            } else {
                uint32_t buf_size = ops->getSerializedSize(lastPAO);
                ops->serialize(lastPAO, a->data_ + a->size_, buf_size);

                a->hashes_[a->num_] = l->hashes_[lastIndex];
                a->sizes_[a->num_] = buf_size;
                a->size_ += buf_size;
            }
            a->num_++;

            // free pointer memory
            free(perm_);
        } else { // aggregating merged buffer
            List* a;
            if (numElements() < MAX_ELS_PER_BUFFER)
                a = aux.addList();
            else
                a = aux.addList(/*isLarge=*/true);

            Buffer::List* l = lists_[0];
            uint32_t num = numElements();
            uint32_t numMerged = 0;
            uint32_t offset = 0;

            uint32_t lastIndex = 0;
            offset += l->sizes_[0];
            uint32_t lastOffset = 0;

            // aggregate elements in buffer
            for (uint32_t i = 1; i < num; ++i) {
                if (l->hashes_[i] == l->hashes_[lastIndex]) {
                    if (numMerged == 0) {
                        if (!(ops->deserialize(lastPAO, l->data_ + lastOffset,
                                l->sizes_[lastIndex]))) {
                            fprintf(stderr, "Can't deserialize at %u, index: %u\n",
                                    lastOffset, lastIndex);
                            assert(false);
                        }
                    }
                    if (!(ops->deserialize(thisPAO, l->data_ + offset,
                                    l->sizes_[i]))) {
                        fprintf(stderr, "Can't deserialize at %u, index: %u\n",
                                offset, i);
                        assert(false);
                    }
                    if (ops->sameKey(thisPAO, lastPAO)) {
                        ops->merge(lastPAO, thisPAO);
                        numMerged++;
                        offset += l->sizes_[i];
                        continue;
                    }
                }
                a->hashes_[a->num_] = l->hashes_[lastIndex];
                if (numMerged == 0) {
                    uint32_t buf_size = l->sizes_[lastIndex];
                    a->sizes_[a->num_] = l->sizes_[lastIndex];
                    // memset(a->data_ + a->size_, 0, l->sizes_[lastIndex]);
                    memmove(a->data_ + a->size_,
                            reinterpret_cast<void*>(l->data_ + lastOffset),
                            l->sizes_[lastIndex]);
                    a->size_ += buf_size;
                } else {
                    uint32_t buf_size = ops->getSerializedSize(lastPAO);
                    ops->serialize(lastPAO, a->data_ + a->size_, buf_size);
                    a->sizes_[a->num_] = buf_size;
                    a->size_ += buf_size;
                }
                a->num_++;
                numMerged = 0;
                lastIndex = i;
                lastOffset = offset;
                offset += l->sizes_[i];
            }
            // copy last PAO
            a->hashes_[a->num_] = l->hashes_[lastIndex];
            if (numMerged == 0) {
                uint32_t buf_size = l->sizes_[lastIndex];
                a->sizes_[a->num_] = l->sizes_[lastIndex];
                // memset(a->data_ + a->size_, 0, l->sizes_[lastIndex]);
                memmove(a->data_ + a->size_,
                        reinterpret_cast<void*>(l->data_ + lastOffset),
                        l->sizes_[lastIndex]);
                a->size_ += buf_size;
            } else {
                uint32_t buf_size = ops->getSerializedSize(lastPAO);
                ops->serialize(lastPAO, a->data_ + a->size_, buf_size);
                a->sizes_[a->num_] = buf_size;
                a->size_ += buf_size;
            }
            a->num_++;
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Node %d aggregated from %u to %u\n", node_->id_,
                    numElements(), aux.numElements());
#endif
        }

        // clear buffer and shallow copy aux into buffer
        // aux is on stack and will be destroyed
        deallocate();
        lists_ = aux.lists_;
        aux.clear();

        ops->destroyPAO(lastPAO);
        ops->destroyPAO(thisPAO);

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
            } 
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "ingressed node %d; n: %u\n", node_->id_,
                    numElements());
#endif  // CT_NODE_DEBUG

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
