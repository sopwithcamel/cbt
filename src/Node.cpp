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
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <queue>
#include <vector>

#include "CompressTree.h"
#include "HashUtil.h"
#include "Node.h"
#include "Slaves.h"

namespace cbt {
    Node::Node(CompressTree* tree, uint32_t level) :
            tree_(tree),
            input_buffer_(NULL),
            empty_buffer_(NULL),
            empty_buffer_available_(true),
            both_buffers_full_(false),
            level_(level),
            parent_(NULL) {
        id_ = tree_->nodeCtr++;

        input_buffer_ = new Buffer();
        input_buffer_->setParent(this);
        input_buffer_->setupPaging();

        empty_buffer_ = new Buffer();
        empty_buffer_->setParent(this);
        empty_buffer_->setupPaging();

        pthread_mutex_init(&emptyMutex_, NULL);
        pthread_cond_init(&emptyCond_, NULL);

        pthread_mutex_init(&sortMutex_, NULL);
        pthread_cond_init(&sortCond_, NULL);

        pthread_mutex_init(&xgressMutex_, NULL);
        pthread_cond_init(&xgressCond_, NULL);

        pthread_spin_init(&buffer_availability_lock_,
                PTHREAD_PROCESS_PRIVATE);
    }

    Node::~Node() {
        pthread_mutex_destroy(&sortMutex_);
        pthread_cond_destroy(&sortCond_);

        pthread_mutex_destroy(&xgressMutex_);
        pthread_cond_destroy(&xgressCond_);

        if (input_buffer_) {
            input_buffer_->cleanupPaging();
            delete input_buffer_;
            input_buffer_ = NULL;
        }
        pthread_spin_destroy(&buffer_availability_lock_);
    }

    bool Node::insert(PartialAgg* agg) {
        uint32_t buf_size = tree_->ops()->getSerializedSize(agg);
        const char* key = tree_->ops()->getKey(agg);

        // copy into Buffer fields
        Buffer::List* l = input_buffer_->lists_[0];
        l->hashes_[l->num_] = HashUtil::MurmurHash(key, strlen(key), 42);
        l->sizes_[l->num_] = buf_size;
        // is this required?
//        memset(l->data_ + l->size_, 0, buf_size);
        tree_->ops()->serialize(agg, l->data_ + l->size_,
                buf_size);
        l->size_ += buf_size;
        l->num_++;
#ifdef ENABLE_COUNTERS
        tree_->monitor_->numElements++;
#endif
        return true;
    }

    bool Node::isLeaf() const {
        if (children_.empty())
            return true;
        return false;
    }

    bool Node::isRoot() const {
        if (parent_ == NULL)
            return true;
        return false;
    }

    bool Node::emptyOrEgress() {
        bool ret = true;
        if (tree_->emptyType_ == ALWAYS || input_buffer_->full()) {
            switchBuffers();
            ret = spillBuffer();
        } else {
            schedule(EGRESS);
        }
        return ret;
    }

    bool Node::spillBuffer() {
        schedule(INGRESS);
        return true;
    }

    bool Node::emptyBuffer() {
        uint32_t curChild = 0;
        uint32_t curElement = 0;
        uint32_t lastElement = 0;

        Buffer* buffer;
        if (isRoot())
            buffer = input_buffer_;
        else
            buffer = empty_buffer_;

        /* if i am a leaf node, queue up for action later after all the
         * internal nodes have been processed */
        if (isLeaf()) {
            /* this may be called even when buffer is not full (when flushing
             * all buffers at the end). */
            if (buffer->full() || isRoot()) {
                tree_->addLeafToEmpty(this);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Leaf node %d added to full-leaf-list\
                        %u/%u\n", id_, buffer->numElements(),
                        EMPTY_THRESHOLD);
#endif
            } else {  // compress
                schedule(EGRESS);
            }
            return true;
        }

        if (buffer->empty()) {
            for (curChild = 0; curChild < children_.size(); curChild++) {
                children_[curChild]->emptyOrEgress();
            }
        } else {
            checkSerializationIntegrity();
            Buffer::List* l = buffer->lists_[0];
            // find the first separator strictly greater than the first element
            while (l->hashes_[curElement] >=
                    children_[curChild]->separator_) {
                children_[curChild]->emptyOrEgress();
                curChild++;
#ifdef ENABLE_ASSERT_CHECKS
                if (curChild >= children_.size()) {
                    fprintf(stderr,
                            "Node: %d: Can't place %u among children\n", id_,
                            l->hashes_[curElement]);
                    checkIntegrity();
                    assert(false);
                }
#endif
            }
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Node: %d: first node chosen: %d (sep: %u, \
                child: %d); first element: %u\n", id_, children_[curChild]->id_,
                    children_[curChild]->separator_, curChild, l->hashes_[0]);
#endif
            uint32_t num = buffer->numElements();
#ifdef ENABLE_ASSERT_CHECKS
            // there has to be a single list in the buffer at this point
            assert(buffer->lists_.size() == 1);
#endif
            while (curElement < num) {
                if (l->hashes_[curElement] >=
                        children_[curChild]->separator_) {
                    /* this separator is the largest separator that is not greater
                     * than *curHash. This invariant needs to be maintained.
                     */
                    if (curElement > lastElement) {
                        // copy elements into child
                        children_[curChild]->copyIntoBuffer(l, lastElement,
                                curElement - lastElement);
#ifdef CT_NODE_DEBUG
                        fprintf(stderr, "Copied %u elements into node %d\
                                 list:%lu\n",
                                curElement - lastElement,
                                children_[curChild]->id_,
                                children_[curChild]->input_buffer_->lists_.size()-1);
#endif
                        lastElement = curElement;
                    }
                    // skip past all separators not greater than current hash
                    while (l->hashes_[curElement]
                            >= children_[curChild]->separator_) {
                        children_[curChild]->emptyOrEgress();
                        curChild++;
#ifdef ENABLE_ASSERT_CHECKS
                        if (curChild >= children_.size()) {
                            fprintf(stderr, "Can't place %u among children\n",
                                    l->hashes_[curElement]);
                            assert(false);
                        }
#endif
                    }
                }
                // proceed to next element
                assert(l->sizes_[curElement] != 0);
                curElement++;
            }

            // copy remaining elements into child
            if (curElement >= lastElement) {
                // copy elements into child
                children_[curChild]->copyIntoBuffer(l, lastElement,
                        curElement - lastElement);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Copied %u elements into node %d; \
                        list: %lu\n",
                        curElement - lastElement,
                        children_[curChild]->id_,
                        children_[curChild]->input_buffer_->lists_.size()-1);
#endif
                children_[curChild]->emptyOrEgress();
                curChild++;
            }
            // empty or egress any remaining children
            while (curChild < children_.size()) {
                children_[curChild]->emptyOrEgress();
                curChild++;
            }

            // reset
            l->setEmpty();

            if (!isRoot()) {
                buffer->deallocate();
            }
        }
        // Split leaves can cause the number of children to increase. Check.
        if (children_.size() > tree_->b_) {
            splitNonLeaf();
        }
        return true;
    }

    /* A leaf is split by moving half the elements of the buffer into a
     * new leaf and inserting a median value as the separator element into the
     * parent */
    Node* Node::splitLeaf() {
        checkIntegrity();
        Buffer* buffer;
        if (isRoot())
            buffer = input_buffer_;
        else
            buffer = empty_buffer_;

        // select splitting index
        uint32_t num = buffer->numElements();
        uint32_t splitIndex = num/2;
        Buffer::List* l = buffer->lists_[0];
        while (l->hashes_[splitIndex] == l->hashes_[splitIndex-1]) {
            splitIndex++;
#ifdef ENABLE_ASSERT_CHECKS
            if (splitIndex == num) {
                assert(false);
            }
#endif
        }

        checkSerializationIntegrity();
        // create new leaf
        Node* newLeaf = new Node(tree_, 0);
        newLeaf->copyIntoBuffer(l, splitIndex, num - splitIndex);
        newLeaf->separator_ = separator_;

        // modify this leaf properties

        // copy the first half into another list in this buffer and delete
        // the original list
        copyIntoBuffer(l, 0, splitIndex);
        separator_ = l->hashes_[splitIndex];
        // delete the old list
        buffer->delList(0);
        l = buffer->lists_[0];

        // check integrity of both leaves
        newLeaf->checkIntegrity();
        checkIntegrity();
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d splits to Node %d: new indices: %u and\
                %u; new separators: %u and %u\n", id_, newLeaf->id_,
                l->num_, newLeaf->input_buffer_->lists_[0]->num_, separator_,
                newLeaf->separator_);
#endif

        // if leaf is also the root, create new root
        if (isRoot()) {
            buffer->setEgressible(true);
            tree_->createNewRoot(newLeaf);
        } else {
            parent_->addChild(newLeaf);
        }
        return newLeaf;
    }

    bool Node::copyIntoBuffer(Buffer::List* parent_list, uint32_t index,
            uint32_t num) {
        // check if the node is still queued up for a previous compression
        wait(EGRESS);

        // calculate offset
        uint32_t offset = 0;
        uint32_t num_bytes = 0;
        for (uint32_t i = 0; i < index; ++i) {
            offset += parent_list->sizes_[i];
        }
        for (uint32_t i = 0; i < num; ++i) {
            num_bytes += parent_list->sizes_[index + i];
        }
#ifdef ENABLE_ASSERT_CHECKS
        assert(parent_list->state_ == Buffer::List::IN);
        if (num_bytes >= BUFFER_SIZE) {
            fprintf(stderr, "Node: %d, buf: %d\n", id_,
                    num_bytes);
            assert(false);
        }
#endif
        // allocate a new List in the buffer and copy data into it
        Buffer::List* l = input_buffer_->addList();
        // memset(l->hashes_, 0, num * sizeof(uint32_t));
        memmove(l->hashes_, parent_list->hashes_ + index,
                num * sizeof(uint32_t));
        // memset(l->sizes_, 0, num * sizeof(uint32_t));
        memmove(l->sizes_, parent_list->sizes_ + index,
                num * sizeof(uint32_t));
        // memset(l->data_, 0, num_bytes);
        memmove(l->data_, parent_list->data_ + offset,
                num_bytes);
        l->num_ = num;
        l->size_ = num_bytes;
        checkSerializationIntegrity(input_buffer_->lists_.size()-1);
        return true;
    }

    void Node::switchBuffers() {
        // check if the other buffer is available for insertion (or if it's
        // still being emptied). If available, switch buffers and set as
        // unavailable; this will be reset by the emptying thread. If not, then
        // set both buffers as full. This prevents the parent from emptying;
        // this is also reset by the emptying threads.
        if (empty_buffer_->available_for_insertion()) {
            Buffer* temp = input_buffer_;
            input_buffer_ = empty_buffer_;
            empty_buffer_ = temp;        
        } else {
            set_both_buffers_full(true);
        }
    }

    bool Node::both_buffers_full() {
        pthread_spin_lock(&buffer_availability_lock_);
        bool ret = both_buffers_full_;
        pthread_spin_unlock(&buffer_availability_lock_);
        return ret;
    }

    void Node::set_both_buffers_full(bool both_full) {
        pthread_spin_lock(&buffer_availability_lock_);
        both_buffers_full_ = both_full;
        pthread_spin_unlock(&buffer_availability_lock_);
    }

    bool Node::addChild(Node* newNode) {
        uint32_t i;
        // insert separator value

        // find position of insertion
        std::vector<Node*>::iterator it = children_.begin();
        for (i = 0; i < children_.size(); ++i) {
            if (newNode->separator_ > children_[i]->separator_)
                continue;
            break;
        }
        it += i;
        children_.insert(it, newNode);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node: %d: Node %d added at pos %u, [", id_,
                newNode->id_, i);
        for (uint32_t j = 0; j < children_.size(); ++j)
            fprintf(stderr, "%d, ", children_[j]->id_);
        fprintf(stderr, "], num children: %ld\n", children_.size());
#endif
        // set parent
        newNode->parent_ = this;

        return true;
    }

    bool Node::splitNonLeaf() {
        // ensure node's buffer is empty
#ifdef ENABLE_ASSERT_CHECKS
        if (!empty_buffer_->empty()) {
            fprintf(stderr, "Node %d has non-empty buffer\n", id_);
            assert(false);
        }
#endif
        // create new node
        Node* newNode = new Node(tree_, level_);
        // move the last floor((b+1)/2) children to new node
        int newNodeChildIndex = (children_.size() + 1) / 2;
#ifdef ENABLE_ASSERT_CHECKS
        if (children_[newNodeChildIndex]->separator_ <=
                children_[newNodeChildIndex-1]->separator_) {
            fprintf(stderr, "%d sep is %u and %d sep is %u\n",
                    newNodeChildIndex,
                    children_[newNodeChildIndex]->separator_,
                    newNodeChildIndex-1,
                    children_[newNodeChildIndex-1]->separator_);
            assert(false);
        }
#endif
        // add children to new node
        for (uint32_t i = newNodeChildIndex; i < children_.size(); ++i) {
            newNode->children_.push_back(children_[i]);
            children_[i]->parent_ = newNode;
        }
        // set separator
        newNode->separator_ = separator_;

        // remove children from current node
        std::vector<Node*>::iterator it = children_.begin() +
                newNodeChildIndex;
        children_.erase(it, children_.end());

        // median separator from node
        separator_ = children_[children_.size()-1]->separator_;
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "After split, %d: [", id_);
        for (uint32_t j = 0; j < children_.size(); ++j)
            fprintf(stderr, "%u, ", children_[j]->separator_);
        fprintf(stderr, "] and %d: [", newNode->id_);
        for (uint32_t j = 0; j < newNode->children_.size(); ++j)
            fprintf(stderr, "%u, ", newNode->children_[j]->separator_);
        fprintf(stderr, "]\n");

        fprintf(stderr, "Children, %d: [", id_);
        for (uint32_t j = 0; j < children_.size(); ++j)
            fprintf(stderr, "%d, ", children_[j]->id_);
        fprintf(stderr, "] and %d: [", newNode->id_);
        for (uint32_t j = 0; j < newNode->children_.size(); ++j)
            fprintf(stderr, "%d, ", newNode->children_[j]->id_);
        fprintf(stderr, "]\n");
#endif

        if (isRoot()) {
            input_buffer_->setEgressible(true);
            empty_buffer_->setEgressible(true);
            empty_buffer_->deallocate();
            return tree_->createNewRoot(newNode);
        } else {
            return parent_->addChild(newNode);
        }
    }

    uint32_t Node::level() const {
        return level_;
    }

    uint32_t Node::id() const {
        return id_;
    }

    void Node::done(const Action& act) {
        switch(act) {
            case EGRESS:
            case INGRESS:
            case INGRESS_ONLY:
                {
                    // Signal that we're done comp/decomp
                    pthread_mutex_unlock(&xgressMutex_);
                    pthread_cond_signal(&xgressCond_);
                    pthread_mutex_unlock(&xgressMutex_);
                }
                break;
            case MERGE:
                {
                    // Signal that we're done sorting
                    pthread_mutex_lock(&sortMutex_);
                    pthread_cond_signal(&sortCond_);
                    pthread_mutex_unlock(&sortMutex_);
                }
                break;
            case EMPTY:
                {
                }
                break;
            case NONE:
                {
                    assert(false && "Can't signal NONE");
                }
                break;
        }
    }

    void Node::schedule(const Action& act) {
        switch(act) {
            case EGRESS:
            case INGRESS:
            case INGRESS_ONLY:
                {
                    bool add;
                    if (!input_buffer_->egressible_) {
                        fprintf(stderr, "Node %d not xgressible\n", id_);
                        return;
                    }
                    if (act == EGRESS) {
                        // check if node has to be added on queue
                        add = checkEgress();
                    } else if (act == INGRESS || act == INGRESS_ONLY) {
                        // check if node has to be added on queue
                        add = checkIngress();
                    } else {
                        assert(false && "Invalid compress action");
                    }

                    if (add) {
                        input_buffer_->setQueueStatus(act);
                        if (act == EGRESS)
                            tree_->compressor_->addNode(this, INSERT_BUFFER);
                        else if (act == INGRESS || act == INGRESS_ONLY)
                            tree_->compressor_->addNode(this, EMPTY_BUFFER);
                        tree_->compressor_->wakeup();
                    }
                }
                break;
            case SORT:
                {
                    input_buffer_->setQueueStatus(SORT);
                    // add node to merger
                    tree_->sorter_->addNode(this, INSERT_BUFFER);
                    tree_->sorter_->wakeup();
                }
                break;
            case MERGE:
                {
                    input_buffer_->setQueueStatus(MERGE);
                    // add node to merger
                    tree_->merger_->addNode(this, EMPTY_BUFFER);
                    tree_->merger_->wakeup();
                }
                break;
            case EMPTY:
                {
                    input_buffer_->setQueueStatus(act);
                    // add node to empty
                    tree_->emptier_->addNode(this, EMPTY_BUFFER);
                    tree_->emptier_->wakeup();
                }
                break;
            case NONE:
                {
                    assert(false && "Can't schedule NONE");
                }
                break;
        }
    }

    bool Node::checkEgress() {
        bool ret;
        Action act = input_buffer_->getQueueStatus();
        if (input_buffer_->empty()) {
            // nothing to be done
            input_buffer_->setQueueStatus(NONE);
            ret = false;
        } else if (act == INGRESS || act == INGRESS_ONLY) {
            // check if node already queued as INGRESS. This shouldn't
            // happen.
            fprintf(stderr, "Node %d trying to be compressed while\
                    waiting for decompression\n", id());
            assert(false);
        } else if (act == EGRESS) {
            // previous list queued for compression hasn't been compressed
            // yet. No need to add node again
            ret = false;
        } else {
            // Node not present
            input_buffer_->setQueueStatus(EGRESS);
            ret = true;
        }
        return ret;
    }

    bool Node::checkIngress() {
        bool ret;
        Action act = input_buffer_->getQueueStatus();
        if (input_buffer_->empty()) {
            input_buffer_->setQueueStatus(NONE);
            ret = false;
        } else if (act == EGRESS) {
            // check if compression request is outstanding and cancel this */
            input_buffer_->setQueueStatus(act);
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Node %d reset to decompress\n", id());
#endif
            ret = false;
        } else if (act == INGRESS || act == INGRESS_ONLY) {
            // we're decompressing twice
            fprintf(stderr, "ingressing node %d twice", id());
            assert(false);
        } else { // NONE
            input_buffer_->setQueueStatus(act);
            ret = true;
        }
        return ret;
    }

    void Node::wait(const Action& act) {
        switch (act) {
            case EGRESS:
            case INGRESS:
            case INGRESS_ONLY:
                {
                    pthread_mutex_lock(&xgressMutex_);
                    while (input_buffer_->getQueueStatus() == act)
                        pthread_cond_wait(&xgressCond_, &xgressMutex_);
                    pthread_mutex_unlock(&xgressMutex_);
                }
                break;
            case MERGE:
                {
                    pthread_mutex_lock(&sortMutex_);
                    while (input_buffer_->getQueueStatus() == act)
                        pthread_cond_wait(&sortCond_, &sortMutex_);
                    pthread_mutex_unlock(&sortMutex_);
                }
                break;
            case EMPTY:
                {
                    pthread_mutex_lock(&emptyMutex_);
                    while (input_buffer_->getQueueStatus() == act)
                        pthread_cond_wait(&emptyCond_, &emptyMutex_);
                    pthread_mutex_unlock(&emptyMutex_);
                }
                break;
            case NONE:
                {
                    assert(false && "Can't wait for NONE");
                }
                break;
        }
    }

    void Node::perform(BufferType type) {
        Action act = buffer(type)->getQueueStatus();
        switch (act) {
            case EGRESS:
            case INGRESS:
            case INGRESS_ONLY:
                {
                    if (act == EGRESS) {
                        buffer(type)->egress();
                    } else if (act == INGRESS || act == INGRESS_ONLY) {
                        buffer(type)->ingress();
                    }
                }
                break;
            case SORT:
                {
                    if (isRoot()) {
                        buffer(type)->sort();
                        buffer(type)->aggregate(/*isRoot=*/true);
                    } else {
                        assert(false && "Only the root buffer is sorted");
                    }
                }
                break;
            case MERGE:
                {
                    if (!isRoot()) {
                        buffer(type)->merge();
                        buffer(type)->aggregate(/*isRoot=*/false);
                    } else {
                        assert(false && "root buffer never sorted");
                    }
                }
                break;
            case EMPTY:
                {
                    bool rootFlag = isRoot();
                    emptyBuffer();
                    if (isLeaf())
                        tree_->handleFullLeaves();
                    // if it is a leaf, it might be queued for compression
                    if (!isLeaf())
                        empty_buffer_->setQueueStatus(NONE);

                    // if both buffers are full, we pick up the other node for
                    // emptying
                    if (both_buffers_full()) {
                        switchBuffers();
                        set_both_buffers_full(false);
                        spillBuffer();
                    }

                    if (rootFlag) {
                        tree_->sorter_->submitNextNodeForEmptying();
                    }
                }
                break;
            case NONE:
                {
                    assert(false && "Can't perform NONE");
                }
                break;
        }
    }

    bool Node::checkSerializationIntegrity(int listn  /* =-1*/) {
#if 0
        uint32_t offset;
        PartialAgg* pao;
        tree_->createPAO_(NULL, &pao);
        if (listn < 0) {
            for (uint32_t j = 0; j < input_buffer_->lists_.size(); ++j) {
                Buffer::List* l = input_buffer_->lists_[j];
                offset = 0;
                for (uint32_t i = 0; i < l->num_; ++i) {
                    if (!(static_cast<ProtobufPartialAgg*>(pao)->deserialize(
                            l->data_ + offset, l->sizes_[i])) {
                        fprintf(stderr,
                                "Error in list %u, index %u, offset %u\n",
                                j, i, offset);
                        assert(false);
                    }
                    offset += l->sizes_[i];
                }
            }
        } else {
            Buffer::List* l = input_buffer_->lists_[listn];
            offset = 0;
            for (uint32_t i = 0; i < l->num_; ++i) {
                if (!(static_cast<ProtobufPartialAgg*>(pao)->deserialize(
                        l->data_ + offset, l->sizes_[i])) {
                    fprintf(stderr, "Error in list %u, index %u, offset %u\n",
                            listn, i, offset);
                    assert(false);
                }
                offset += l->sizes_[i];
            }
        }
        tree_->destroyPAO_(pao);
#endif
        return true;
    }

    bool Node::checkIntegrity() {
#ifdef ENABLE_INTEGRITY_CHECK
        uint32_t offset;
        offset = 0;
        for (uint32_t j = 0; j < input_buffer_->lists_.size(); ++j) {
            Buffer::List* l = input_buffer_->lists_[j];
            for (uint32_t i = 0; i < l->num_-1; ++i) {
                if (l->hashes_[i] > l->hashes_[i+1]) {
                    fprintf(stderr, "Node: %d, List: %d: Hash %u at index %u\
                            greater than hash %u at %u (size: %u)\n", id_, j,
                            l->hashes_[i], i, l->hashes_[i+1],
                            i+1, l->num_);
                    assert(false);
                }
            }
            for (uint32_t i = 0; i < l->num_; ++i) {
                if (l->sizes_[i] == 0) {
                    fprintf(stderr, "Element %u in list %u has 0 size; tot\
                            size: %u\n", i, j, l->num_);
                    assert(false);
                }
            }
            if (l->hashes_[l->num_-1] >= separator_) {
                fprintf(stderr, "Node: %d: Hash %u at index %u\
                        greater than separator %u\n", id_,
                        l->hashes_[l->num_-1], l->num_-1, separator_);
                assert(false);
            }
        }
#endif
        return true;
    }
}

