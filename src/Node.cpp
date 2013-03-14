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
            level_(level),
            parent_(NULL) {
        id_ = tree_->nodeCtr++;
        buffer_.setParent(this);

        pthread_mutex_init(&bufferMutex_, NULL);
        pthread_mutex_init(&childrenMutex_, NULL);

        pthread_mutex_init(&emptyMutex_, NULL);
        pthread_cond_init(&emptyCond_, NULL);

        pthread_mutex_init(&sortMutex_, NULL);
        pthread_cond_init(&sortCond_, NULL);

        pthread_mutex_init(&compMutex_, NULL);
        pthread_cond_init(&compCond_, NULL);

        buffer_.setupPaging();
    }

    Node::~Node() {
        pthread_mutex_destroy(&bufferMutex_);
        pthread_mutex_destroy(&childrenMutex_);

        pthread_mutex_destroy(&sortMutex_);
        pthread_cond_destroy(&sortCond_);

        pthread_mutex_destroy(&compMutex_);
        pthread_cond_destroy(&compCond_);

        buffer_.cleanupPaging();
    }

    bool Node::insert(PartialAgg* agg) {
        uint32_t buf_size = tree_->ops->getSerializedSize(agg);
        const char* key = tree_->ops->getKey(agg);

        // copy into Buffer fields
        Buffer::List* l = buffer_.lists_[0];
        l->hashes_[l->num_] = HashUtil::MurmurHash(key, strlen(key), 42);
//        l->hashes_[l->num_] = HashUtil::DigramHash(key, strlen(key));
        if (l->hashes_[l->num_] == 0xffffffff)
            l->hashes_[l->num_]--;

        l->sizes_[l->num_] = buf_size;
        // is this required?
//        memset(l->data_ + l->size_, 0, buf_size);
        tree_->ops->serialize(agg, l->data_ + l->size_,
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

    bool Node::emptyOrCompress() {
        bool ret = true;
        // if in flush mode, then schedule the node for emptying regardless
        if (tree_->emptyType_ == FLUSH) {
            schedule(EMPTY);
            return ret;
        }
        uint32_t siz = buffer_.size();        

        if (!isFull()) {
            buffer_.page_out();
        } else {
            schedule(EMPTY);
        }
        return ret;
    }

    bool Node::emptyBuffer() {
        uint32_t curChild = 0;
        uint32_t curElement = 0;
        uint32_t lastElement = 0;

        // if it is a leaf node, queue up for action later after all the internal
        // nodes have been processed
        if (isLeaf()) {
            // it can't be assumed that the leaf is full as this may be called
            // even when when flushing all buffers at the end. In the latter
            // case, we compress the leaf without splitting
            if (isFull() || isRoot()) {
                handleFullLeaf();
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Leaf node %d added to full-leaf-list\
                        %u/%u\n", id_, buffer_.size(), EMPTY_THRESHOLD);
#endif
            } else {  // compress
                buffer_.page_out();
            }
            return true;
        }

        // work on a copy of the children vector because spilling may cause one
        // or more children (if they are leaves) to split and modify the real
        // children vector
        std::vector<Node*> children_copy = children_;
        if (buffer_.empty()) {
            for (curChild = 0; curChild < children_copy.size(); curChild++) {
                children_copy[curChild]->emptyOrCompress();
            }
        } else {
            checkSerializationIntegrity();
            Buffer::List* l = buffer_.lists_[0];
            // find the first separator strictly greater than the first element
            while (l->hashes_[curElement] >=
                    children_copy[curChild]->separator_) {
                children_copy[curChild]->emptyOrCompress();
                curChild++;
#ifdef ENABLE_ASSERT_CHECKS
                if (curChild >= children_copy.size()) {
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
                    children_copy[curChild]->separator_, curChild, l->hashes_[0]);
#endif
            uint32_t num = buffer_.numElements();
#ifdef ENABLE_ASSERT_CHECKS
            // there has to be a single list in the buffer at this point
            assert(buffer_.lists_.size() == 1);
#endif
            while (curElement < num) {
                if (l->hashes_[curElement] >=
                        children_copy[curChild]->separator_) {
                    /* this separator is the largest separator that is not greater
                     * than *curHash. This invariant needs to be maintained.
                     */
                    if (curElement > lastElement) {
                        // copy elements into child
                        children_copy[curChild]->copyIntoBuffer(l, lastElement,
                                curElement - lastElement);
#ifdef CT_NODE_DEBUG
                        fprintf(stderr, "Copied %u elements into node %d\
                                 list:%lu\n",
                                curElement - lastElement,
                                children_copy[curChild]->id_,
                                children_copy[curChild]->buffer_.lists_.size()-1);
#endif
                        lastElement = curElement;
                    }
                    // skip past all separators not greater than current hash
                    while (l->hashes_[curElement]
                            >= children_copy[curChild]->separator_) {
                        children_copy[curChild]->emptyOrCompress();
                        curChild++;
#ifdef ENABLE_ASSERT_CHECKS
                        if (curChild >= children_copy.size()) {
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
                children_copy[curChild]->copyIntoBuffer(l, lastElement,
                        curElement - lastElement);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Copied %u elements into node %d; \
                        list: %lu\n",
                        curElement - lastElement,
                        children_copy[curChild]->id_,
                        children_copy[curChild]->buffer_.lists_.size()-1);
#endif
                children_copy[curChild]->emptyOrCompress();
                curChild++;
            }
            // empty or compress any remaining children
            while (curChild < children_copy.size()) {
                children_copy[curChild]->emptyOrCompress();
                curChild++;
            }

            // reset
            l->setEmpty();

            if (!isRoot()) {
                buffer_.deallocate();
            }
        }
        children_copy.clear();

        // Split leaves can cause the number of children to increase. Check.
        if (children_.size() > tree_->b_) {
            splitNonLeaf();
        }
        return true;
    }

    void Node::handleFullLeaf() {
        Node* newLeaf = splitLeaf();

        buffer_.page_out();
        if (newLeaf) {
            newLeaf->buffer_.page_out();
        }
    }

    /* A leaf is split by moving half the elements of the buffer into a
     * new leaf and inserting a median value as the separator element into the
     * parent */
    Node* Node::splitLeaf() {
        checkIntegrity();
#ifdef ENABLE_ASSERT_CHECKS
        assert(buffer_.lists_.size() == 1);
#endif  // ENABLE_ASSERT_CHECKS

        // select splitting index
        uint32_t num = buffer_.numElements();
        uint32_t splitIndex = num/2;
        Buffer::List* l = buffer_.lists_[0];
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
        delete l;
        buffer_.delList(0);

        // check integrity of both leaves
        newLeaf->checkIntegrity();
        checkIntegrity();
#ifdef CT_NODE_DEBUG
        // the newly created list...
        l = buffer_.lists_[0];
        fprintf(stderr, "Node %d splits to Node %d: new indices: %u and\
                %u; new separators: %u and %u\n", id_, newLeaf->id_,
                l->num_, newLeaf->buffer_.lists_[0]->num_, separator_,
                newLeaf->separator_);
#endif

        // if leaf is also the root, create new root
        if (isRoot()) {
            buffer_.set_pageable(true);
            tree_->createNewRoot(newLeaf);
        } else {
            parent_->addChild(newLeaf);
        }
        return newLeaf;
    }

    bool Node::copyIntoBuffer(Buffer::List* parent_list, uint32_t index,
            uint32_t num) {

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
        assert(parent_list->state_ == Buffer::List::DECOMPRESSED);
        if (num_bytes >= BUFFER_SIZE) {
            fprintf(stderr, "Node: %d, buf: %d\n", id_,
                    num_bytes);
            assert(false);
        }
#endif
        // allocate a new List in the buffer and copy data into it
        Buffer::List* l = new Buffer::List();

        // memset(l->hashes_, 0, num * sizeof(uint32_t));
        memcpy(l->hashes_, parent_list->hashes_ + index,
                num * sizeof(uint32_t));
        // memset(l->sizes_, 0, num * sizeof(uint32_t));
        memcpy(l->sizes_, parent_list->sizes_ + index,
                num * sizeof(uint32_t));
        // memset(l->data_, 0, num_bytes);
        memcpy(l->data_, parent_list->data_ + offset,
                num_bytes);
        l->num_ = num;
        l->size_ = num_bytes;

        buffer_.addList(l);

        checkSerializationIntegrity(buffer_.lists_.size()-1);
        buffer_.checkSortIntegrity(l);
        return true;
    }

    bool Node::addChild(Node* newNode) {
        uint32_t i;
        // insert separator value

        // find position of insertion
        pthread_mutex_lock(&childrenMutex_);
        std::vector<Node*>::iterator it = children_.begin();
        for (i = 0; i < children_.size(); ++i) {
            if (newNode->separator_ > children_[i]->separator_)
                continue;
            break;
        }
        it += i;
        children_.insert(it, newNode);
        pthread_mutex_unlock(&childrenMutex_);
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

    // TODO: Do we hold the children mutex here? When this function is called,
    // no child is supposed to be emptying anyway
    bool Node::splitNonLeaf() {
        // ensure node's buffer is empty
#ifdef ENABLE_ASSERT_CHECKS
        if (!buffer_.empty()) {
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
            buffer_.set_pageable(true);
            buffer_.deallocate();
            return tree_->createNewRoot(newNode);
        } else {
            return parent_->addChild(newNode);
        }
    }

    bool Node::isFull() {
        if (buffer_.size() > EMPTY_THRESHOLD)
            return true;
        return false;
    }

    uint32_t Node::level() const {
        return level_;
    }

    uint32_t Node::id() const {
        return id_;
    }

    void Node::done(const NodeState& state) {
        switch(state) {
            case SORT:
                {
                    // Signal that we're done sorting
                    pthread_mutex_lock(&sortMutex_);
                    pthread_cond_signal(&sortCond_);
                    pthread_mutex_unlock(&sortMutex_);
                }
                break;
            case EMPTY:
            default:
                break;
        }
    }

    void Node::schedule(const NodeState& state) {
        switch(state) {
            case SORT:
                {
                    // add node to sorter
                    if (!schedule_mask_.is_set(SORT)) {
                        schedule_mask_.set(SORT);
                        tree_->sorter_->addNode(this);
                        tree_->sorter_->wakeup();
                    }
                }
                break;
            case EMPTY:
                {
                    // add node to empty
                    if (!schedule_mask_.is_set(EMPTY)) {
                        schedule_mask_.set(EMPTY);
                        tree_->emptier_->addNode(this);
                        tree_->emptier_->wakeup();
                    }
                }
                break;
            default:
                assert(false && "Illegal state");
        }
    }

    void Node::wait(const NodeState& state) {
        switch (state) {
            case SORT:
                {
                    pthread_mutex_lock(&sortMutex_);
                    while (schedule_mask_.is_set(state))
                        pthread_cond_wait(&sortCond_, &sortMutex_);
                    pthread_mutex_unlock(&sortMutex_);
                }
                break;
            case EMPTY:
                {
                    pthread_mutex_lock(&emptyMutex_);
                    while (schedule_mask_.is_set(state))
                        pthread_cond_wait(&emptyCond_, &emptyMutex_);
                    pthread_mutex_unlock(&emptyMutex_);
                }
                break;
            default:
                assert(false && "Illegal state");
        }
    }

    void Node::perform(const NodeState& state) {
        bool rootFlag = isRoot();
        switch (state) {
            case SORT:
                {
                    pthread_mutex_lock(&bufferMutex_);
                    assert(rootFlag && "Only the root buffer is sorted");
                    buffer_.sort();
                    checkIntegrity();

                    buffer_.aggregate(/*isSort = */true);
                    checkIntegrity();
                    pthread_mutex_unlock(&bufferMutex_);
                }
                break;
            case EMPTY:
                {
                    if (!rootFlag) {
                        buffer_.page_in();
                        buffer_.merge();
                        buffer_.aggregate(/*isSort = */false);
                    }
                    emptyBuffer();
                }
                break;
            default:
                assert(false && "Illegal state");
        }

        // clear the current mask
        state_mask_.clear();

        // set next state
        if (state == EMPTY)
            state_mask_.set(DEFAULT);
        else
            state_mask_.set(state);

        // unset from schedule mask and set in state mask
        schedule_mask_.unset(state);

        if (state == EMPTY && rootFlag) {
            tree_->sorter_->submitNextNodeForEmptying();
        }
    }

    bool Node::canEmptyIntoNode() {
        bool ret = true;
        uint32_t schedule_test_mask = 1;
        uint32_t state_test_mask = 1;
        ret &= (schedule_mask_.or_mask(schedule_test_mask) ==
                schedule_test_mask);        
        ret &= (state_mask_.or_mask(state_test_mask) ==
                state_test_mask);        
        return ret;
    }

    bool Node::checkSerializationIntegrity(int listn  /* =-1*/) {
#if 0
        uint32_t offset;
        PartialAgg* pao;
        tree_->createPAO_(NULL, &pao);
        if (listn < 0) {
            for (uint32_t j = 0; j < buffer_.lists_.size(); ++j) {
                Buffer::List* l = buffer_.lists_[j];
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
            Buffer::List* l = buffer_.lists_[listn];
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
#if 0
        uint32_t offset;
        offset = 0;
        for (uint32_t j = 0; j < buffer_.lists_.size(); ++j) {
            Buffer::List* l = buffer_.lists_[j];
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

