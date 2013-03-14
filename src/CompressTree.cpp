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
#include <stdio.h>
#define __STDC_LIMIT_MACROS /* for UINT32_MAX etc. */
#include <stdint.h>
#include <stdlib.h>
#include <deque>

#include "Buffer.h"
#include "CompressTree.h"
#include "Slaves.h"

namespace cbt {

    uint32_t BUFFER_SIZE;
    uint32_t MAX_ELS_PER_BUFFER;
    uint32_t EMPTY_THRESHOLD;

    CompressTree::CompressTree(uint32_t a, uint32_t b, uint32_t nodesInMemory,
                uint32_t buffer_size, uint32_t pao_size,
                const Operations* const ops) :
            a_(a),
            b_(b),
            nodeCtr(1),
            ops(ops),
            alg_(SNAPPY),
            allFlush_(true),
            empty_(true),
            lastLeafRead_(0),
            lastOffset_(0),
            lastElement_(0),
            threadsStarted_(false) {
        BUFFER_SIZE = buffer_size;
        MAX_ELS_PER_BUFFER = BUFFER_SIZE / 16; //pao_size;
        EMPTY_THRESHOLD = BUFFER_SIZE >> 1;

        pthread_cond_init(&emptyRootAvailable_, NULL);

        pthread_mutex_init(&emptyRootNodesMutex_, NULL);

        tree_prefix_ = rand();
#ifdef ENABLE_COUNTERS
        monitor_ = NULL;
#endif
    }

    CompressTree::~CompressTree() {
        pthread_cond_destroy(&emptyRootAvailable_);
        pthread_mutex_destroy(&emptyRootNodesMutex_);
        pthread_barrier_destroy(&threadsBarrier_);
    }

    bool CompressTree::bulk_insert(PartialAgg** paos, uint64_t num) {
        bool ret = true;
        // copy buf into root node buffer
        // root node buffer always decompressed
        if (num > 0)
            allFlush_ = empty_ = false;
        if (!threadsStarted_) {
            startThreads();
        }

        for (uint64_t i = 0; i < num; ++i) {
            PartialAgg* agg = paos[i];
            if (inputNode_->isFull()) {
                // add inputNode_ to be sorted
                inputNode_->schedule(SORT);

                // get an empty root. This function can block until there are
                // empty roots available
                inputNode_ = getEmptyRootNode();
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Now inputting into node %d\n",
                        inputNode_->id());
#endif  // CT_NODE_DEBUG
            }
            ret &= inputNode_->insert(agg);
        }
        return ret;
    }

    bool CompressTree::insert(PartialAgg* agg) {
        bool ret = bulk_insert(&agg, 1);
        return ret;
    }

    bool CompressTree::bulk_read(PartialAgg** pao_list, uint64_t& num_read,
            uint64_t max) {
        uint64_t hash;
        void* ptrToHash = reinterpret_cast<void*>(&hash);
        num_read = 0;
        while (num_read < max) {
            if (!(nextValue(ptrToHash, pao_list[num_read])))
                return false;
            num_read++;
        }
        return true;
    }

    bool CompressTree::nextValue(void*& hash, PartialAgg*& agg) {
        if (empty_)
            return false;
        if (!allFlush_) {
            flushBuffers();
            lastLeafRead_ = 0;
            lastOffset_ = 0;
            lastElement_ = 0;

            allFlush_ = true;

            // page in and decompress first leaf
            Node* curLeaf = allLeaves_[0];
            assert(curLeaf->buffer_.lists_.size() == 1);
            uint32_t numLeaves = allLeaves_.size();

            while (curLeaf->buffer_.numElements() == 0 &&
                    lastLeafRead_ < numLeaves)
                curLeaf = allLeaves_[++lastLeafRead_];
            curLeaf->buffer_.page_in();

            // also schedule the next leaf for decompression
            uint32_t nextLeafIndex = lastLeafRead_ + 1;
            if (nextLeafIndex < numLeaves) {
                Node* nextLeaf = allLeaves_[nextLeafIndex];
                while (nextLeaf->buffer_.numElements() == 0 &&
                        nextLeafIndex < numLeaves)
                    nextLeaf = allLeaves_[++nextLeafIndex];
                if (nextLeafIndex < numLeaves)
                    nextLeaf->buffer_.page_in();
            }
        }

        Node* curLeaf = allLeaves_[lastLeafRead_];
        Buffer::List* l = curLeaf->buffer_.lists_[0];
        hash = reinterpret_cast<void*>(&l->hashes_[lastElement_]);
        ops->createPAO(NULL, &agg);
//        if (lastLeafRead_ == 0)
//            fprintf(stderr, "%ld\n", lastOffset_);
        if (!(ops->deserialize(agg, l->data_ + lastOffset_,
                l->sizes_[lastElement_]))) {
            fprintf(stderr, "Can't deserialize at %u, index: %u\n", lastOffset_,
                    lastElement_);
            assert(false);
        }
        lastOffset_ += l->sizes_[lastElement_];
        lastElement_++;

        if (lastElement_ >= curLeaf->buffer_.numElements()) {
            curLeaf->buffer_.page_out();
            if (++lastLeafRead_ == allLeaves_.size()) {
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Emptying tree!\n");
#endif
                // Again wait for all to end
                int all_done;
                do {
                    usleep(10000);
                    sem_getvalue(&sleepSemaphore_, &all_done);
                } while (all_done);
                emptyTree();
                stopThreads();
                return false;
            }
            Node *n = allLeaves_[lastLeafRead_];
            // has already been decompressed
            // also decompress next leaf
            uint32_t nextLeafIndex = lastLeafRead_ + 1;
            uint32_t numLeaves = allLeaves_.size();
            if (nextLeafIndex < numLeaves) {
                Node* nextLeaf = allLeaves_[nextLeafIndex];
                while (nextLeaf->buffer_.numElements() == 0 &&
                        nextLeafIndex < numLeaves)
                    nextLeaf = allLeaves_[++nextLeafIndex];
                if (nextLeafIndex < numLeaves)
                    nextLeaf->buffer_.page_in();
            }
            lastOffset_ = 0;
            lastElement_ = 0;
        }

        return true;
    }

    void CompressTree::clear() {
        emptyTree();
        stopThreads();
    }

    void CompressTree::emptyTree() {
        if (empty_)
            return;

        std::deque<Node*> delList1;
        std::deque<Node*> delList2;
        delList1.push_back(rootNode_);
        while (!delList1.empty()) {
            Node* n = delList1.front();
            delList1.pop_front();
            for (uint32_t i = 0; i < n->children_.size(); ++i) {
                delList1.push_back(n->children_[i]);
            }
            delList2.push_back(n);
        }
        while (!delList2.empty()) {
            Node* n = delList2.front();
            delList2.pop_front();
            delete n;
        }
        allLeaves_.clear();
        leavesToBeEmptied_.clear();
        allFlush_ = empty_ = true;
        lastLeafRead_ = 0;
        lastOffset_ = 0;
        lastElement_ = 0;

        nodeCtr = 0;
    }

    bool CompressTree::flushBuffers() {
        Node* curNode;
        std::deque<Node*> visitQueue;
        fprintf(stderr, "Starting to flush\n");

        emptyType_ = FLUSH;
        inputNode_->schedule(SORT);

        int all_done;
        do {
            usleep(10000);
            sem_getvalue(&sleepSemaphore_, &all_done);
        } while (all_done);

        // add all leaves;
        visitQueue.push_back(rootNode_);
        while (!visitQueue.empty()) {
            curNode = visitQueue.front();
            visitQueue.pop_front();
            if (curNode->isLeaf()) {
                allLeaves_.push_back(curNode);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Pushing node %d to all-leaves\t",
                        curNode->id_);
                fprintf(stderr, "Now has: ");
                for (uint32_t i = 0; i < allLeaves_.size(); ++i) {
                    fprintf(stderr, "%d ", allLeaves_[i]->id_);
                }
                fprintf(stderr, "\n");
#endif
                continue;
            }
            for (uint32_t i = 0; i < curNode->children_.size(); ++i) {
                visitQueue.push_back(curNode->children_[i]);
            }
        }
        fprintf(stderr, "Tree has %ld leaves\n", allLeaves_.size());
        uint32_t depth = 1;
        curNode = rootNode_;
        while (curNode->children_.size() > 0) {
            depth++;
            curNode = curNode->children_[0];
        }
        fprintf(stderr, "Tree has depth: %d\n", depth);
        uint64_t numit = 0;
        for (uint64_t i = 0; i < allLeaves_.size(); ++i)
            numit += allLeaves_[i]->buffer_.numElements();
        fprintf(stderr, "Tree has %ld elements\n", numit);
        return true;
    }

    bool CompressTree::addLeafToEmpty(Node* node) {
        leavesToBeEmptied_.push_back(node);
        return true;
    }

    Node* CompressTree::getEmptyRootNode() {
        pthread_mutex_lock(&emptyRootNodesMutex_);
        while (emptyRootNodes_.empty()) {
#ifdef CT_NODE_DEBUG
            if (!rootNode_->buffer_.empty())
                fprintf(stderr, "inserter sleeping (buffer not empty)\n");
            else
                fprintf(stderr, "inserter sleeping (queued somewhere %d)\n",
                        emptyRootNodes_.size());
#endif

            pthread_cond_wait(&emptyRootAvailable_, &emptyRootNodesMutex_);

#ifdef CT_NODE_DEBUG
            fprintf(stderr, "inserter fingered\n");
#endif
        }
        Node* e = emptyRootNodes_.front();
        emptyRootNodes_.pop_front();
        pthread_mutex_unlock(&emptyRootNodesMutex_);
        return e;
    }

    void CompressTree::addEmptyRootNode(Node* n) {
        bool no_empty_nodes = false;
        pthread_mutex_lock(&emptyRootNodesMutex_);
        // check if there are no empty nodes right now
        if (emptyRootNodes_.empty())
            no_empty_nodes = true;
        emptyRootNodes_.push_back(n);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Added empty root (now has: %d)\n",
                emptyRootNodes_.size());
#endif
        // if this is the first empty node, then signal
//        if (no_empty_nodes)
        pthread_cond_signal(&emptyRootAvailable_);
        pthread_mutex_unlock(&emptyRootNodesMutex_);
    }

    bool CompressTree::rootNodeAvailable() {
        if (!rootNode_->buffer_.empty() ||
                !rootNode_->state_mask_.is_set(DEFAULT))
            return false;
        return true;
    }

    void CompressTree::submitNodeForEmptying(Node* n) {
        // perform the switch, schedule root, add node to empty list
        Buffer temp;
        temp.lists_ = rootNode_->buffer_.lists_;
        rootNode_->buffer_.lists_ = n->buffer_.lists_;
        rootNode_->schedule(EMPTY);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Submitting node %d for emptying\n",
                rootNode_->id());
#endif  // CT_NODE_DEBUG

        n->buffer_.lists_ = temp.lists_;
        temp.clear();
        addEmptyRootNode(n);
    }

    void CompressTree::startThreads() {
        // create root node; initially a leaf
        rootNode_ = new Node(this, 0);
        rootNode_->buffer_.addList(new Buffer::List());
        rootNode_->separator_ = UINT32_MAX;
        rootNode_->buffer_.set_pageable(false);

        inputNode_ = new Node(this, 0);
        inputNode_->buffer_.addList(new Buffer::List());
        inputNode_->separator_ = UINT32_MAX;
        inputNode_->buffer_.set_pageable(false);

        uint32_t number_of_root_nodes = 4;
        for (uint32_t i = 0; i < number_of_root_nodes - 1; ++i) {
            Node* n = new Node(this, 0);
            n->buffer_.addList(new Buffer::List());
            n->separator_ = UINT32_MAX;
            n->buffer_.set_pageable(false);
            emptyRootNodes_.push_back(n);
        }

        emptyType_ = IF_FULL;

        uint32_t emptierThreadCount = 4;
        uint32_t sorterThreadCount = 4;

        // One for the inserter
        uint32_t threadCount = emptierThreadCount + sorterThreadCount + 1;
        pthread_barrier_init(&threadsBarrier_, NULL, threadCount);
        sem_init(&sleepSemaphore_, 0, threadCount - 1);

        sorter_ = new Sorter(this);
        sorter_->startThreads(sorterThreadCount);

        emptier_ = new Emptier(this);
        emptier_->startThreads(emptierThreadCount);

        pthread_barrier_wait(&threadsBarrier_);
        threadsStarted_ = true;
    }

    void CompressTree::stopThreads() {
        if (!threadsStarted_)
            return;

        delete inputNode_;

        sorter_->stopThreads();
        emptier_->stopThreads();
#ifdef ENABLE_COUNTERS
        monitor_->stopThreads();
#endif
        threadsStarted_ = false;
    }

    bool CompressTree::createNewRoot(Node* otherChild) {
        Node* newRoot = new Node(this, rootNode_->level() + 1);
        newRoot->buffer_.addList(new Buffer::List());
        newRoot->separator_ = UINT32_MAX;
        newRoot->buffer_.set_pageable(false);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d is new root; children are %d and %d\n",
                newRoot->id_, rootNode_->id_, otherChild->id_);
#endif
        // add two children of new root
        newRoot->addChild(rootNode_);
        newRoot->addChild(otherChild);
        rootNode_ = newRoot;
        return true;
    }
}
