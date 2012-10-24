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
            lastLeafRead_(0),
            lastOffset_(0),
            lastElement_(0),
            threadsStarted_(false),
            nodesInMemory_(nodesInMemory),
            numEvicted_(0) {
        BUFFER_SIZE = buffer_size;
        MAX_ELS_PER_BUFFER = BUFFER_SIZE / pao_size;
        EMPTY_THRESHOLD = MAX_ELS_PER_BUFFER >> 1;

        pthread_cond_init(&emptyRootAvailable_, NULL);

        pthread_mutex_init(&emptyRootNodesMutex_, NULL);

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
            allFlush_ = false;
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
        if (!allFlush_) {
            flushBuffers();
            lastLeafRead_ = 0;
            lastOffset_ = 0;
            lastElement_ = 0;

            /* Wait for all outstanding compression work to finish */
            compressor_->waitUntilCompletionNoticeReceived();
            allFlush_ = true;

            // page in and decompress first leaf
            Node* curLeaf = allLeaves_[0];
            while (curLeaf->input_buffer_->numElements() == 0)
                curLeaf = allLeaves_[++lastLeafRead_];
            curLeaf->schedule(INGRESS_ONLY);
            curLeaf->wait(INGRESS_ONLY);
        }

        Node* curLeaf = allLeaves_[lastLeafRead_];
        Buffer::List* l = curLeaf->input_buffer_->lists_[0];
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

        if (lastElement_ >= curLeaf->input_buffer_->numElements()) {
            curLeaf->schedule(EGRESS);
            if (++lastLeafRead_ == allLeaves_.size()) {
                /* Wait for all outstanding compression work to finish */
                compressor_->waitUntilCompletionNoticeReceived();
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Emptying tree!\n");
#endif
                emptyTree();
                stopThreads();
                return false;
            }
            Node *n = allLeaves_[lastLeafRead_];
            while (curLeaf->input_buffer_->numElements() == 0)
                curLeaf = allLeaves_[++lastLeafRead_];
            n->schedule(INGRESS_ONLY);
            n->wait(INGRESS_ONLY);
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
        allFlush_ = true;
        lastLeafRead_ = 0;
        lastOffset_ = 0;
        lastElement_ = 0;

        nodeCtr = 0;
    }

    bool CompressTree::flushBuffers() {
        Node* curNode;
        std::deque<Node*> visitQueue;
        fprintf(stderr, "Starting to flush\n");

        emptyType_ = ALWAYS;
        inputNode_->schedule(SORT);

        /* wait for all nodes to be sorted and emptied
           before proceeding */
        do {
            merger_->waitUntilCompletionNoticeReceived();
            emptier_->waitUntilCompletionNoticeReceived();
            compressor_->waitUntilCompletionNoticeReceived();
        } while (!merger_->empty() ||
                !emptier_->empty() ||
                !compressor_->empty());

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
            numit += allLeaves_[i]->input_buffer_->numElements();
        fprintf(stderr, "Tree has %ld elements\n", numit);
        return true;
    }

    bool CompressTree::addLeafToEmpty(Node* node) {
        leavesToBeEmptied_.push_back(node);
        return true;
    }

    /* A full leaf is handled by splitting the leaf into two leaves.*/
    void CompressTree::handleFullLeaves() {
        while (!leavesToBeEmptied_.empty()) {
            Node* node = leavesToBeEmptied_.front();
            leavesToBeEmptied_.pop_front();

            Node* newLeaf = node->splitLeaf();

            Node *l1 = NULL, *l2 = NULL;
            if (node->isFull()) {
                l1 = node->splitLeaf();
            }
            if (newLeaf && newLeaf->isFull()) {
                l2 = newLeaf->splitLeaf();
            }
            node->schedule(EGRESS);
            if (newLeaf) {
                newLeaf->schedule(EGRESS);
            }
            if (l1) {
                l1->schedule(EGRESS);
            }
            if (l2) {
                l2->schedule(EGRESS);
            }
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Leaf node %d removed from full-leaf-list\n",
                    node->id_);
#endif
            // % WHY?
            //node->setQueueStatus(NONE);
        }
    }

    Node* CompressTree::getEmptyRootNode() {
        pthread_mutex_lock(&emptyRootNodesMutex_);
        while (emptyRootNodes_.empty()) {
#ifdef CT_NODE_DEBUG
            if (!rootNode_->input_buffer_->empty())
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
        if (!rootNode_->input_buffer_->empty() ||
                rootNode_->input_buffer_->getQueueStatus() != NONE)
            return false;
        return true;
    }

    void CompressTree::submitNodeForEmptying(Node* n) {
        // perform the switch, schedule root, add node to empty list
        Buffer temp;
        temp.lists_ = rootNode_->input_buffer_->lists_;
        rootNode_->input_buffer_->lists_ = n->input_buffer_->lists_;
        rootNode_->schedule(EMPTY);

        n->input_buffer_->lists_ = temp.lists_;
        temp.clear();
        addEmptyRootNode(n);
    }

    void CompressTree::startThreads() {
        // create root node; initially a leaf
        rootNode_ = new Node(this, 0);
        rootNode_->input_buffer_->addList();
        rootNode_->separator_ = UINT32_MAX;
        rootNode_->input_buffer_->setEgressible(false);

        inputNode_ = new Node(this, 0);
        inputNode_->input_buffer_->addList();
        inputNode_->separator_ = UINT32_MAX;
        inputNode_->input_buffer_->setEgressible(false);

        uint32_t number_of_root_nodes = 4;
        for (uint32_t i = 0; i < number_of_root_nodes - 1; ++i) {
            Node* n = new Node(this, 0);
            n->input_buffer_->addList();
            n->separator_ = UINT32_MAX;
            n->input_buffer_->setEgressible(false);
            emptyRootNodes_.push_back(n);
        }

        emptyType_ = IF_FULL;

        uint32_t mergerThreadCount = 4;
        uint32_t compressorThreadCount = 3;
        uint32_t emptierThreadCount = 4;
        uint32_t sorterThreadCount = 1;

        // One for the inserter
        uint32_t threadCount = mergerThreadCount + compressorThreadCount +
                emptierThreadCount + sorterThreadCount + 1;
#ifdef ENABLE_COUNTERS
        uint32_t monitorThreadCount = 1;
        threadCount += monitorThreadCount;
#endif
        pthread_barrier_init(&threadsBarrier_, NULL, threadCount);

        sorter_ = new Sorter(this);
        sorter_->startThreads(sorterThreadCount);

        merger_ = new Merger(this);
        merger_->startThreads(mergerThreadCount);

        compressor_ = new Compressor(this);
        compressor_->startThreads(compressorThreadCount);

        emptier_ = new Emptier(this);
        emptier_->startThreads(emptierThreadCount);

#ifdef ENABLE_COUNTERS
        monitor_ = new Monitor(this);
        monitor_->startThreads(monitorThreadCount);
#endif

        pthread_barrier_wait(&threadsBarrier_);
        threadsStarted_ = true;
    }

    void CompressTree::stopThreads() {
        delete inputNode_;

        merger_->stopThreads();
        sorter_->stopThreads();
        emptier_->stopThreads();
        compressor_->stopThreads();
#ifdef ENABLE_COUNTERS
        monitor_->stopThreads();
#endif
        threadsStarted_ = false;
    }

    bool CompressTree::createNewRoot(Node* otherChild) {
        Node* newRoot = new Node(this, rootNode_->level() + 1);
        newRoot->input_buffer_->addList();
        newRoot->separator_ = UINT32_MAX;
        newRoot->input_buffer_->setEgressible(false);
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
