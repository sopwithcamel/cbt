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
#include <pthread.h>
#include <stdint.h>
#include <deque>
#include "Slaves.h"

namespace cbt {
    Slave::Slave(CompressTree* tree) :
            tree_(tree),
            askForCompletionNotice_(false),
            tmask_(0), // everyone awake
            inputComplete_(false),
            nodesEmpty_(true) {
        pthread_spin_init(&nodesLock_, PTHREAD_PROCESS_PRIVATE);

        pthread_mutex_init(&completionMutex_, NULL);
        pthread_cond_init(&complete_, NULL);

        pthread_spin_init(&maskLock_, PTHREAD_PROCESS_PRIVATE);
    }

    inline bool Slave::empty() {
        pthread_spin_lock(&nodesLock_);
        bool ret = nodes_.empty();
        pthread_spin_unlock(&nodesLock_);
        return ret;
    }

    inline bool Slave::inputComplete() {
        pthread_spin_lock(&nodesLock_);
        bool ret = inputComplete_;
        pthread_spin_unlock(&nodesLock_);
        return ret;
    }

    Node* Slave::getNextNode(bool fromHead) {
        Node* ret;
        pthread_spin_lock(&nodesLock_);
        if (nodes_.empty()) {
            ret = false;
        } else if (fromHead) {
            ret = nodes_.front();
            nodes_.pop_front();
        } else {
            ret = nodes_.back();
            nodes_.pop_back();
        }
        pthread_spin_unlock(&nodesLock_);
        return ret;
    }

    bool Slave::addNodeToQueue(Node* node, bool toTail) {
        pthread_spin_lock(&nodesLock_);
        if (toTail) {
            nodes_.push_back(node);
        } else {
            nodes_.push_front(node);
        }
        pthread_spin_unlock(&nodesLock_);
        return true;
    }

    /* Naive for now */
    void Slave::wakeup() {
        // find first set bit
        pthread_spin_lock(&maskLock_);
        uint32_t temp = tmask_, next = 0;
        for ( ; temp; temp >>= 1) {
            next++;
            if (temp & 1)
                break;
        }
        if (next > 0) {  // sleeping thread found, so set as awake
            tmask_ &= ~(1 << (next - 1));
        }
        pthread_spin_unlock(&maskLock_);

        // wake up thread
        if (next > 0) {
            pthread_mutex_lock(&(threads_[next - 1]->mutex_));
            pthread_cond_signal(&(threads_[next - 1]->hasWork_));
            pthread_mutex_unlock(&(threads_[next - 1]->mutex_));
        }
    }

    inline void Slave::setThreadSleep(uint32_t ind) {
        pthread_spin_lock(&maskLock_);
        tmask_ |= (1 << ind);
        pthread_spin_unlock(&maskLock_);
    }

    // TODO: Using a naive method for now
    inline uint32_t Slave::getNumberOfSleepingThreads() {
        uint32_t c;
        pthread_spin_lock(&maskLock_);
        uint64_t v = tmask_;
        for (c = 0; v; v >>= 1)
            c += (v & 1);
        pthread_spin_unlock(&maskLock_);
        return c; 
    }

    void Slave::checkSendCompletionNotice() {
        pthread_mutex_lock(&completionMutex_);
        if (askForCompletionNotice_) {
            // can signal only if I'm the last thread awake so I check if the
            // number of sleeping threads is numThreads_ - 1
            if (getNumberOfSleepingThreads() == (numThreads_ - 1)) {
                pthread_cond_signal(&complete_);
                askForCompletionNotice_ = false;
            }
        }
        pthread_mutex_unlock(&completionMutex_);
    }

    inline void Slave::setInputComplete(bool value) {
        pthread_spin_lock(&nodesLock_);
        inputComplete_ = value;
        pthread_spin_unlock(&nodesLock_);
    }

    bool Slave::checkInputComplete() {
        pthread_spin_lock(&nodesLock_);
        bool ret = inputComplete_;
        pthread_spin_unlock(&nodesLock_);
        return ret;
    }

    void Slave::waitUntilCompletionNoticeReceived() {
        if (!empty()) {
            pthread_mutex_lock(&completionMutex_);
            askForCompletionNotice_ = true;
            pthread_cond_wait(&complete_, &completionMutex_);
            pthread_mutex_unlock(&completionMutex_);
        }
    }

    void* Slave::callHelper(void* arg) {
        Pthread_args* a = reinterpret_cast<Pthread_args*>(arg);
        Slave* slave = static_cast<Slave*>(a->context);
        slave->slaveRoutine(a->desc);
        pthread_exit(NULL);
    }

    void Slave::slaveRoutine(ThreadStruct* me) {
        // Things get messed up if some workers enter before all are created
        pthread_barrier_wait(&tree_->threadsBarrier_);

        while (empty()) {
            // check if anybody wants a notification when list is empty
            checkSendCompletionNotice();

#ifdef CT_NODE_DEBUG
            fprintf(stderr, "%s (%d) sleeping\n", getSlaveName().c_str(),
                    me->index_);
#endif  // CT_NODE_DEBUG

            // mark thread as sleeping in mask
            setThreadSleep(me->index_);

            // sleep until woken up
            pthread_mutex_lock(&(me->mutex_));
            pthread_cond_wait(&(me->hasWork_), &(me->mutex_));
            pthread_mutex_unlock(&(me->mutex_));

#ifdef CT_NODE_DEBUG
            fprintf(stderr, "%s (%d) fingered\n", getSlaveName().c_str(),
                    me->index_);
#endif  // CT_NODE_DEBUG

            // Actually do Slave work
            while (true) {
                Node* n = getNextNode();
                if (!n)
                    break;
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "%s (%d): working on node: %d (size: %u)\t",
                        getSlaveName().c_str(), me->index_, n->id_,
                        n->buffer_.numElements());
                fprintf(stderr, "remaining: ");
                printElements();
#endif
                work(n);
            }
            if (checkInputComplete())
                break;
        }
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "%s (%d) quitting: %ld\n",
                getSlaveName().c_str(), me->index_, nodes_.size());
#endif  // CT_NODE_DEBUG
    }

#ifdef CT_NODE_DEBUG
    void Slave::printElements() {
        if (empty()) {
            fprintf(stderr, "NULL\n");
            return;
        }
        pthread_spin_lock(&nodesLock_);
        for (uint32_t i = 0; i < nodes_.size(); ++i) {
            if (nodes_[i]->isRoot())
                fprintf(stderr, "%d*, ", nodes_[i]->id());
            else
                fprintf(stderr, "%d, ", nodes_[i]->id());
        }
        fprintf(stderr, "\n");
        pthread_spin_unlock(&nodesLock_);
    }
#endif  // CT_NODE_DEBUG

    void Slave::startThreads(uint32_t num) {
        pthread_attr_t attr;
        numThreads_ = num;
        pthread_attr_init(&attr);
        for (uint32_t i = 0; i < numThreads_; ++i) {
            ThreadStruct* t = new ThreadStruct();
            t->index_ = i;

            Pthread_args* arg = new Pthread_args(); 
            arg->context = reinterpret_cast<void*>(this);
            arg->desc = t;

            pthread_create(&(t->thread_), &attr, callHelper,
                    reinterpret_cast<void*>(arg));
            threads_.push_back(t);
        }
    }

    void Slave::stopThreads() {
        void* status;
        setInputComplete(true);
        for (uint32_t i = 0; i < numThreads_; ++i) {
            wakeup();
            pthread_join(threads_[i]->thread_, &status);
        }
        // TODO clean up thread state
    }


    // Emptier

    Emptier::Emptier(CompressTree* tree) :
            Slave(tree) {
    }

    Emptier::~Emptier() {
    }

    bool Emptier::empty() {
        pthread_spin_lock(&nodesLock_);
        bool ret = queue_.empty();
        pthread_spin_unlock(&nodesLock_);
        return ret;
    }

    Node* Emptier::getNextNode(bool fromHead) {
        Node* ret;
        pthread_spin_lock(&nodesLock_);
        if (queue_.empty()) {
            ret = false;
        } else {
            ret = queue_.pop();
        }
        pthread_spin_unlock(&nodesLock_);
        return ret;
    }

    void Emptier::work(Node* n) {
        bool rootFlag = false;

        pthread_mutex_lock(&n->queuedForEmptyMutex_);
        n->queuedForEmptying_ = false;
        pthread_mutex_unlock(&n->queuedForEmptyMutex_);

        if (n->isRoot())
            rootFlag = true;

        if (n->isRoot()) {
            n->aggregateSortedBuffer();
        } else {
            n->aggregateMergedBuffer();
        }

        n->emptyBuffer();
        if (n->isLeaf())
            tree_->handleFullLeaves();
        if (rootFlag) {
            // do the split and create new root
            rootFlag = false;

            pthread_mutex_lock(&tree_->rootNodeAvailableMutex_);
            pthread_cond_signal(&tree_->rootNodeAvailableForWriting_);
            pthread_mutex_unlock(&tree_->rootNodeAvailableMutex_);
        }
    }

    void Emptier::addNode(Node* node) {
        if (node) {
            // set queue status
            setQueueStatus(EMPTY);

            pthread_spin_lock(&nodesLock_);
            queue_.insert(node, node->level());

            std::deque<Node*> depNodes;
            uint32_t prio = node->level();
            depNodes.push_back(node);
            while (!depNodes.empty()) {
                Node* t = depNodes.front();
                for (uint32_t i = 0; i < t->children_.size(); i++) {
                    if (t->children_[i]->queuedForEmptying_) {
                        queue_.insert(t->children_[i], ++prio);
                        depNodes.push_back(t->children_[i]);
                    }
                }
                depNodes.pop_front();
            }
        pthread_spin_unlock(&nodesLock_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d (size: %u) added to to-empty list: ",
                node->id_, node->buffer_.numElements());
        printElements();
#endif
        }
    }

    std::string Emptier::getSlaveName() const {
        return "Emptier";
    }

    void Emptier::printElements() {
        if (empty()) {
            fprintf(stderr, "NULL\n");
            return;
        }
        pthread_spin_lock(&nodesLock_);
        queue_.printElements();
        pthread_spin_unlock(&nodesLock_);
    }

    // Compressor

    Compressor::Compressor(CompressTree* tree) :
            Slave(tree) {
    }

    Compressor::~Compressor() {
    }

    void Compressor::work(Node* n) {
        n->perform();
    }

    void Compressor::addNode(Node* node) {
        Buffer::CompressionAction act = node->getCompressAction();
        if (act == Buffer::COMPRESS) {
            addNodeToQueue(node);
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "adding node %d to compress: ", node->id_);
            printElements();
#endif  // CT_NODE_DEBUG
#ifdef ENABLE_PAGING
            /* Put in a request for paging out. This is necessary to do
             * right away because of the following case: if a page-in request
             * arrives when the node is on the compression queue waiting to
             * be compressed, the page-in request could simply get discarded
             * since there is no page-out request (yet). This leads to a case
             * where a decompression later assumes that the page-in has
             * completed */
            node->scheduleBufferPageAction(Buffer::PAGE_OUT);
#endif  // ENABLE_PAGING
        } else {
#ifdef ENABLE_PAGING
            node->scheduleBufferPageAction(Buffer::PAGE_IN);
#endif  // ENABLE_PAGING

            addNodeToQueue(node, /* toTail = */false);

#ifdef CT_NODE_DEBUG
            fprintf(stderr, "adding node %d (size: %u) to decompress: ",
                    node->id_, node->buffer_.numElements());
            printElements();
#endif  // CT_NODE_DEBUG
        }
    }

    std::string Compressor::getSlaveName() const {
        return "Compressor";
    }

    // Sorter

    Sorter::Sorter(CompressTree* tree) :
            Slave(tree) {
    }

    Sorter::~Sorter() {
    }

    void Sorter::work(Node* n) {
        n->wait(DECOMPRESS);
        if (n->isRoot()) {
            n->sortBuffer();
        } else {
            n->mergeBuffer();
        }
        tree_->emptier_->addNode(n);
        tree_->emptier_->wakeup();
    }

    void Sorter::addNode(Node* node) {
        if (node) {
            // Set node as queued for emptying
            setQueueStatus(SORT);
            addNodeToQueue(node);

#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Node %d added to to-sort list (size: %u)\n",
                    node->id_, node->buffer_.numElements());
            printElements();
#endif  // CT_NODE_DEBUG
        }
    }

    std::string Sorter::getSlaveName() const {
        return "Sorter";
    }

#ifdef ENABLE_PAGING

    // Pager

    Pager::Pager(CompressTree* tree) :
            Slave(tree) {
    }

    Pager::~Pager() {
    }


    void Pager::work(Node* n) {
        bool suc = n->perform();
        if (!suc)
            addNodeToQueue(n);
    }

    void Pager::addNode(Node* node) {
        Buffer::PageAction act = node->getPageAction();
        if (act == Buffer::PAGE_OUT) {
            addNodeToQueue(node);
        } else {
            addNodeToQueue(node, /* toTail = */false);
        }

#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d added to page list: ", node->id_);
        printElements();
#endif  // CT_NODE_DEBUG
    }

    std::string Pager::getSlaveName() const {
        return "Pager";
    }
#endif  // ENABLE_PAGING

#ifdef ENABLE_COUNTERS
    Monitor::Monitor(CompressTree* tree) :
            Slave(tree),
            numElements(0),
            numMerged(0),
            actr(0),
            bctr(0),
            cctr(0) {
    }

    Monitor::~Monitor() {
    }

    void Monitor::work(Node* n) {
        pthread_barrier_wait(&tree_->threadsBarrier_);
        while (!inputComplete()) {
            sleep(1);
            elctr.push_back(numElements);
        }
        uint64_t tot = 0;
        for (uint32_t i = 0; i < elctr.size(); ++i) {
            tot += elctr[i];
        }
        fprintf(stderr, "Avg. number of elements: %f\n",
                static_cast<float>(tot) / elctr.size());
        fprintf(stderr, "A: %lu, B:%lu\n", numElements, numMerged);
        elctr.clear();
    }

    void Monitor::addNode(Node* n) {
        return;
    }

    std::string Monitor::getSlaveName() const {
        return "Monitor";
    }
#endif
}
