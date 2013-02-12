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
        bool ret = nodes_.empty() && allAsleep();
        pthread_spin_unlock(&nodesLock_);
        return ret;
    }

    inline bool Slave::more() {
        pthread_spin_lock(&nodesLock_);
        bool ret = nodes_.empty();
        pthread_spin_unlock(&nodesLock_);
        return !ret;
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
            ret = NULL;
        } else {
            ret = nodes_.front();
            nodes_.pop_front();
        }
        pthread_spin_unlock(&nodesLock_);
        return ret;
    }

    bool Slave::addNodeToQueue(Node* n, uint32_t priority) {
        pthread_spin_lock(&nodesLock_);
        nodes_.push_back(n);
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
        if (next > 0) {  // sleeping thread found
#ifdef CT_NODE_DEBUG
            int ret;
            sem_getvalue(&tree_->sleepSemaphore_, &ret);
            fprintf(stderr, "%s (%d) fingered [sem: %d]\n",
                    getSlaveName().c_str(), next - 1, ret);
#endif  // CT_NODE_DEBUG
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
#ifdef CT_NODE_DEBUG
        int ret;
        sem_getvalue(&tree_->sleepSemaphore_, &ret);
        fprintf(stderr, "%s (%d) sleeping [sem: %d]\n",
                getSlaveName().c_str(), ind, ret);
#endif  // CT_NODE_DEBUG

        tmask_ |= (1 << ind);
        pthread_spin_unlock(&maskLock_);
    }

    void Slave::setThreadAwake(uint32_t ind) {
        pthread_spin_lock(&maskLock_);
        // set thread as awake
        tmask_ &= ~(1 << ind );
        pthread_spin_unlock(&maskLock_);
    }

    uint32_t Slave::getNumberOfSleepingThreads() {
        // check if we are the last thread to go to sleep (naive for now)
        uint32_t c;
        uint64_t v = tmask_;
        for (c = 0; v; v >>= 1)
            c += (v & 1);
        return c;
    }

    bool Slave::allAsleep() {
        pthread_spin_lock(&maskLock_);
        bool ret = (getNumberOfSleepingThreads() == numThreads_);
        pthread_spin_unlock(&maskLock_);
        return ret;
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

    void* Slave::callHelper(void* arg) {
        Pthread_args* a = reinterpret_cast<Pthread_args*>(arg);
        Slave* slave = static_cast<Slave*>(a->context);
        slave->slaveRoutine(a->desc);
        pthread_exit(NULL);
    }

    void Slave::slaveRoutine(ThreadStruct* me) {
        // Things get messed up if some workers enter before all are created
        pthread_barrier_wait(&tree_->threadsBarrier_);

        while (true) {
            // should never block
            assert(sem_trywait(&tree_->sleepSemaphore_) == 0);
            // mark thread as sleeping in mask
            setThreadSleep(me->index_);

            // sleep until woken up
            pthread_mutex_lock(&(me->mutex_));
            pthread_cond_wait(&(me->hasWork_), &(me->mutex_));
            pthread_mutex_unlock(&(me->mutex_));

            sem_post(&tree_->sleepSemaphore_);

            setThreadAwake(me->index_);
            if (checkInputComplete())
                break;

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
        }
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "%s (%d) quitting: %ld\n",
                getSlaveName().c_str(), me->index_, nodes_.size());
#endif  // CT_NODE_DEBUG
    }

#ifdef CT_NODE_DEBUG
    void Slave::printElements() {
        if (!more()) {
            fprintf(stderr, "NULL\n");
            return;
        }
        pthread_spin_lock(&nodesLock_);
        std::deque<Node*> dq = nodes_;
        pthread_spin_unlock(&nodesLock_);
        while (!dq.empty()) {
            Node* n = dq.front();
            dq.pop_front();
            if (n->isRoot())
                fprintf(stderr, "%d*, ", n->id());
            else
                fprintf(stderr, "%d, ", n->id());
        }
        fprintf(stderr, "\n");
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

    // Sorter

    Sorter::Sorter(CompressTree* tree) :
            Slave(tree) {
        pthread_mutex_init(&sortedNodesMutex_, NULL);
    }

    Sorter::~Sorter() {
        pthread_mutex_destroy(&sortedNodesMutex_);
    }

    void Sorter::work(Node* n) {
        n->perform(SORT);
        addToSorted(n); 
    }

    void Sorter::addNode(Node* node) {
        addNodeToQueue(node, node->level());
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d (sz: %u) added to to-sort list: ",
                node->id_, node->buffer_.numElements());
        printElements();
#endif
    }

    std::string Sorter::getSlaveName() const {
        return "Sorter";
    }

    void Sorter::addToSorted(Node* n) {
        // pick up the lock so no sorted node can be picked up
        // for emptying
        pthread_mutex_lock(&sortedNodesMutex_);
        if (sortedNodes_.empty()) {
            // the root node might be out being emptied
            if (!tree_->rootNodeAvailable()) {
                sortedNodes_.push_back(n);
            } else {
                tree_->submitNodeForEmptying(n);
            }
        } else {
            // there are other full root nodes waiting to be emptied. So just
            // add this node on to the queue
            sortedNodes_.push_back(n);
        }
        pthread_mutex_unlock(&sortedNodesMutex_);
    }

    void Sorter::submitNextNodeForEmptying() {
        pthread_mutex_lock(&sortedNodesMutex_);
        if (!sortedNodes_.empty()) {
            Node* n = sortedNodes_.front();
            sortedNodes_.pop_front();
            tree_->submitNodeForEmptying(n);
        }
        pthread_mutex_unlock(&sortedNodesMutex_);
    }

    // Emptier

    Emptier::Emptier(CompressTree* tree) :
            Slave(tree) {
    }

    Emptier::~Emptier() {
    }

    bool Emptier::empty() {
        pthread_spin_lock(&nodesLock_);
        bool ret = queue_.empty() && allAsleep();
        pthread_spin_unlock(&nodesLock_);
        return ret;
    }

    bool Emptier::more() {
        pthread_spin_lock(&nodesLock_);
        bool ret = queue_.empty();
        pthread_spin_unlock(&nodesLock_);
        return !ret;
    }

    Node* Emptier::getNextNode(bool fromHead) {
        Node* ret;
        pthread_spin_lock(&nodesLock_);
        ret = queue_.pop();
        pthread_spin_unlock(&nodesLock_);
        return ret;
    }

    void Emptier::work(Node* n) {
        n->wait(MERGE);
        bool is_root = n->isRoot();

        n->perform(EMPTY);

        // No other node is dependent on the root. Performing this check also
        // avoids the problem, where perform() causes the creation of a new
        // root which is immediately submitted for emptying. In a regular case,
        // a parent of n would be in the disabled queue, but in this case it is
        // not.
        if (!is_root) {
            // possibly enable parent etc.
            pthread_spin_lock(&nodesLock_);
            queue_.post(n);
            pthread_spin_unlock(&nodesLock_);
        }
        
        // handle notifications
        n->done(EMPTY);
    }

    void Emptier::addNode(Node* node) {
        pthread_spin_lock(&nodesLock_);
        bool ret = queue_.insert(node);
        pthread_spin_unlock(&nodesLock_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d (sz: %u) (enab: %s) added to to-empty list: ",
                node->id_, node->buffer_.numElements(), ret? "True" : "False");
        printElements();
#endif
    }

    std::string Emptier::getSlaveName() const {
        return "Emptier";
    }

    void Emptier::printElements() {
        if (!more()) {
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
        NodeState state;
        if (n->schedule_mask_.is_set(COMPRESS))
            state = COMPRESS;
        else
            state = DECOMPRESS;
        n->perform(state);
        n->done(state);
    }

    void Compressor::addNode(Node* node) {
        NodeState state;
        if (node->schedule_mask_.is_set(COMPRESS))
            state = COMPRESS;
        else
            state = DECOMPRESS;
        if (state == COMPRESS) {
            addNodeToQueue(node, /*priority=*/0);
        } else {
            addNodeToQueue(node, /*priority=*/node->level());
        }

#ifdef CT_NODE_DEBUG
        fprintf(stderr, "adding node %d (size: %u) to %s: ",
                node->id_, node->buffer_.numElements(),
                state == COMPRESS? "compress" : "decompress");
        printElements();
#endif  // CT_NODE_DEBUG
    }

    std::string Compressor::getSlaveName() const {
        return "Compressor";
    }

    // Merger

    Merger::Merger(CompressTree* tree) :
            Slave(tree) {
    }

    Merger::~Merger() {
    }

    void Merger::work(Node* n) {
        // block until buffer is decompressed
        n->wait(DECOMPRESS);
        // perform merge
        n->perform(MERGE);
        // schedule for emptying
        n->schedule(EMPTY);
        // indicate that we're done sorting
        n->done(MERGE);
    }

    void Merger::addNode(Node* node) {
        if (node) {
            // Set node as queued for emptying
            addNodeToQueue(node, node->level());

#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Node %d (size: %u) added to to-merge list: ",
                    node->id_, node->buffer_.numElements());
            printElements();
#endif  // CT_NODE_DEBUG
        }
    }

    std::string Merger::getSlaveName() const {
        return "Merger";
    }
}
