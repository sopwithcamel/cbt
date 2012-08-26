#include <assert.h>
#include <deque>
#include <stdio.h>

#define __STDC_LIMIT_MACROS /* for UINT32_MAX etc. */
#include <stdint.h>
#include <stdlib.h>

#include "Buffer.h"
#include "CompressTree.h"
#include "HashUtil.h"
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
        numEvicted_(0)
    {
        BUFFER_SIZE = buffer_size;
        MAX_ELS_PER_BUFFER = BUFFER_SIZE / pao_size;
        EMPTY_THRESHOLD = MAX_ELS_PER_BUFFER >> 1;
    
        pthread_mutex_init(&rootNodeAvailableMutex_, NULL);
        pthread_cond_init(&rootNodeAvailableForWriting_, NULL);

        uint32_t threadCount = 4;
#ifdef ENABLE_PAGING
        threadCount++;
#endif
#ifdef ENABLE_COUNTERS
        threadCount++;
        monitor_ = NULL;
#endif
        pthread_barrier_init(&threadsBarrier_, NULL, threadCount);
    }

    CompressTree::~CompressTree()
    {
        pthread_cond_destroy(&rootNodeAvailableForWriting_);
        pthread_mutex_destroy(&rootNodeAvailableMutex_);
        pthread_barrier_destroy(&threadsBarrier_);
    }

    bool CompressTree::bulk_insert(PartialAgg** paos, uint64_t num)
    {
        PartialAgg* pao;
        bool ret = true;
        for (uint64_t i=0; i<num; i++) {
            pao = paos[i];
            uint64_t hashv = HashUtil::MurmurHash(ops->getKey(pao), 42);
            void* ptrToHash = (void*)&hashv;
            ret &= insert(ptrToHash, pao);
        }
        return ret;
    }

    bool CompressTree::insert(void* hash, PartialAgg* agg)
    {
        // copy buf into root node buffer
        // root node buffer always decompressed
        allFlush_ = false;
        if (!threadsStarted_) {
            startThreads();
        }
        if (inputNode_->isFull()) {
            // check if rootNode_ is available
            inputNode_->checkSerializationIntegrity();
            pthread_mutex_lock(&rootNodeAvailableMutex_);
            while (!rootNode_->buffer_.empty() || 
                    rootNode_->queuedForEmptying_) {
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "inserter sleeping\n");
#endif
                pthread_cond_wait(&rootNodeAvailableForWriting_,
                        &rootNodeAvailableMutex_);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "inserter fingered\n");
#endif
            }
            // switch buffers
            Buffer temp;
            temp.lists_ = rootNode_->buffer_.lists_;
            rootNode_->buffer_.lists_ = inputNode_->buffer_.lists_;
            inputNode_->buffer_.lists_ = temp.lists_;
            temp.clear();

            pthread_mutex_unlock(&rootNodeAvailableMutex_);

            // schedule the root node for emptying
            sorter_->addNode(rootNode_);
            sorter_->wakeup();
        }
        bool ret = inputNode_->insert(*(uint64_t*)hash, agg);
        return ret;
    }

    bool CompressTree::nextValue(void*& hash, PartialAgg*& agg)
    {
        if (!allFlush_) {
            /* wait for all nodes to be sorted and emptied
               before proceeding */
/*
            do {
#ifdef ENABLE_PAGING
                pager_->waitUntilCompletionNoticeReceived();
#endif
                sorter_->waitUntilCompletionNoticeReceived();
                emptier_->waitUntilCompletionNoticeReceived();
#ifdef ENABLE_PAGING
            } while (!sorter_->empty() || !emptier_->empty() || 
                    !pager_->empty());
#else
            } while (!sorter_->empty() || !emptier_->empty());
#endif
*/
            flushBuffers();

            /* Wait for all outstanding compression work to finish */
            compressor_->waitUntilCompletionNoticeReceived();
            allFlush_ = true;

            // page in and decompress first leaf
            Node* curLeaf = allLeaves_[0];
            while (curLeaf->buffer_.numElements() == 0)
                curLeaf = allLeaves_[++lastLeafRead_];
            curLeaf->scheduleBufferCompressAction(Buffer::DECOMPRESS);
            curLeaf->waitForCompressAction(Buffer::DECOMPRESS);
        }

        Node* curLeaf = allLeaves_[lastLeafRead_];
        Buffer::List* l = curLeaf->buffer_.lists_[0];
        hash = (void*)&l->hashes_[lastElement_];
        ops->createPAO(NULL, &agg);
        if (!(ops->deserialize(agg, l->data_ + lastOffset_,
                l->sizes_[lastElement_]))) {
            fprintf(stderr, "Can't deserialize at %u, index: %u\n", lastOffset_,
                    lastElement_);
            assert(false);
        }
        lastOffset_ += l->sizes_[lastElement_];
        lastElement_++;

        if (lastElement_ >= curLeaf->buffer_.numElements()) {
            curLeaf->scheduleBufferCompressAction(Buffer::COMPRESS);
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
            while (curLeaf->buffer_.numElements() == 0)
                curLeaf = allLeaves_[++lastLeafRead_];
            n->scheduleBufferCompressAction(Buffer::DECOMPRESS);
            n->waitForCompressAction(Buffer::DECOMPRESS);
            lastOffset_ = 0;
            lastElement_ = 0;
        }

        return true;
    }

    void CompressTree::emptyTree()
    {
        std::deque<Node*> delList1;
        std::deque<Node*> delList2;
        delList1.push_back(rootNode_);
        while (!delList1.empty()) {
            Node* n = delList1.front();
            delList1.pop_front();
            for (uint32_t i=0; i<n->children_.size(); i++) {
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

    bool CompressTree::flushBuffers()
    {
        Node* curNode;
        std::deque<Node*> visitQueue;
        fprintf(stderr, "Starting to flush\n");
        // check if rootNode_ is available
        pthread_mutex_lock(&rootNodeAvailableMutex_);
        while (!rootNode_->buffer_.empty() || 
                rootNode_->queuedForEmptying_) {
            pthread_cond_wait(&rootNodeAvailableForWriting_,
                    &rootNodeAvailableMutex_);
        }
        pthread_mutex_unlock(&rootNodeAvailableMutex_);
        // root node is now empty
        emptyType_ = ALWAYS;

        // switch buffers
        Buffer temp;
        temp.lists_ = rootNode_->buffer_.lists_;
        rootNode_->buffer_.lists_ = inputNode_->buffer_.lists_;
        inputNode_->buffer_.lists_ = temp.lists_;
        temp.clear();

        sorter_->addNode(rootNode_);
        sorter_->wakeup();

        /* wait for all nodes to be sorted and emptied
           before proceeding */
        do {
#ifdef ENABLE_PAGING
            pager_->waitUntilCompletionNoticeReceived();
#endif
            sorter_->waitUntilCompletionNoticeReceived();
            emptier_->waitUntilCompletionNoticeReceived();
#ifdef ENABLE_PAGING
        } while (!sorter_->empty() || !emptier_->empty() || !pager_->empty());
#else
        } while (!sorter_->empty() || !emptier_->empty());
#endif

        // add all leaves; 
        visitQueue.push_back(rootNode_);
        while(!visitQueue.empty()) {
            curNode = visitQueue.front();
            visitQueue.pop_front();
            if (curNode->isLeaf()) {
                allLeaves_.push_back(curNode);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Pushing node %d to all-leaves\t", curNode->id_);
                fprintf(stderr, "Now has: ");
                for (int i=0; i<allLeaves_.size(); i++) {
                    fprintf(stderr, "%d ", allLeaves_[i]->id_);
                }
                fprintf(stderr, "\n");
#endif
                continue;
            }
            for (uint32_t i=0; i<curNode->children_.size(); i++) {
                visitQueue.push_back(curNode->children_[i]);
            }
        }
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Tree has %ld leaves\n", allLeaves_.size());
        uint32_t numit = 0;
        for (int i=0; i<allLeaves_.size(); i++)
            numit += allLeaves_[i]->buffer_.numElements();
        fprintf(stderr, "Tree has %ld elements\n", numit);
#endif
        return true;
    }

    bool CompressTree::addLeafToEmpty(Node* node)
    {
        leavesToBeEmptied_.push_back(node);
        return true;
    }

    /* A full leaf is handled by splitting the leaf into two leaves.*/
    void CompressTree::handleFullLeaves()
    {
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
            node->scheduleBufferCompressAction(Buffer::COMPRESS);
            if (newLeaf) {
                newLeaf->scheduleBufferCompressAction(Buffer::COMPRESS);
            }
            if (l1) {
                l1->scheduleBufferCompressAction(Buffer::COMPRESS);
            }
            if (l2) {
                l2->scheduleBufferCompressAction(Buffer::COMPRESS);
            }
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "Leaf node %d removed from full-leaf-list\n", node->id_);
#endif
            pthread_mutex_lock(&node->queuedForEmptyMutex_);
            node->queuedForEmptying_ = false;
            pthread_mutex_unlock(&node->queuedForEmptyMutex_);
        }
    }

    void CompressTree::startThreads()
    {
        // create root node; initially a leaf
        rootNode_ = new Node(this, 0);
        rootNode_->buffer_.addList();
        rootNode_->separator_ = UINT32_MAX;
        rootNode_->buffer_.setCompressible(false);

        inputNode_ = new Node(this, 0);
        inputNode_->buffer_.addList();
        inputNode_->separator_ = UINT32_MAX;
        inputNode_->buffer_.setCompressible(false);
#ifdef ENABLE_PAGING
        rootNode_->buffer_.setPageable(false);
        inputNode_->buffer_.setPageable(false);
#endif
        
        emptyType_ = IF_FULL;

        pthread_attr_t attr;

        sorter_ = new Sorter(this);
        pthread_attr_init(&attr);
        pthread_create(&sorter_->thread_, &attr,
                &Sorter::callHelper, (void*)sorter_);

        compressor_ = new Compressor(this);
        pthread_attr_init(&attr);
        pthread_create(&compressor_->thread_, &attr, 
                &Compressor::callHelper, (void*)compressor_);

        emptier_ = new Emptier(this);
        pthread_attr_init(&attr);
        pthread_create(&emptier_->thread_, &attr,
                &Emptier::callHelper, (void*)emptier_);

#ifdef ENABLE_PAGING
        pager_ = new Pager(this);
        pthread_attr_init(&attr);
        pthread_create(&pager_->thread_, &attr,
                &Pager::callHelper, (void*)pager_);
#endif

#ifdef ENABLE_COUNTERS
        monitor_ = new Monitor(this);
        pthread_attr_init(&attr);
        pthread_create(&monitor_->thread_, &attr,
                &Monitor::callHelper, (void*)monitor_);
#endif

        pthread_barrier_wait(&threadsBarrier_);
        threadsStarted_ = true;
    }

    void CompressTree::stopThreads()
    {
        void* status;
        delete inputNode_;

        sorter_->setInputComplete(true);
        sorter_->wakeup();
        pthread_join(sorter_->thread_, &status);
        emptier_->setInputComplete(true);
        emptier_->wakeup();
        pthread_join(emptier_->thread_, &status);
        compressor_->setInputComplete(true);
        compressor_->wakeup();
        pthread_join(compressor_->thread_, &status);
#ifdef ENABLE_PAGING
        pager_->setInputComplete(true);
        pager_->wakeup();
        pthread_join(pager_->thread_, &status);
#endif
#ifdef ENABLE_COUNTERS
        monitor_->setInputComplete(true);
        pthread_join(monitor_->thread_, &status);
#endif
        threadsStarted_ = false;
    }

    bool CompressTree::createNewRoot(Node* otherChild)
    {
        Node* newRoot = new Node(this, rootNode_->level() + 1);
        newRoot->buffer_.addList();
        newRoot->separator_ = UINT32_MAX;
        newRoot->buffer_.setCompressible(false);
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
