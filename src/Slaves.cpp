#include <assert.h>
#include <deque>
#include <pthread.h>
#include <stdint.h>
#include "Slaves.h"

namespace cbt {
    Slave::Slave(CompressTree* tree) :
            tree_(tree),
            inputComplete_(false),
            queueEmpty_(true),
            askForCompletionNotice_(false)
    {
        pthread_mutex_init(&queueMutex_, NULL);
        pthread_cond_init(&queueHasWork_, NULL);

        pthread_mutex_init(&completionMutex_, NULL);
        pthread_cond_init(&complete_, NULL);

    }

    bool Slave::empty() const
    {
        return queueEmpty_;
    }

    bool Slave::wakeup()
    {
        pthread_mutex_lock(&queueMutex_);
        pthread_cond_signal(&queueHasWork_);
        pthread_mutex_unlock(&queueMutex_);
        return true;
    }

    void Slave::sendCompletionNotice()
    {
        pthread_mutex_lock(&completionMutex_);
        if (askForCompletionNotice_) {
            pthread_cond_signal(&complete_);
            askForCompletionNotice_ = false;
        }
        pthread_mutex_unlock(&completionMutex_);
    }
    
    void Slave::waitUntilCompletionNoticeReceived()
    {
        pthread_mutex_lock(&queueMutex_);
        if (!empty()) {
            pthread_mutex_lock(&completionMutex_);
            askForCompletionNotice_ = true;
            pthread_mutex_unlock(&queueMutex_);

            pthread_cond_wait(&complete_, &completionMutex_);
            pthread_mutex_unlock(&completionMutex_);
        } else {
            pthread_mutex_unlock(&queueMutex_);
        }
    }

    void* Slave::callHelper(void* context)
    {
        return ((Slave*)context)->work();
    }

    void Slave::printElements() const
    {
        for (uint32_t i=0; i<nodes_.size(); i++) {
            fprintf(stderr, "%d, ", nodes_[i]->id());
        }
        fprintf(stderr, "\n");
    }

    void Slave::startThreads()
    {
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_create(&thread_, &attr, callHelper, (void*)this);
    }

    void Slave::stopThreads()
    {
        void* status;
        setInputComplete(true);
        wakeup();
        pthread_join(thread_, &status);
    }

    Emptier::Emptier(CompressTree* tree) :
            Slave(tree)
    {
    }

    Emptier::~Emptier()
    {
    }

    void* Emptier::work()
    {
        bool rootFlag = false;
        pthread_mutex_lock(&queueMutex_);
        pthread_barrier_wait(&tree_->threadsBarrier_);
        while (queue_.empty() && !inputComplete_) {
            /* check if anybody wants a notification when list is empty */
            sendCompletionNotice();
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "emptier sleeping\n");
#endif
            queueEmpty_ = true;
            pthread_cond_wait(&queueHasWork_, &queueMutex_);
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "emptier fingered\n");
#endif
            while (!queue_.empty()) {
                Node* n = queue_.pop();
                pthread_mutex_unlock(&queueMutex_);
                pthread_mutex_lock(&n->queuedForEmptyMutex_);
                n->queuedForEmptying_ = false;
                pthread_mutex_unlock(&n->queuedForEmptyMutex_);
                if (n->isRoot())
                    rootFlag = true;
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "emptier: emptying node: %d (size: %u)\t", n->id_, n->buffer_.numElements());
                fprintf(stderr, "remaining: ");
                queue_.printElements();
#endif
                if (n->isRoot())
                    n->aggregateSortedBuffer();
                else {
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
                pthread_mutex_lock(&queueMutex_);
            }
        }
        pthread_mutex_unlock(&queueMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Emptier quitting: %ld\n", queue_.size());
#endif
        pthread_exit(NULL);
    }

    void Emptier::addNode(Node* node)
    {
        pthread_mutex_lock(&queueMutex_);
        if (node) {
            queue_.insert(node, node->level());
            queueEmpty_ = false;

            std::deque<Node*> depNodes;
            uint32_t prio = node->level();
            depNodes.push_back(node);
            while (!depNodes.empty()) {
                Node* t = depNodes.front();
                for (uint32_t i=0; i<t->children_.size(); i++) {
                    if (t->children_[i]->queuedForEmptying_) {
                        queue_.insert(t->children_[i], ++prio); 
                        depNodes.push_back(t->children_[i]);
                    }
                }
                depNodes.pop_front();
            }
        }
        pthread_mutex_unlock(&queueMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d (size: %u) added to to-empty list: ", node->id_, node->buffer_.numElements());
        queue_.printElements();
#endif
    }

    Compressor::Compressor(CompressTree* tree) :
            Slave(tree)
    {
    }

    Compressor::~Compressor()
    {
    }

    void* Compressor::work()
    {
        pthread_mutex_lock(&queueMutex_);
        pthread_barrier_wait(&tree_->threadsBarrier_);
        while (nodes_.empty() && !inputComplete_) {
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "compressor sleeping\n");
#endif
            queueEmpty_ = true;
            pthread_cond_wait(&queueHasWork_, &queueMutex_);
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "compressor fingered\n");
#endif
            while (!nodes_.empty()) {
                Node* n = nodes_.front();
                nodes_.pop_front();
                pthread_mutex_unlock(&queueMutex_);
                n->performCompressAction();
                pthread_mutex_lock(&queueMutex_);
            }
            /* check if anybody wants a notification when list is empty */
            sendCompletionNotice();
        }
        pthread_mutex_unlock(&queueMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Compressor quitting: %ld\n", nodes_.size());
#endif
        pthread_exit(NULL);
    }

    void Compressor::addNode(Node* node)
    {
        Buffer::CompressionAction act = node->getCompressAction();
        if (act == Buffer::COMPRESS) {
            pthread_mutex_lock(&queueMutex_);
            nodes_.push_back(node);
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "adding node %d to compress: ", node->id_);
            printElements();
#endif
#ifdef ENABLE_PAGING
            /* Put in a request for paging out. This is necessary to do
             * right away because of the following case: if a page-in request
             * arrives when the node is on the compression queue waiting to
             * be compressed, the page-in request could simply get discarded
             * since there is no page-out request (yet). This leads to a case
             * where a decompression later assumes that the page-in has
             * completed */
            node->scheduleBufferPageAction(Buffer::PAGE_OUT);
#endif
        } else {
#ifdef ENABLE_PAGING
            node->scheduleBufferPageAction(Buffer::PAGE_IN);
#endif
            pthread_mutex_lock(&queueMutex_);
            nodes_.push_front(node);
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "adding node %d (size: %u) to decompress: ", node->id_, node->buffer_.numElements());
            printElements();
#endif
        }
        queueEmpty_ = false;
        pthread_mutex_unlock(&queueMutex_);
    }

    Sorter::Sorter(CompressTree* tree) :
            Slave(tree)
    {
    }

    Sorter::~Sorter()
    {
    }

    void* Sorter::work()
    {
        pthread_mutex_lock(&queueMutex_);
        pthread_barrier_wait(&tree_->threadsBarrier_);
        while (nodes_.empty() && !inputComplete_) {
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "sorter sleeping\n");
#endif
            queueEmpty_ = true;
            pthread_cond_wait(&queueHasWork_, &queueMutex_);
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "sorter fingered\n");
#endif
            while (!nodes_.empty()) {
                Node* n = nodes_.front();
                nodes_.pop_front();
                pthread_mutex_unlock(&queueMutex_);
                n->waitForCompressAction(Buffer::DECOMPRESS);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "sorter: sorting node: %d (size: %u)\t", n->id_, n->buffer_.numElements());
                fprintf(stderr, "remaining: ");
                for (int i=0; i<nodes_.size(); i++)
                    fprintf(stderr, "%d, ", nodes_[i]->id_);
                fprintf(stderr, "\n");
#endif
                if (n->isRoot())
                    n->sortBuffer();
                else {
                    n->mergeBuffer();
                }
                tree_->emptier_->addNode(n);
                tree_->emptier_->wakeup();
                pthread_mutex_lock(&queueMutex_);
            }
            sendCompletionNotice();
        }
        pthread_mutex_unlock(&queueMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Sorter quitting: %ld\n", nodes_.size());
#endif
        pthread_exit(NULL);
    }

    void Sorter::addNode(Node* node)
    {
        pthread_mutex_lock(&queueMutex_);
        if (node) {
            // Set node as queued for emptying
            pthread_mutex_lock(&node->queuedForEmptyMutex_);
            node->queuedForEmptying_ = true;
            pthread_mutex_unlock(&node->queuedForEmptyMutex_);
            nodes_.push_back(node);
            queueEmpty_ = false;
        }
        pthread_mutex_unlock(&queueMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d added to to-sort list (size: %u)\n",
                node->id_, node->buffer_.numElements());
        for (int i=0; i<nodes_.size(); i++)
            fprintf(stderr, "%d, ", nodes_[i]->id_);
        fprintf(stderr, "\n");
#endif
    }

#ifdef ENABLE_PAGING
    Pager::Pager(CompressTree* tree) :
            Slave(tree)
    {
    }

    Pager::~Pager()
    {
    }


    void* Pager::work()
    {
        pthread_mutex_lock(&queueMutex_);
        pthread_barrier_wait(&tree_->threadsBarrier_);
        while (nodes_.empty() && !inputComplete_) {
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "pager sleeping\n");
#endif
            queueEmpty_ = true;
            pthread_cond_wait(&queueHasWork_, &queueMutex_);
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "pager fingered\n");
#endif
            while (!nodes_.empty()) {
                Node* n = nodes_.front();
                nodes_.pop_front();
                pthread_mutex_unlock(&queueMutex_);
                bool suc = n->performPageAction();
                pthread_mutex_lock(&queueMutex_);
                if (!suc)
                    nodes_.push_back(n);
            }
            /* check if anybody wants a notification when list is empty */
            sendCompletionNotice();
        }
        pthread_mutex_unlock(&queueMutex_);
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Pager quitting: %ld\n", nodes_.size());
#endif
        pthread_exit(NULL);
    }

    void Pager::addNode(Node* node)
    {
        Buffer::PageAction act = node->getPageAction();
        if (act == Buffer::PAGE_OUT) {
            pthread_mutex_lock(&queueMutex_);
            nodes_.push_back(node);
        } else {
            pthread_mutex_lock(&queueMutex_);
            nodes_.push_front(node);
        }
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d added to page list: ", node->id_);
        printElements();
#endif
        queueEmpty_ = false;
        pthread_mutex_unlock(&queueMutex_);
    }
#endif //ENABLE_PAGING

#ifdef ENABLE_COUNTERS
    Monitor::Monitor(CompressTree* tree) :
            Slave(tree),
            numElements(0),
            numMerged(0),
            actr(0),
            bctr(0),
            cctr(0)
    {
    }

    Monitor::~Monitor()
    {
    }

    void* Monitor::work()
    {
        pthread_barrier_wait(&tree_->threadsBarrier_);
        while (!inputComplete_) {
            sleep(1);
            elctr.push_back(numElements);
        }
        uint64_t tot = 0;
        for (uint32_t i=0; i<elctr.size(); i++) {
            tot += elctr[i];
        }
        fprintf(stderr, "Avg. number of elements: %f\n", (float)tot / 
                elctr.size());
        fprintf(stderr, "A: %lu, B:%lu\n", numElements, numMerged);
        elctr.clear();
    }

    void Monitor::addNode(Node* n)
    {
        return;
    }
#endif
}
