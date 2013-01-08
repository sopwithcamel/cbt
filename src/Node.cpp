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

        // set initial state as S_EMPTY
        state_mask_.set(S_EMPTY);
    
        pthread_spin_init(&buffer_lock_, PTHREAD_PROCESS_PRIVATE);
        pthread_mutex_init(&mask_mutex_, NULL);

        for (uint32_t i = 0; i < NUMBER_OF_STATES; ++i) {
            pthread_cond_init(&state_cond_[i], NULL);
            pthread_mutex_init(&state_cond_mutex_[i], NULL);
        }
#ifdef ENABLE_PAGING
        buffer_.setupPaging();
#endif  // ENABLE_PAGING
    }

    Node::~Node() {
        pthread_spin_destroy(&buffer_lock_);
        pthread_mutex_destroy(&mask_mutex_);
        
        for (uint32_t i = 0; i < NUMBER_OF_STATES; ++i) {
            pthread_cond_destroy(&state_cond_[i]);
            pthread_mutex_destroy(&state_cond_mutex_[i]);
        }
#ifdef ENABLE_PAGING
        buffer_.cleanupPaging();
#endif  // ENABLE_PAGING
    }

    bool Node::insert(PartialAgg* agg) {
        uint32_t buf_size = tree_->ops->getSerializedSize(agg);
        const char* key = tree_->ops->getKey(agg);

        // copy into Buffer fields
        Buffer::List* l = buffer_.lists_[0];
        l->hashes_[l->num_] = HashUtil::MurmurHash(key, strlen(key), 42);
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
        if (tree_->emptyType_ == ALWAYS) {
#ifdef ENABLE_PAGING
            schedule(A_PAGEIN);
#endif  // ENABLE_PAGING
            schedule(A_DECOMPRESS);
            schedule(A_MERGE);
            return true;
        }

        uint32_t n = buffer_.numElements();        
        if (n < EMPTY_THRESHOLD * 0.75) {
            schedule(A_COMPRESS);
#ifdef ENABLE_PAGING
            schedule(A_PAGEOUT);
#endif  // ENABLE_PAGING
        } else {
#ifdef ENABLE_PAGING
            schedule(A_PAGEIN);
#endif  // ENABLE_PAGING
            schedule(A_DECOMPRESS);
            if (isFull()) {
                schedule(A_MERGE);
            }
        }
        return ret;
    }

    bool Node::emptyBuffer() {
        uint32_t curChild = 0;
        uint32_t curElement = 0;
        uint32_t lastElement = 0;

        /* if i am a leaf node, queue up for action later after all the
         * internal nodes have been processed */
        if (isLeaf()) {
            /* this may be called even when buffer is not full (when flushing
             * all buffers at the end). */
            if (isFull() || isRoot()) {
                tree_->addLeafToEmpty(this);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Leaf node %d added to full-leaf-list\
                        %u/%u\n", id_, buffer_.numElements(), EMPTY_THRESHOLD);
#endif
            } else {  // compress
                schedule(A_COMPRESS);
#ifdef ENABLE_PAGING
                schedule(A_PAGEOUT);
#endif  // ENABLE_PAGING
            }
            return true;
        }

        if (buffer_.empty()) {
            for (curChild = 0; curChild < children_.size(); curChild++) {
                children_[curChild]->emptyOrCompress();
            }
        } else {
            checkSerializationIntegrity();
            Buffer::List* l = buffer_.lists_[0];
            // find the first separator strictly greater than the first element
            while (l->hashes_[curElement] >=
                    children_[curChild]->separator_) {
                children_[curChild]->emptyOrCompress();
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
            uint32_t num = buffer_.numElements();
#ifdef ENABLE_ASSERT_CHECKS
            // there has to be a single list in the buffer at this point
            assert(buffer_.lists_.size() == 1);
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
                                children_[curChild]->buffer_.lists_.size()-1);
#endif
                        lastElement = curElement;
                    }
                    // skip past all separators not greater than current hash
                    while (l->hashes_[curElement]
                            >= children_[curChild]->separator_) {
                        children_[curChild]->emptyOrCompress();
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
                        children_[curChild]->buffer_.lists_.size()-1);
#endif
                children_[curChild]->emptyOrCompress();
                curChild++;
            }
            // empty or compress any remaining children
            while (curChild < children_.size()) {
                children_[curChild]->emptyOrCompress();
                curChild++;
            }

            // reset
            l->setEmpty();

            if (!isRoot()) {
                buffer_.deallocate();
            }
        }
        // Split leaves can cause the number of children to increase. Check.
        if (children_.size() > tree_->b_) {
            splitNonLeaf();
        }
        return true;
    }

    bool Node::sortBuffer() {
        bool ret = buffer_.sort();
        checkIntegrity();
        return ret;
    }

    bool Node::aggregateBuffer(const NodeAction& act) {
        bool ret = buffer_.aggregate(act == A_SORT? true : false);
        checkIntegrity();
        return ret;
    }

    bool Node::mergeBuffer() {
        bool ret = buffer_.merge();
        checkIntegrity();
        return ret;
    }

    /* A leaf is split by moving half the elements of the buffer into a
     * new leaf and inserting a median value as the separator element into the
     * parent */
    Node* Node::splitLeaf() {
        checkIntegrity();

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
        buffer_.delList(0);
        l = buffer_.lists_[0];

        // check integrity of both leaves
        newLeaf->checkIntegrity();
        checkIntegrity();
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "Node %d splits to Node %d: new indices: %u and\
                %u; new separators: %u and %u\n", id_, newLeaf->id_,
                l->num_, newLeaf->buffer_.lists_[0]->num_, separator_,
                newLeaf->separator_);
#endif

        // if leaf is also the root, create new root
        if (isRoot()) {
            buffer_.setCompressible(true);
#ifdef ENABLE_PAGING
            buffer_.setPageable(true);
#endif  // ENABLE_PAGING
            tree_->createNewRoot(newLeaf);
        } else {
            parent_->addChild(newLeaf);
        }
        return newLeaf;
    }

    bool Node::copyIntoBuffer(Buffer::List* parent_list, uint32_t index,
            uint32_t num) {
        // check if the node is still queued up for a previous compression
        // i wonder if we can get away without doing this.
//        wait(S_COMPRESSED);

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
        Buffer::List* l = buffer_.addList();
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

        pthread_mutex_lock(&mask_mutex_);
        if (state_mask_.is_set(S_EMPTY)) {
            state_mask_.unset(S_EMPTY);
            state_mask_.set(S_DECOMPRESSED);
        }
        pthread_mutex_unlock(&mask_mutex_);

        checkSerializationIntegrity(buffer_.lists_.size()-1);
        buffer_.checkSortIntegrity(l);
        return true;
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
            buffer_.setCompressible(true);
#ifdef ENABLE_PAGING
            buffer_.setPageable(true);
#endif  // ENABLE_PAGING
            buffer_.deallocate();
            return tree_->createNewRoot(newNode);
        } else {
            return parent_->addChild(newNode);
        }
    }

    bool Node::isFull() const {
        if (buffer_.numElements() > EMPTY_THRESHOLD)
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
        // Signal that we're done
        pthread_mutex_lock(&state_cond_mutex_[state]);
        pthread_cond_broadcast(&state_cond_[state]);
        pthread_mutex_unlock(&state_cond_mutex_[state]);
    }

    void Node::schedule(const NodeAction& action) {
        switch(action) {
            case A_COMPRESS:
                {
                    if (!buffer_.compressible_ || buffer_.empty())
                        return;
#ifdef ENABLE_ASSERT_CHECKS
                    // check if the node is queued for decompression or if
                    // decompression is in progress
                    pthread_mutex_lock(&mask_mutex_);
                    assert(!queue_mask_.is_set(A_DECOMPRESS) &&
                            "Node queued for A_DECOMPRESS");

                    assert(!in_progress_mask_.is_set(A_DECOMPRESS) &&
                            "A_DECOMPRESS in progress");
                    pthread_mutex_unlock(&mask_mutex_);
#endif  // ENABLE_ASSERT_CHECKS

                    // we need to check if compression has already been
                    // scheduled. This means that the node could still be
                    // queued, or compression could be in progress
                    pthread_mutex_lock(&mask_mutex_);
                    bool add_node = true;
                    if (in_progress_mask_.is_set(A_COMPRESS) ||
                            queue_mask_.is_set(A_COMPRESS))
                        add_node = false;
                    pthread_mutex_unlock(&mask_mutex_);

                    if (add_node) {
                        queue_mask_.set(A_COMPRESS);
                        tree_->compressor_->addNode(this);
                        tree_->compressor_->wakeup();
                    }
                } break;
            case A_DECOMPRESS:
                {
                    if (!buffer_.compressible_ || buffer_.empty())
                        return;

                    bool decomp_necessary = false, add_node = false;
                    pthread_mutex_lock(&mask_mutex_);
                    fprintf(stderr, "C: %u, %u, %u\n", state_mask_.mask_,
                            in_progress_mask_.mask_, queue_mask_.mask_);
                    if (state_mask_.is_set(S_COMPRESSED) ||
                            state_mask_.is_set(S_PAGED_OUT)) {
                        // if the current state is S_COMPRESSED or S_PAGED_OUT
                        // then by default we need to decompress. We check if
                        // we have already scheduled decompression or if it's
                        // in progress
                        decomp_necessary = true;
                        if (in_progress_mask_.is_set(A_DECOMPRESS))
                            decomp_necessary = false;
                    } else if (state_mask_.is_set(S_DECOMPRESSED) ||
                            state_mask_.is_set(S_AGGREGATED)) {
                        // if the current state is S_DECOMPRESSED or
                        // S_AGGREGATED then by default, decompression is not
                        // required. We check if compression is in progress
                        // already and schedule decompression if so
                        decomp_necessary = false;
                        if (in_progress_mask_.is_set(A_COMPRESS))
                            decomp_necessary = true;
                    } else
                        assert(false);

                    // We then check we if need to add the node to the
                    // compression queue or not.
                    // 1. If compression is queued then
                    // cancel it. This means, however, that the node is already
                    // queued in the compressor, so no need to queue again.
                    // Even if we cancel, we need to schedule decompression,
                    // because there may be some lists in the buffer which are
                    // already compressed.
                    // 2. If decompression is already queued, then no need to
                    // add
                    if (decomp_necessary) {
                        add_node = true;
                        if (queue_mask_.is_set(A_COMPRESS)) {
                            queue_mask_.unset(A_COMPRESS);
                            if (state_mask_.is_set(S_COMPRESSED) ||
                                    state_mask_.is_set(S_PAGED_OUT))
                                queue_mask_.set(A_DECOMPRESS);
                            add_node = false;    
                        } else if (queue_mask_.is_set(A_DECOMPRESS)) {
                            add_node = false;
                        } else {
                            queue_mask_.set(A_DECOMPRESS);
                        }
                    }
                    pthread_mutex_unlock(&mask_mutex_);

                    fprintf(stderr, "C: decomp_nec: %d, addnode: %d\n",
                            decomp_necessary? 1 : 0,
                            add_node? 1: 0);

                    if (add_node) {
                        tree_->compressor_->addNode(this);
                        tree_->compressor_->wakeup();
                    }
                }
                break;
#ifdef ENABLE_PAGING
            case A_PAGEOUT:
                {
                    if (!buffer_.pageable_ || buffer_.empty())
                        return;
#ifdef ENABLE_ASSERT_CHECKS
                    // check if the node is queued for paging in or if
                    // decompression is in progress. This shouldn't occur.
                    pthread_mutex_lock(&mask_mutex_);
                    assert(!queue_mask_.is_set(A_PAGEIN) &&
                            "Node queued for A_PAGEIN");

                    assert(!in_progress_mask_.is_set(A_PAGEIN) &&
                            "A_PAGEIN in progress");
                    pthread_mutex_unlock(&mask_mutex_);
#endif  // ENABLE_ASSERT_CHECKS

                    // we need to check if paging-out has already been
                    // scheduled. This means that the node could still be
                    // queued, or paging-out could be in progress
                    pthread_mutex_lock(&mask_mutex_);
                    bool add_node = true;
                    if (in_progress_mask_.is_set(A_PAGEOUT) ||
                            queue_mask_.is_set(A_PAGEOUT))
                        add_node = false;
                    if (add_node)
                        queue_mask_.set(A_PAGEOUT);
                    pthread_mutex_unlock(&mask_mutex_);

                    if (add_node) {
                        tree_->pager_->addNode(this);
                        tree_->pager_->wakeup();
                    }
                } break;
            case A_PAGEIN:
                {
                    if (!buffer_.pageable_ || buffer_.empty())
                        return;

                    bool add_node;
                    pthread_mutex_lock(&mask_mutex_);
                    fprintf(stderr, "P: %u, %u, %u\n", state_mask_.mask_,
                            in_progress_mask_.mask_, queue_mask_.mask_);
                    // if paging-out is queued then cancel it. This means,
                    // however, that the node is already queued in the
                    // pager, so no need to queue again.
                    // We still need to schedule paging-in, because there
                    // may be some lists in the buffer which are already
                    // paged-out.
                    if (queue_mask_.is_set(A_PAGEOUT)) {
                        queue_mask_.unset(A_PAGEOUT);
                        if (state_mask_.is_set(S_PAGED_OUT))
                            queue_mask_.set(A_PAGEIN);
                        add_node = false;    
                    } else if (!state_mask_.is_set(S_PAGED_OUT) &&
                                !in_progress_mask_.is_set(A_PAGEOUT)) {
                        // if the node is not paged out or being paged out,
                        // then paging in is unnecessary
                        add_node = false;
                    } else {
                        // Ok, the node is still paged out. But, we still need
                        // to check if paging-in has already been scheduled or
                        // is in progress.
                        if (in_progress_mask_.is_set(A_PAGEIN) ||
                                queue_mask_.is_set(A_PAGEIN)) {
                            add_node = false;
                        } else {
                            add_node = true;
                            queue_mask_.set(A_PAGEIN);
                        }
                    }
                    pthread_mutex_unlock(&mask_mutex_);
                    fprintf(stderr, "P: addnode: %d\n", add_node? 1: 0);

                    if (add_node) {
                        tree_->pager_->addNode(this);
                        tree_->pager_->wakeup();
                    }
                }
                break;
#endif  // ENABLE_PAGING
            case A_SORT:
                {
                    // schedule node for sorting
                    pthread_mutex_lock(&mask_mutex_);
                    queue_mask_.set(A_SORT);
                    pthread_mutex_unlock(&mask_mutex_);

                    tree_->sorter_->addNode(this);
                    tree_->sorter_->wakeup();
                }
                break;
            case A_MERGE:
                {
                    // schedule node for mergin
                    pthread_mutex_lock(&mask_mutex_);
                    queue_mask_.set(A_MERGE);
                    pthread_mutex_unlock(&mask_mutex_);

                    tree_->merger_->addNode(this);
                    tree_->merger_->wakeup();
                }
                break;
            case A_EMPTY:
                {
                    // schedule node for mergin
                    pthread_mutex_lock(&mask_mutex_);
                    queue_mask_.set(A_EMPTY);
                    pthread_mutex_unlock(&mask_mutex_);

                    tree_->emptier_->addNode(this);
                    tree_->emptier_->wakeup();
                }
                break;
            default:
                assert(false && "Illegal state");
        }
    }

    void Node::wait(const NodeState& state) {
        pthread_mutex_lock(&state_cond_mutex_[state]);
        while (!state_mask_.is_set(state))
            pthread_cond_wait(&state_cond_[state], &state_cond_mutex_[state]);
        pthread_mutex_unlock(&state_cond_mutex_[state]);
    }

    void Node::perform(const NodeAction& action) {
        bool rootFlag = isRoot();

        // change the status of the action from queued to in-progress
        pthread_mutex_lock(&mask_mutex_);
        queue_mask_.unset(action);
        in_progress_mask_.set(action);
        pthread_mutex_unlock(&mask_mutex_);

        pthread_spin_lock(&buffer_lock_);
        switch (action) {
            case A_COMPRESS:
                buffer_.compress();
                break;
            case A_DECOMPRESS:
                buffer_.decompress();
                break;
#ifdef ENABLE_PAGING
            case A_PAGEIN:
                buffer_.pageIn();
                break;
            case A_PAGEOUT:
                buffer_.pageOut();
                break;
#endif  // ENABLE_PAGING
            case A_SORT:
#ifdef ENABLE_ASSERT_CHECKS
                assert(rootFlag && "Only root buffer sorted");
#endif  // ENABLE_ASSERT_CHECKS
                sortBuffer();
                aggregateBuffer(A_SORT);
                break;
            case A_MERGE:
#ifdef ENABLE_ASSERT_CHECKS
                assert(!rootFlag && "Non-root buffer always merged");
                pthread_mutex_lock(&mask_mutex_);
                assert(state_mask_.is_set(S_DECOMPRESSED) &&
                        "Node not decompressed!");
                pthread_mutex_unlock(&mask_mutex_);
#endif  // ENABLE_ASSERT_CHECKS
                mergeBuffer();
                aggregateBuffer(A_MERGE);
                break;
            case A_EMPTY:
#ifdef ENABLE_ASSERT_CHECKS
                pthread_mutex_lock(&mask_mutex_);
                assert(state_mask_.is_set(S_AGGREGATED) &&
                        "Node not aggregated!");
                pthread_mutex_unlock(&mask_mutex_);
#endif  // ENABLE_ASSERT_CHECKS
                emptyBuffer();
                if (isLeaf())
                    tree_->handleFullLeaves();
                break;
            default:
                assert(false && "Illegal state");
        }

        // set state
        switch(action) {
            case A_DECOMPRESS:
                // clear the current mask
                pthread_mutex_lock(&mask_mutex_);
                state_mask_.clear();
                state_mask_.set(S_DECOMPRESSED);
                pthread_mutex_unlock(&mask_mutex_);

                done(S_DECOMPRESSED);
                break;
            case A_COMPRESS:
#ifdef ENABLE_ASSERT_CHECKS
                // A_COMPRESS is valid from each of S_DECOMPRESSED,
                // S_COMPRESSED, S_AGGREGATED, and S_PAGED_OUT
                assert(!state_mask_.is_set(S_EMPTY) &&
                        "A_COMPRESS not valid from S_EMPTY");
#endif  // ENABLE_ASSERT_CHECKS
                pthread_mutex_lock(&mask_mutex_);
                if (state_mask_.is_set(S_DECOMPRESSED) ||
                        state_mask_.is_set(S_AGGREGATED)) {
                    state_mask_.clear();
                    state_mask_.set(S_COMPRESSED);
                    pthread_mutex_unlock(&mask_mutex_);
                    done(S_COMPRESSED);
                } else
                    pthread_mutex_unlock(&mask_mutex_);
                break;
#ifdef ENABLE_PAGING
            case A_PAGEIN:
#ifdef ENABLE_ASSERT_CHECKS
                assert((state_mask_.is_set(S_PAGED_OUT) ||
                        state_mask_.is_set(S_COMPRESSED)) &&
                        "A_PAGEIN valid from S_PAGED_OUT || S_COMPRESSED");
#endif  // ENABLE_ASSERT_CHECKS
                pthread_mutex_lock(&mask_mutex_);
                state_mask_.clear();
                state_mask_.set(S_COMPRESSED);
                pthread_mutex_unlock(&mask_mutex_);

                done(S_COMPRESSED);
                break;
            case A_PAGEOUT:
#ifdef ENABLE_ASSERT_CHECKS
                assert(state_mask_.is_set(S_COMPRESSED) &&
                        "A_PAGEOUT only valid from S_COMPRESSED");
#endif  // ENABLE_ASSERT_CHECKS
                pthread_mutex_lock(&mask_mutex_);
                state_mask_.clear();
                state_mask_.set(S_PAGED_OUT);
                pthread_mutex_unlock(&mask_mutex_);

                done(S_PAGED_OUT);
                break;
#endif  // ENABLE_PAGING
            case A_SORT:
            case A_MERGE:
#ifdef ENABLE_ASSERT_CHECKS
                if (!rootFlag)
                    assert(state_mask_.is_set(S_DECOMPRESSED) &&
                            "A_SORT and A_MERGE only valid from S_DECOMPRESSED");
#endif  // ENABLE_ASSERT_CHECKS
                pthread_mutex_lock(&mask_mutex_);
                state_mask_.clear();
                state_mask_.set(S_AGGREGATED);
                pthread_mutex_unlock(&mask_mutex_);

                done(S_AGGREGATED);
                break;
            case A_EMPTY:
#ifdef ENABLE_ASSERT_CHECKS
                assert(state_mask_.is_set(S_AGGREGATED) &&
                        "A_EMPTY only valid from S_AGGREGATED");
#endif  // ENABLE_ASSERT_CHECKS
                // emptying a leaf would have resulted in the leaf splitting
                // and producing two aggregated and uncompressed leaves. For
                // each of the leaves, copyIntoBuffer() would have set the
                // state of the leaves to S_DECOMPRESSED, so we let that be.
                if (!isLeaf()) {
                    pthread_mutex_lock(&mask_mutex_);
                    state_mask_.clear();
                    state_mask_.set(S_EMPTY);
                    pthread_mutex_unlock(&mask_mutex_);
                    done(S_EMPTY);
                }
                break;
            default:
                assert(false && "Which state??");
        }
        pthread_spin_unlock(&buffer_lock_);

        // unset action from in-progress mask
        pthread_mutex_lock(&mask_mutex_);
        in_progress_mask_.unset(action);
        pthread_mutex_unlock(&mask_mutex_);

        if (action == A_EMPTY && rootFlag) {
            tree_->sorter_->submitNextNodeForEmptying();
        }
    }

    bool Node::canEmptyIntoNode() {
        bool ret = true;

        // it is ok to be compressing or paging when adding a new list into the
        // buffer
        Mask in_progress_test_mask;
        in_progress_test_mask.set(A_DECOMPRESS);
        in_progress_test_mask.set(A_COMPRESS);
#ifdef ENABLE_PAGING
        in_progress_test_mask.set(A_PAGEOUT);
        in_progress_test_mask.set(A_PAGEIN);
#endif  // ENABLE_PAGING

        // same for the queue mask
        Mask queue_test_mask = in_progress_test_mask;

        pthread_mutex_lock(&mask_mutex_);
        ret &= (queue_mask_.or_mask(queue_test_mask) ==
                queue_test_mask);        
        ret &= (in_progress_mask_.or_mask(in_progress_test_mask) ==
                in_progress_test_mask);        
        pthread_mutex_unlock(&mask_mutex_);

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

