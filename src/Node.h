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

#ifndef SRC_NODE_H_
#define SRC_NODE_H_
#include <stdint.h>
#include <string>
#include <vector>

#include "Buffer.h"
#include "CompressTree.h"
#include "Config.h"
#include "PartialAgg.h"

namespace cbt {

    class Buffer;
    class CompressTree;
#ifdef PIPELINED_IMPL
    class Emptier;
    class Compressor;
    class Merger;
#else  // !PIPELINED_IMPL
    class Genie;
#endif  // PIPELINED_IMPL

    enum NodeState {
        DEFAULT = 0,
        DECOMPRESS = 1,
        COMPRESS = 2,
        SORT = 3,
        MERGE = 4,
        EMPTY = 5,
    };

    struct StateMask {
      public:
        StateMask() : mask_(1) {
            pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);
        }
        ~StateMask() {
            pthread_spin_destroy(&lock_);
        }

        bool is_set(const NodeState& state) {
            uint32_t a = 1 << state;
            pthread_spin_lock(&lock_);
            bool ret = a & mask_;
            pthread_spin_unlock(&lock_);
            return (ret != 0);
        }

        void clear() {
            pthread_spin_lock(&lock_);
            mask_ = 0;
            pthread_spin_unlock(&lock_);
        }

        void set(const NodeState& state) {
            uint32_t a = 1 << state;
            pthread_spin_lock(&lock_);
            mask_ = a | mask_;
            pthread_spin_unlock(&lock_);
        }

        void unset(const NodeState& state) {
            uint32_t a = 0xffffffff - (1 << state);
            pthread_spin_lock(&lock_);
            mask_ = a & mask_;
            pthread_spin_unlock(&lock_);
        }

        bool zero() {
            pthread_spin_lock(&lock_);
            bool ret = mask_;
            pthread_spin_unlock(&lock_);
            return (ret == 0);
        }

        uint32_t and_mask(uint32_t m) {
            pthread_spin_lock(&lock_);
            uint32_t ret = mask_ & m;
            pthread_spin_unlock(&lock_);
            return ret;
        }

        uint32_t or_mask(uint32_t m) {
            pthread_spin_lock(&lock_);
            uint32_t ret = mask_ | m;
            pthread_spin_unlock(&lock_);
            return ret;
        }
      private:
        uint32_t mask_;
        pthread_spinlock_t lock_;
    };


    class Node {
        friend class CompressTree;
        friend class Buffer;
        friend class Compressor;
#ifdef PIPELINED_IMPL
        friend class Emptier;
        friend class Merger;
        friend class Sorter;
        friend class Pager;
#else  // !PIPELINED_IMPL
        friend class Genie;
#endif  // PIPELINED_IMPL
        friend class Slave;
        friend class PriorityDAG;

      public:
        explicit Node(CompressTree* tree, uint32_t level);
        ~Node();
        /* copy user data into buffer. Buffer should be decompressed
           before calling. */
        bool insert(PartialAgg* agg);

        // identification functions
        bool isLeaf() const;
        bool isRoot() const;

        bool isFull() const;
        uint32_t level() const;
        uint32_t id() const;

      private:
        /* Buffer handling functions */

        bool emptyOrCompress();
        /* Responsible for handling the spilling of the buffer. */
        bool spillBuffer();
        /* Function: empty the buffer into the buffers in the next level.
         *  + Must be called with buffer decompressed.
         *  + Buffer will be freed after invocation.
         *  + If children buffers overflow, it recursively calls itself.
         *    until the recursion reaches the leaves. At this stage, handling
         *    the leaf buffer overflows is queued for later because this may
         *    cause splitting (recursively) up the tree which is best done
         *    when no internal nodes are over-full.
         *  + an emptyBuffer() invocation should be followed by a
         *    handleFullLeaves() call.
         */
        bool emptyBuffer();
        /* Sort the root buffer based on hash value. All other nodes can
         * aggregating by merging. */
        bool sortBuffer();
        /* Merge the buffer based on hash value */
        bool mergeBuffer();
        /* Aggregate the sorted/merged buffer */
        bool aggregateBuffer(const NodeState& act);
        /* copy contents from node's buffer into this buffer. Starting from
         * index = index, copy num elements' data.
         */
        bool copyIntoBuffer(Buffer::List* l, uint32_t index, uint32_t num);

        /* Tree-related functions */

        /* split leaf node and return new leaf */
        Node* splitLeaf();
        /* Add a new child to the node; the child type indicates which side
         * of the separator the child must be inserted.
         * if the number of children is more than the allowed number:
         * + first check if siblings have fewer children
         * + if not, split the node into two and call addChild recursively
         */
        bool addChild(Node* newNode);
        /* Split non-leaf node; must be called with the buffer decompressed
         * and sorted. If called on the root, then a new root is created */
        bool splitNonLeaf();
        bool checkSerializationIntegrity(int listn=-1);


        //
        // management of queues
        //
        // Actually perform action. This assumes that it is ok to perform the
        // action and does not check. For example, perform(MERGE) will directly
        // sort/merge the buffer. The caller has to ensure that the buffer is
        // already decompressed.
        void perform(const NodeState& state);
        // Check if the node is currently scheduled for action state and block
        // until receipt of signal indicating completion.
        void wait(const NodeState& state);
        // Signal that the state waited upon has been reached
        void done(const NodeState& state);
        // 
        void schedule(const NodeState& act);

        // Ugh. This is required because a single compressor queue handles both
        // compression and decompression. The Slave, unfortunately, needs to
        // know which of these is being performed, and we don't want to expose
        // the locking etc. to the Slave.
        bool canEmptyIntoNode();

      private:
        /* pointer to the tree */
        CompressTree* tree_;
        /* Buffer */
        Buffer buffer_;
        pthread_mutex_t stateMutex_;
        uint32_t id_;
        /* level in the tree; 0 at leaves and increases upwards */
        uint32_t level_;
        Node* parent_;

        /* Pointers to children */
        std::vector<Node*> children_;
#ifndef PIPELINED_IMPL
        pthread_spinlock_t children_lock_;
#endif  // PIPELINED_IMPL
        uint32_t separator_;

        // Bit-masks that indicate the current state of the node and requested
        // actions respectively, along with locks to protect them
        StateMask state_mask_;
        StateMask schedule_mask_;

        pthread_cond_t emptyCond_;
        pthread_mutex_t emptyMutex_;

        pthread_cond_t sortCond_;
        pthread_mutex_t sortMutex_;

        pthread_cond_t compCond_;
        pthread_mutex_t compMutex_;

#ifdef ENABLE_PAGING
        pthread_cond_t pageCond_;
        pthread_mutex_t pageMutex_;
#endif  // ENABLE_PAGING
    };
}

#endif  // SRC_NODE_H_
