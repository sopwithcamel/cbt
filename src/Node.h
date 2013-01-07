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
#include "Mask.h"

namespace cbt {

    class Buffer;
    class CompressTree;
    class Emptier;
    class Compressor;
    class Merger;

    enum NodeState {
        // No PAOs in buffer
        S_EMPTY = 0,
        // All lists in buffer are decompressed
        S_DECOMPRESSED = 1,
        // Single aggregated list (only one PAO in buffer per key)
        S_AGGREGATED = 2,
        // At least one list in compressed state; the rest are decompressed
        S_COMPRESSED = 3,
        // At least one list is paged-out; the rest are decompressed or
        // compressed
        S_PAGED_OUT = 4,
    };

    enum NodeAction {
        A_DECOMPRESS = 0,
        A_COMPRESS = 1,
#ifdef ENABLE_PAGING
        A_PAGEIN = 2,
        A_PAGEOUT = 3,
#endif  // ENABLE_PAGING
        A_SORT = 4,
        A_MERGE = 5,
        A_EMPTY = 6,
    };

    class Node {
        friend class CompressTree;
        friend class Buffer;
        friend class Compressor;
        friend class Emptier;
        friend class Merger;
        friend class Sorter;
        friend class Pager;
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
        bool aggregateBuffer(const NodeAction& act);
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
        bool checkIntegrity();
        bool checkSerializationIntegrity(int listn=-1);


        //
        // management of queues
        //
        // Actually perform action. This assumes that it is ok to perform the
        // action and does not check. For example, perform(MERGE) will directly
        // sort/merge the buffer. The caller has to ensure that the buffer is
        // already decompressed.
        void perform(const NodeAction& action);
        // Check if the node is currently in some state and block until receipt
        // of signal indicating that the state has been reached.
        void wait(const NodeState& state);
        // Work that must be done on the completion of a certain action. This
        // includes setting and resetting of state and schedule bitmasks as
        // well as signalling the reaching of some state being waited upon
        void done(const NodeAction& state);
        // Queue a node for the performance of action act. This function takes
        // care of checking whether the node must be queued and making
        // appropriate changes to queueing masks
        void schedule(const NodeAction& act);

        bool canEmptyIntoNode();

      private:
        /* pointer to the tree */
        CompressTree* tree_;
        /* Buffer */
        Buffer buffer_;
        // lock to prevent multiple actions modifying buffer state at the same
        // time
        pthread_spinlock_t buffer_lock_;
        uint32_t id_;
        /* level in the tree; 0 at leaves and increases upwards */
        uint32_t level_;
        Node* parent_;

        /* Pointers to children */
        std::vector<Node*> children_;
        uint32_t separator_;

        // Bit-masks that indicate the current state of the node and requested
        // actions respectively, along with locks to protect them
        Mask state_mask_;
        pthread_cond_t state_cond_;
        pthread_mutex_t state_mask_mutex_;

        Mask queue_mask_;
        pthread_mutex_t queue_mask_mutex_;

        Mask in_progress_mask_;
        pthread_mutex_t in_progress_mask_mutex_;
    };
}

#endif  // SRC_NODE_H_
