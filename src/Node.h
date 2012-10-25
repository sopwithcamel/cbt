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
    class Emptier;
    class Compressor;
    class Merger;

    class Node {
        friend class CompressTree;
        friend class Buffer;
        friend class Compressor;
        friend class Emptier;
        friend class Merger;
        friend class Sorter;
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

        uint32_t level() const;
        uint32_t id() const;

      private:
        /* Buffer handling functions */

        bool emptyOrEgress();
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
        /* copy contents from node's buffer into this buffer. Starting from
         * index = index, copy num elements' data.
         */
        bool copyIntoBuffer(Buffer::List* l, uint32_t index, uint32_t num);
        // switch the input and emptying buffers
        void switchBuffers();

        // double-buffering stuff
        bool both_buffers_full();
        void set_both_buffers_full(bool both_full);

        Buffer* buffer(BufferType type) const {
            return (type == INSERT_BUFFER? input_buffer_ : empty_buffer_);
        }

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
        /* Compression-related functions */

        // return value indicates whether the node needs to be added or
        // if it's already present in the queue
        bool checkEgress(BufferType type);
        bool checkIngress(BufferType type);

        //
        // management of queues
        //
        // Actually perform action. This assumes that it is ok to perform the
        // action and does not check. For example, perform(MERGE) will directly
        // sort/merge the buffer. The caller has to ensure that the buffer is
        // already decompressed.
        void perform(BufferType type);
        // Check if the node is currently queued up for Action act and
        // block until receipt of signal indicating completion.
        void wait(const Action& act);
        // Signal that Action act is complete.
        void done(const Action& act);
        // 
        void schedule(BufferType type, const Action& act);

        /* pointer to the tree */
        CompressTree* tree_;
        /* Buffer */
        Buffer* input_buffer_;
        Buffer* empty_buffer_;
        bool empty_buffer_available_;
        bool both_buffers_full_;
        pthread_spinlock_t buffer_availability_lock_;

        pthread_mutex_t stateMutex_;
        uint32_t id_;
        /* level in the tree; 0 at leaves and increases upwards */
        uint32_t level_;
        Node* parent_;

        /* Pointers to children */
        std::vector<Node*> children_;
        uint32_t separator_;

        pthread_cond_t emptyCond_;
        pthread_mutex_t emptyMutex_;

        pthread_cond_t sortCond_;
        pthread_mutex_t sortMutex_;

        pthread_cond_t xgressCond_;
        pthread_mutex_t xgressMutex_;
    };
}

#endif  // SRC_NODE_H_
