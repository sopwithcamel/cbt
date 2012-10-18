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

#ifndef SRC_COMPRESSTREE_H_
#define SRC_COMPRESSTREE_H_

#include <pthread.h>
#include <deque>
#include <queue>
#include <vector>
#include "Config.h"
#include "Node.h"
#include "PartialAgg.h"

namespace cbt {

    extern uint32_t BUFFER_SIZE;
    extern uint32_t MAX_ELS_PER_BUFFER;
    extern uint32_t EMPTY_THRESHOLD;

    enum CompressAlgorithm {
        SNAPPY,
        ZLIB
    };

    enum EmptyType {
        ALWAYS,
        IF_FULL
    };

    class Node;
    class Emptier;
    class Compressor;
    class Sorter;
    class Pager;
#ifdef ENABLE_COUNTERS
    class Monitor;
#endif

    class CompressTree {
      public:
        CompressTree(uint32_t a, uint32_t b, uint32_t nodesInMemory,
                uint32_t buffer_size, uint32_t pao_size,
                const Operations* const ops);
        ~CompressTree();

        /* Insert record into tree */
        bool insert(PartialAgg* agg);
        bool bulk_insert(PartialAgg** paos, uint64_t num);
        /* read values */
        // returns true if there are more values to be read and false otherwise
        bool bulk_read(PartialAgg** pao_list, uint64_t& num_read,
                uint64_t max);
        bool nextValue(void*& hash, PartialAgg*& agg);
        void clear();

      private:
        friend class Node;
        friend class Slave;
        friend class Emptier;
        friend class Sorter;
        friend class Pager;
#ifdef ENABLE_COUNTERS
        friend class Monitor;
#endif
        Node* getEmptyRootNode();
        void addEmptyRootNode(Node* n);
        void addFullRootNode(Node* n);
        void rootNodeAvailable();
        bool addLeafToEmpty(Node* node);
        bool createNewRoot(Node* otherChild);
        void emptyTree();
        /* Write out all buffers to leaves. Do this before reading */
        bool flushBuffers();
        void handleFullLeaves();
        void startThreads();
        void stopThreads();

      private:
        // (a,b)-tree...
        const uint32_t a_;
        const uint32_t b_;
        uint32_t nodeCtr;
        const Operations* const ops;
        CompressAlgorithm alg_;
        Node* rootNode_;
        Node* inputNode_;

        std::deque<Node*> emptyRootNodes_;
        pthread_mutex_t emptyRootNodesMutex_;
        pthread_mutex_t fullRootNodesMutex_;
        std::deque<Node*> fullRootNodes_;

        pthread_cond_t emptyRootAvailable_;

        bool allFlush_;
        EmptyType emptyType_;
        std::deque<Node*> leavesToBeEmptied_;
        std::vector<Node*> allLeaves_;
        uint32_t lastLeafRead_;
        uint32_t lastOffset_;
        uint32_t lastElement_;

        /* Slave-threads */
        bool threadsStarted_;
        pthread_barrier_t threadsBarrier_;

        /* Eviction-related */
        uint32_t nodesInMemory_;
        uint32_t numEvicted_;
        char* evictedBuffer_;
        uint32_t evictedBufferOffset_;
        pthread_mutex_t evictedBufferMutex_;

        /* Members for async-emptying */
        Emptier* emptier_;

        /* Members for async-sorting */
        Sorter* sorter_;

        /* Compression-related */
        Compressor* compressor_;

#ifdef ENABLE_PAGING
        /* Paging */
        Pager* pager_;
#endif

#ifdef ENABLE_COUNTERS
        /* Monitor */
        Monitor* monitor_;
#endif
    };
}

#endif  // SRC_COMPRESSTREE_H_
