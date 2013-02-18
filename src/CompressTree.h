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
#include <semaphore.h>
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
        FLUSH,
        IF_FULL
    };

    class Node;
    class Emptier;
    class Decompressor;
    class Compressor;
    class Merger;
    class Sorter;
#ifdef ENABLE_COUNTERS
    class Monitor;
#endif  // ENABLE_COUNTERS

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
        friend class Buffer;
        friend class Node;
        friend class Slave;
        friend class Decompressor;
        friend class Emptier;
        friend class Merger;
        friend class Sorter;

        Node* getEmptyRootNode();
        void addEmptyRootNode(Node* n);
        void submitNodeForEmptying(Node* n);
        bool rootNodeAvailable();
        bool addLeafToEmpty(Node* node);
        bool createNewRoot(Node* otherChild);
        void emptyTree();
        /* Write out all buffers to leaves. Do this before reading */
        bool flushBuffers();
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
        // used as unique prefix for filenames
        uint32_t tree_prefix_;

        std::deque<Node*> emptyRootNodes_;
        pthread_mutex_t emptyRootNodesMutex_;
        pthread_cond_t emptyRootAvailable_;

        bool allFlush_;
        bool empty_;
        sem_t sleepSemaphore_;
        sem_t decompressedSemaphore_;
        EmptyType emptyType_;
        std::deque<Node*> leavesToBeEmptied_;
        std::vector<Node*> allLeaves_;
        uint32_t lastLeafRead_;
        uint32_t lastOffset_;
        uint32_t lastElement_;

        /* Slave-threads */
        bool threadsStarted_;
        pthread_barrier_t threadsBarrier_;

        Emptier* emptier_;
        Sorter* sorter_;
        Merger* merger_;
        Compressor* compressor_;
        Decompressor* decompressor_;
    };
}

#endif  // SRC_COMPRESSTREE_H_
