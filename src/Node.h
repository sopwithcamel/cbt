#ifndef LIB_COMPRESS_NODE_H
#define LIB_COMPRESS_NODE_H
#include <iostream>
#include <stdint.h>
#include <string>
#include <vector>

#include "Buffer.h"
#include "CompressTree.h"
#include "Config.h"
#include "ProtobufPartialAgg.h"

#define CALL_MEM_FUNC(object,ptrToMember) ((object).*(ptrToMember))

namespace cbt {

    class Buffer;
    class CompressTree;
    class Emptier;
    class Compressor;
    class Sorter;

    class Node {
        friend class CompressTree;
        friend class Buffer;
        friend class Compressor;
        friend class Emptier;
        friend class Sorter;
        friend class Pager;

        class MergeElement {
          public:
            MergeElement(Buffer::List* l)
            {
                ind = off = 0;
                list = l;
            }
            uint32_t hash()
            {
                return list->hashes_[ind];
            }
            uint32_t size()
            {
                return list->sizes_[ind];
            }
            char* data()
            {
                return list->data_ + off;
            }
            bool next()
            {
                if (ind >= list->num_-1) {
                    return false;
                }
                off += list->sizes_[ind];
                ind++;
                return true;
            }                
            uint32_t ind;           // index of hash being compared
            uint32_t off;           // offset of serialized PAO
            Buffer::List* list;     // list containing element
        };

        class MergeComparator {
          public:
            bool operator()(const MergeElement& lhs,
                    const MergeElement& rhs) const
            {
                return (lhs.list->hashes_[lhs.ind] >
                        rhs.list->hashes_[rhs.ind]);
            } 
        };
      public:
        Node(CompressTree* tree, uint32_t level);
        ~Node();
        /* copy user data into buffer. Buffer should be decompressed
           before calling. */
        bool insert(uint64_t hash, PartialAgg* agg);

        // identification functions
        bool isLeaf() const;
        bool isRoot() const;

        bool isFull() const;
        uint32_t level() const;
        uint32_t id() const;
      private:
        /* Buffer handling functions */

        bool emptyOrCompress();
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
        /* Aggregate the sorted root buffer */
        bool aggregateSortedBuffer();
        bool aggregateMergedBuffer();
        /* Merge the sorted sub-lists of the buffer */
        bool mergeBuffer();
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

        /* Sorting-related functions */
        void quicksort(uint32_t left, uint32_t right);
        void waitForSort();

        /* Compression-related functions */
        void scheduleBufferCompressAction(const Buffer::CompressionAction& act);
        void waitForCompressAction(const Buffer::CompressionAction& act);
        void performCompressAction();
        Buffer::CompressionAction getCompressAction();

#ifdef ENABLE_PAGING
        /* Paging-related functions */
        void scheduleBufferPageAction(const Buffer::PageAction& act);
        void waitForPageAction(const Buffer::PageAction& act);
        bool performPageAction();
        Buffer::PageAction getPageAction();
#endif // ENABLE_PAGING

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
        ProtobufPartialAgg *lastPAO, *thisPAO;

        /* Pointers to children */
        std::vector<Node*> children_;
        uint32_t separator_;

        /* Emptying related */
        bool queuedForEmptying_;
        pthread_mutex_t queuedForEmptyMutex_;
        char** perm_;
    };
}

#endif
