#ifndef LIB_COMPRESS_EMPTYQUEUE_H
#define LIB_COMPRESS_EMPTYQUEUE_H
#include <tr1/unordered_map>
#include <stdint.h>
#include <list>

#include "Node.h"

namespace cbt {
    class Node;
    struct NodeID {
        uint32_t operator()(const Node* node) const
        {
            return node->id();
        }
    };
    struct NodeEqual {
        bool operator()(const Node* lhs, const Node* rhs) const
        {
            return (lhs->id() == rhs->id());
        }
    };
    typedef std::tr1::unordered_map<Node*, uint32_t, NodeID, NodeEqual> PQ;
    class EmptyQueue {
      public:
        EmptyQueue() {}
        ~EmptyQueue() {}

        void insert(Node* n, uint32_t prio)
        {
            nodeList_[n] = prio;
        }

        Node* pop()
        {
            PQ::iterator it = nodeList_.begin();
            uint32_t maxPrio = it->second;
            PQ::iterator maxNode = it;
            ++it;
            for (; it != nodeList_.end(); ++it) {
                if (it->second > maxPrio) {
                    maxNode = it;
                    maxPrio = it->second;
                }
            }
            Node* retNode = maxNode->first;
            nodeList_.erase(maxNode);
            return retNode;
        }

        void erase(Node* n)
        {
        }

        bool empty() const
        {
            return nodeList_.empty();
        }

        size_t size() const
        {
            return nodeList_.size();
        }

        void printElements() const
        {
            for (PQ::const_iterator it=nodeList_.begin(); it != nodeList_.end(); 
                    ++it)
                fprintf(stderr, "%d(%d), ", it->first->id(), it->second);
            fprintf(stderr, "\n");
        }
      private:
        PQ nodeList_;
    };
}
#endif
