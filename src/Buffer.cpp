#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include "Buffer.h"
#include "CompressTree.h"
//#include "compsort.h"
//#include "rle.h"
#include "snappy.h"

namespace cbt {
    Buffer::List::List() :
            num_(0),
            size_(0),
            state_(DECOMPRESSED),
            c_hashlen_(0),
            c_sizelen_(0),
            c_datalen_(0)
    {
    }

    Buffer::List::~List()
    {
        deallocate();
    }

    void Buffer::List::allocate(bool isLarge)
    {
        uint32_t nel = cbt::MAX_ELS_PER_BUFFER;
        uint32_t buf = cbt::BUFFER_SIZE;
        if (isLarge) {
            nel *= 2;
            buf *= 2;
        }
        hashes_ = (uint32_t*)malloc(sizeof(uint32_t) * nel);
        sizes_ = (uint32_t*)malloc(sizeof(uint32_t) * nel);
        data_ = (char*)malloc(buf);
    }

    void Buffer::List::deallocate()
    {    
        if (hashes_) { free(hashes_); hashes_ = NULL;}
        if (sizes_) { free(sizes_); sizes_ = NULL;}
        if (data_) { free(data_); data_ = NULL;}
    }

    void Buffer::List::setEmpty()
    {
        num_ = 0;
        size_ = 0;
        state_ = DECOMPRESSED;
    }

    Buffer::Buffer() :
        compressible_(true),
        queuedForCompAct_(false),
        compAct_(NONE)
    {
        pthread_mutex_init(&compActMutex_, NULL);
        pthread_cond_init(&compActCond_, NULL);

#ifdef ENABLE_PAGING
        pthread_mutex_init(&pageMutex_, NULL);
        pthread_cond_init(&pageCond_, NULL);

        pageable_ = true;
        queuedForPaging_ = false;
        pageAct_ = NO_PAGE;
#endif
    }

    Buffer::~Buffer()
    {
        deallocate();

        pthread_mutex_destroy(&compActMutex_);
        pthread_cond_destroy(&compActCond_);
#ifdef ENABLE_PAGING
        pthread_mutex_destroy(&pageMutex_);
        pthread_cond_destroy(&pageCond_);
#endif
    }

    Buffer::List* Buffer::addList(bool isLarge/*=false*/)
    {
        List *l = new List();
        l->allocate(isLarge);
        lists_.push_back(l);
        return l;
    }

    void Buffer::delList(uint32_t ind)
    {
        if (ind < lists_.size()) {
            delete lists_[ind];
            lists_.erase(lists_.begin() + ind);
        }
    }

    void Buffer::addList(Buffer::List* l)
    {
        lists_.push_back(l);
    }

    void Buffer::clear()
    {
        lists_.clear();
    }

    void Buffer::deallocate()
    {
        for (uint32_t i=0; i<lists_.size(); i++)
            lists_[i]->deallocate();
    }

    bool Buffer::empty() const
    {
        return (numElements() == 0);
    }

    uint32_t Buffer::numElements() const
    {
        uint32_t num = 0;
        for (uint32_t i=0; i<lists_.size(); i++)
            num += lists_[i]->num_;
        return num;
    }

    void Buffer::setParent(Node* n)
    {
        node_ = n;
    }

    bool Buffer::compress()
    {
        if (!empty()) {
            // allocate memory for one list
            Buffer compressed;

            for (uint32_t i=0; i<lists_.size(); i++) {
                Buffer::List* l = lists_[i];
                if (l->state_ == Buffer::List::COMPRESSED)
                    continue;
#ifdef ENABLE_PAGING
                if (l->state_ == Buffer::List::PAGED_OUT)
                    continue;
#endif
                compressed.addList();
                // latest added list
                Buffer::List* cl = 
                        compressed.lists_[compressed.lists_.size()-1];
                snappy::RawCompress((const char*)l->hashes_, 
                        l->num_ * sizeof(uint32_t), 
                        (char*)cl->hashes_,
                        &l->c_hashlen_);
                snappy::RawCompress((const char*)l->sizes_, 
                        l->num_ * sizeof(uint32_t), 
                        (char*)cl->sizes_,
                        &l->c_sizelen_);
/*
                compsort::compress(l->hashes_, l->num_,
                        cl->hashes_, (uint32_t&)l->c_hashlen_);
                rle::encode(l->sizes_, l->num_, cl->sizes_,
                        (uint32_t&)l->c_sizelen_);
*/
                snappy::RawCompress(l->data_, l->size_, 
                        cl->data_, 
                        &l->c_datalen_);
                l->deallocate();
                l->hashes_ = cl->hashes_;
                l->sizes_ = cl->sizes_;
                l->data_ = cl->data_;
                l->state_ = Buffer::List::COMPRESSED;
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "compressed list %d in node %d\n",
                        i, node_->id_);
#endif
            }
            // clear compressed list so lists won't be deallocated on return
            compressed.clear();
        }
        return true;
    }

    bool Buffer::decompress()
    {
        if (!empty()) {
            // allocate memory for decompressed buffers
            Buffer decompressed;

            for (uint32_t i=0; i<lists_.size(); i++) {
                Buffer::List* cl = lists_[i];
                if (cl->state_ == Buffer::List::DECOMPRESSED)
                    continue;
                decompressed.addList();
                // latest added list
                Buffer::List* l =
                        decompressed.lists_[decompressed.lists_.size()-1];
                snappy::RawUncompress((const char*)cl->hashes_, 
                        cl->c_hashlen_, (char*)l->hashes_);
                snappy::RawUncompress((const char*)cl->sizes_, 
                        cl->c_sizelen_, (char*)l->sizes_);
/*
                uint32_t siz;
                compsort::decompress(cl->hashes_, (uint32_t)cl->c_hashlen_,
                        l->hashes_, siz);
                rle::decode(cl->sizes_, (uint32_t)cl->c_sizelen_,
                        l->sizes_, siz);
*/
                snappy::RawUncompress(cl->data_, cl->c_datalen_,
                        l->data_);
                cl->deallocate();
                cl->hashes_ = l->hashes_;
                cl->sizes_ = l->sizes_;
                cl->data_ = l->data_;
                cl->state_ = List::DECOMPRESSED;
            }
            // clear decompressed so lists won't be deallocated on return
            decompressed.clear();
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "decompressed node %d; n: %u\n",
                    node_->id_, numElements());
#endif
        }
        return true;
    }

    void Buffer::setCompressible(bool flag)
    {
        pthread_mutex_lock(&compActMutex_);
        compressible_ = flag;
        pthread_mutex_unlock(&compActMutex_);
    }

    bool Buffer::checkCompress()
    {
        pthread_mutex_lock(&compActMutex_);
        // check if node already in compression action list
        if (queuedForCompAct_) {
            // check if compression request has been cancelled
            if (compAct_ == DECOMPRESS) {
                /* This case shouldn't occur */
                fprintf(stderr, "Node %d trying to be compressed while\
                    waiting for decompression\n", node_->id_);
                assert(false);
            } else if (compAct_ == NONE) {
                compAct_ = COMPRESS;
                pthread_mutex_unlock(&compActMutex_);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Node %d reset to compress\n", node_->id_);
#endif
                return false;
            } else {
                /* previous list queued for compression hasn't been compressed
                   yet. No need to add node again */
                pthread_mutex_unlock(&compActMutex_);
                return false;
            }
        } else {
            if (empty()) {
                queuedForCompAct_ = false;
                pthread_mutex_unlock(&compActMutex_);
                return false;
            } else {
                queuedForCompAct_ = true;
                compAct_ = COMPRESS;
                pthread_mutex_unlock(&compActMutex_);
                return true;
            }
        }
    }
    
    bool Buffer::checkDecompress()
    {
        pthread_mutex_lock(&compActMutex_);
        // check if node already in list
        if (queuedForCompAct_) {
            /* check if compression request is outstanding and cancel this */
            if (compAct_ == COMPRESS || compAct_ == NONE) {
                compAct_ = DECOMPRESS;
                pthread_mutex_unlock(&compActMutex_);
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "Node %d reset to decompress\n", node_->id_);
#endif
                return false;
            } else { // we're decompressing twice
                fprintf(stderr, "Trying to decompress node %d twice", node_->id_);
                assert(false);
            }
        } else {
            // check if the buffer is empty;
            if (empty()) {
                queuedForCompAct_ = false;
                pthread_mutex_unlock(&compActMutex_);
                return false;
            } else {
                queuedForCompAct_ = true;
                compAct_ = DECOMPRESS;
                pthread_mutex_unlock(&compActMutex_);
                return true;
            }
        }
    }
    
    void Buffer::waitForCompressAction(const CompressionAction& act)
    {
        // make sure the buffer has been decompressed
        pthread_mutex_lock(&compActMutex_);
        while (queuedForCompAct_ && compAct_ == act)
            pthread_cond_wait(&compActCond_, &compActMutex_);
        pthread_mutex_unlock(&compActMutex_);
    }

    void Buffer::performCompressAction()
    {
        pthread_mutex_lock(&compActMutex_);
        if (compAct_ == COMPRESS) {
            compress();
            // signal to agent waiting for completion.
        } else if (compAct_ == DECOMPRESS) {
            pthread_mutex_unlock(&compActMutex_);
#ifdef ENABLE_PAGING
            waitForPageAction(PAGE_IN);
#endif
            pthread_mutex_lock(&compActMutex_);
            decompress();
        }
        pthread_cond_signal(&compActCond_);
        queuedForCompAct_ = false;
        compAct_ = NONE;
        pthread_mutex_unlock(&compActMutex_);
    }

    Buffer::CompressionAction Buffer::getCompressAction()
    {
        pthread_mutex_lock(&compActMutex_);
        CompressionAction act = compAct_;
        pthread_mutex_unlock(&compActMutex_);
        return act;
    }    

#ifdef ENABLE_PAGING
    bool Buffer::pageOut()
    {
        if (!empty()) {
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "paged out node %d; lists: ", node_->id_);
#endif
            for (uint32_t i=0; i<lists_.size(); i++) {
                Buffer::List* l = lists_[i];
                /* List may be already paged out or yet to be compressed */
                if (l->state_ != List::COMPRESSED)
                    continue;
                size_t ret1, ret2, ret3;
                ret1 = fwrite(l->hashes_, 1, l->c_hashlen_, f_);
                ret2 = fwrite(l->sizes_, 1, l->c_sizelen_, f_);
                ret3 = fwrite(l->data_, 1, l->c_datalen_, f_);
#ifdef ENABLE_ASSERT_CHECKS
                if (ret1 != l->c_hashlen_ || ret2 != l->c_sizelen_ ||
                        ret3 != l->c_datalen_) {
                    fprintf(stderr, "Node %d page-out fail! Error: %s\n",
                            node_->id_, strerror(errno));
                    fprintf(stderr, "HL:%ld;RHL:%ld\nSL:%ld;RSL:%ld\nDL:%ld;RDL:%ld\n",
                            l->c_hashlen_, ret1, l->c_sizelen_, ret2,
                            l->c_datalen_, ret3);
                    assert(false);
                }
#endif
                l->deallocate();
                l->state_ = List::PAGED_OUT;
#ifdef CT_NODE_DEBUG
                fprintf(stderr, "%d (%lu), ", i, lists_[i]->num_);
#endif
            }
        }
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "\n");
#endif
        return true;
    }

    bool Buffer::pageIn()
    {
        // set file pointer to beginning of file
        rewind(f_);

#ifdef CT_NODE_DEBUG
        fprintf(stderr, "paged in node %d; lists: ", node_->id_);
#endif

        Buffer paged_in;
        for (uint32_t i=0; i<lists_.size(); i++) {
            List* l = lists_[i];
            // check if the list is already paged in
            if (l->state_ != List::PAGED_OUT)
                continue;
            List* pgin_list = paged_in.addList();
            size_t ret1, ret2, ret3;
            ret1 = fread(pgin_list->hashes_, 1, l->c_hashlen_, f_);
            ret2 = fread(pgin_list->sizes_, 1, l->c_sizelen_, f_);
            ret3 = fread(pgin_list->data_, 1, l->c_datalen_, f_);
#ifdef ENABLE_ASSERT_CHECKS
            if (ret1 != l->c_hashlen_ || ret2 != l->c_sizelen_ ||
                    ret3 != l->c_datalen_) {
                fprintf(stderr, "Node %d page-in fail! Error: %s\n",
                        node_->id_, strerror(errno));
                fprintf(stderr, "HL:%ld;RHL:%ld\nSL:%ld;RSL:%ld\nDL:%ld;RDL:%ld\n",
                        l->c_hashlen_, ret1, l->c_sizelen_, ret2,
                        l->c_datalen_, ret3);
                assert(false);
            }
#endif
#ifdef CT_NODE_DEBUG
            fprintf(stderr, "%d (%lu), ", i, lists_[i]->num_);
#endif
            l->hashes_ = pgin_list->hashes_;
            l->sizes_ = pgin_list->sizes_;
            l->data_ = pgin_list->data_;
            l->state_ = List::COMPRESSED;
        }
#ifdef CT_NODE_DEBUG
        fprintf(stderr, "\n");
#endif
        // clear paged_in to prevent deallocation on return
        paged_in.clear();

        // set file pointer to beginning of file again
        rewind(f_);

        return true;
    }

    void Buffer::setPageable(bool flag)
    {
        pthread_mutex_lock(&pageMutex_);
        pageable_ = flag;
        pthread_mutex_unlock(&pageMutex_);
    }

    void Buffer::setupPaging()
    {
        char* fileName = (char*)malloc(100);
        char* nodeNum = (char*)malloc(10);
        strcpy(fileName, "/localfs/hamur/minni_data/");
        sprintf(nodeNum, "%d", node_->id_);
        strcat(fileName, nodeNum);
        strcat(fileName, ".buf");
        f_ = fopen(fileName, "w+");
        if (f_ == NULL) {
            fprintf(stderr, "Error opening file: %s\n", strerror(errno));
            assert(false);
        }
        free(fileName);
        free(nodeNum);
    }

    void Buffer::cleanupPaging()
    {
        fclose(f_);
    }

    bool Buffer::checkPageOut()
    {
        pthread_mutex_lock(&pageMutex_);
        // check if node already in list
        if (queuedForPaging_) {
            if (pageAct_ == PAGE_IN) {
                /* Shouldn't happen */
                assert(false);
                return false;
            } else { // we're paging out twice
                assert(false);
                return false;
            }
        } else {
            queuedForPaging_ = true;
            pageAct_ = PAGE_OUT;
            pthread_mutex_unlock(&pageMutex_);
            return true;
        }
    }

    bool Buffer::checkPageIn()
    {
        pthread_mutex_lock(&pageMutex_);
        // check if node already in list
        if (queuedForPaging_) {
            // check if page-out request is outstanding and cancel if so
            if (pageAct_ == PAGE_OUT) {
                // reset action request; node need not be added again
                pageAct_ = PAGE_IN;
                pthread_mutex_unlock(&pageMutex_);
                return false;
            } else { // we're paging-in twice
                assert(false);
            }
        } else {
            queuedForPaging_ = true;
            pageAct_ = PAGE_IN;
            pthread_mutex_unlock(&pageMutex_);
            return true;
        }
    }

    void Buffer::waitForPageAction(const PageAction& act)
    {
        pthread_mutex_lock(&pageMutex_);
        while (queuedForPaging_ && pageAct_ == act)
            pthread_cond_wait(&pageCond_, &pageMutex_);
        pthread_mutex_unlock(&pageMutex_);
    }

    bool Buffer::performPageAction()
    {
        bool ret = true;
        pthread_mutex_lock(&pageMutex_);
        if (pageAct_ == PAGE_OUT) {
            if (lists_[lists_.size()-1]->state_ != List::COMPRESSED)
                ret = false;
            else
                pageOut();
        } else if (pageAct_ == PAGE_IN) {
            pageIn();
        }
        if (ret) {
            pthread_cond_signal(&pageCond_);
            queuedForPaging_ = false;
            pageAct_ = NO_PAGE;
        }
        pthread_mutex_unlock(&pageMutex_);
        return ret;
    }

    Buffer::PageAction Buffer::getPageAction()
    {
        return pageAct_;
    }    
#endif //ENABLE_PAGING
}
