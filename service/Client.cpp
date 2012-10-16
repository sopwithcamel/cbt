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

#include <dlfcn.h>
#include <math.h>
#include <stdint.h>
#include <stdlib.h>

#include <zmq.hpp>
#include "zhelpers.hpp"
#include <string>
#include <iostream>

#include "Client.h"
#include "HashUtil.h"
#include "PartialAgg.h"
#include "ProtobufPartialAgg.h"

using namespace google::protobuf::io;

namespace cbtservice {

    CBTClient::CBTClient(uint32_t u, uint32_t l) :
            kNumUniqKeys(u),
            kKeyLen(l),
            kNumFillers(10000),
            kLettersInAlphabet(26),
            kMaxPAOs(200000) {
        num_full_loops_ = (int)floor(Conv26(log2(kNumUniqKeys)));
        part_loop_ = (int)ceil(kNumUniqKeys / pow(26, num_full_loops_));

        assert(LinkUserMap());

        GenerateFillers(kKeyLen - num_full_loops_);
    }

    CBTClient::~CBTClient() {
    }

    void CBTClient::Run() {
        //  Prepare our context and socket
        zmq::context_t context (1);
        zmq::socket_t socket (context, ZMQ_REQ);

        std::cout << "Connecting to CBTServer" << std::endl;
        socket.connect ("tcp://localhost:5555");

        uint32_t number_of_paos = 100000;

        for (int request_nbr = 0; ; request_nbr++) {
            std::stringstream ss;
            std::vector<PartialAgg*> paos;
            GeneratePAOs(paos, number_of_paos);

            OstreamOutputStream* os = new OstreamOutputStream(&ss);
            CodedOutputStream* cs = new CodedOutputStream(os);
            SerializePAOs(paos, cs);

            delete cs;
            delete os;
            
            s_send(socket, ss.str());

            DeletePAOs(paos);
            paos.clear();

            //  Get the reply.
            zmq::message_t reply;
            socket.recv(&reply);
            assert(!strcmp(reinterpret_cast<char*>(reply.data()), "True"));
        }
    }

    void CBTClient::GenerateFillers(uint32_t filler_len) {
        for (uint32_t i = 0; i < kNumFillers; ++i) {
            std::string f;
            for (uint32_t j = 0; j < filler_len; ++j)
                f[j] = 97 + rand() % kLettersInAlphabet;
            fillers_.push_back(f);
        }
    }

    void CBTClient::GeneratePAOs(std::vector<PartialAgg*>& paos,
            uint32_t number_of_paos) {
        char* word = new char[kKeyLen + 1];
        assert(number_of_paos < kMaxPAOs);
        Token t;
        for (uint32_t i = 0; i < number_of_paos; ++i) {
            for (uint32_t j=0; j < num_full_loops_; j++)
                word[j] = 97 + rand() % kLettersInAlphabet;
            word[num_full_loops_] = 97 + rand() % part_loop_;
            word[num_full_loops_ + 1] = '\0';

            uint32_t filler_number = HashUtil::MurmurHash(word, 42)
                    % kNumFillers;

            strncat(word, fillers_[filler_number].c_str(), kKeyLen -
                    num_full_loops_ - 1);

            t.tokens.push_back(word);
            PartialAgg* agg;
            // TODO: Fix foolishness
            to_->createPAO(NULL, &agg);
            to_->createPAO(&t, &agg);
            paos.push_back(agg);
            t.clear();
        }
        delete[] word;
    }

    bool CBTClient::LinkUserMap() { 
        const char* err;
        void* handle;
        std::string soname = "/usr/local/lib/minni/wc_proto.so";
        handle = dlopen(soname.c_str(), RTLD_LAZY);
        if (!handle) {
            fputs(dlerror(), stderr);
            return false;
        }

        Operations* (*create_ops_obj)() = (Operations* (*)())dlsym(handle,
                "__libminni_create_ops");
        if ((err = dlerror()) != NULL) {
            fprintf(stderr, "Error locating symbol __libminni_create_ops\
                    in %s\n", err);
            exit(-1);
        }
        to_ = create_ops_obj();
        return true;
    }

    void CBTClient::SerializePAOs(const std::vector<PartialAgg*>& paos,
            CodedOutputStream* cs) {
        std::vector<PartialAgg*>::const_iterator it = paos.begin();
        
//        std::cout << "Wrote " << paos.size() << " PAOs" << std::endl;
        for ( ; it != paos.end(); ++it) {
            bool ret = static_cast<ProtobufOperations*>(to_)->serialize(
                    static_cast<PartialAgg*>(*it), cs);
        }
    }

    void CBTClient::DeletePAOs(const std::vector<PartialAgg*>& paos) {
        std::vector<PartialAgg*>::const_iterator it = paos.begin();
        for ( ; it != paos.end(); ++it) {
           to_->destroyPAO(*it);
        }
    }
} // cbtservice


#define USAGE "%s <Number of unique keys> <Length of a key>\n"

int main (int argc, char* argv[])
{
    if (argc < 3) {
        fprintf(stdout, USAGE, argv[0]);
        exit(EXIT_FAILURE);
    }

    uint32_t uniq = atoi(argv[1]);
    uint32_t len = atoi(argv[2]);

    cbtservice::CBTClient* client = new cbtservice::CBTClient(uniq, len);
    client->Run();
    return 0;
}
