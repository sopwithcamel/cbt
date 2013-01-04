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

#include <gflags/gflags.h>
#include <zmq.hpp>
#include "zmq_helpers.hpp"
#include <string>
#include <iostream>

#include "Client.h"
#include "HashUtil.h"
#include "PartialAgg.h"
#include "ProtobufPartialAgg.h"
#include "Zipf.h"

using namespace google::protobuf::io;

DEFINE_uint64(unique, 0, "Number of unique keys");
DEFINE_int32(length, 0, "Key length");
DEFINE_bool(powerlaw, false, "Use power-law input distribution");

#define S(x) #x
#define SX(x) S(x)

namespace cbtservice {

    CBTClient::CBTClient(uint32_t u, uint32_t l, InputDistribution d) :
            kNumUniqKeys(u),
            kKeyLen(l),
            kInputDist(d),
            kNumFillers(10000),
            kLettersInAlphabet(26),
            kMaxPAOs(200000),
            kMaxUniquePAOs(10000000) {
        assert(LinkUserMap());
        recv_paos_ = reinterpret_cast<PartialAgg**>(malloc
                (sizeof(PartialAgg*) * kMaxUniquePAOs));
        for (uint32_t i = 0; i < kMaxUniquePAOs; ++i) {
            to_->createPAO(NULL, &recv_paos_[i]);
        }
            
    }

    CBTClient::~CBTClient() {
        for (uint32_t i = 0; i < kNumFillers; ++i)
            delete[] fillers_[i];
        for (uint32_t i = 0; i < kMaxUniquePAOs; ++i)
            to_->destroyPAO(recv_paos_[i]);
        delete[] recv_paos_;
    }

    void CBTClient::Run() {
        //  Prepare our context and socket
        zmq::context_t context (1);
        zmq::socket_t socket (context, ZMQ_REQ);

        std::cout << "Connecting to CBTServer" << std::endl;
        socket.connect ("tcp://localhost:5555");

        uint32_t number_of_paos = 100000;

        SetupGenerators();

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
            std::string results = s_recv(socket);
            std::stringstream recv;
            recv << results;
            IstreamInputStream* ii = new IstreamInputStream(&recv);
            CodedInputStream* ci = new CodedInputStream(ii);

            uint32_t rem = results.size();
            uint32_t num_PAOs = 0;
            do {
                assert(static_cast<ProtobufOperations*>(to_)->deserialize(
                            recv_paos_[num_PAOs], ci));
                rem -= (to_->getSerializedSize(recv_paos_[num_PAOs]) + 1);
               ++num_PAOs;
            } while (rem > 0);
            fprintf(stderr, "%d results received\n", num_PAOs);
        }
    }

    void CBTClient::SetupGenerators() {
        uint32_t num_full_loops_ =
            (int)floor(Conv26(log2(kNumUniqKeys)));
        uint32_t part_loop_ = (int)ceil(kNumUniqKeys /
                pow(26, num_full_loops_));
        GenerateFillers(kKeyLen - num_full_loops_ - 1);
    }

    void CBTClient::GeneratePAOs(std::vector<PartialAgg*>& paos,
            uint32_t number_of_paos) {
        switch(kInputDist) {
            case UNIFORM:
                GenerateUniformPAOs(paos, number_of_paos);
                break;
            case POWERLAW:
                GeneratePowerLawPAOs(paos, number_of_paos);
                break;
        }
    }

    void CBTClient::GenerateFillers(uint32_t filler_len) {
        for (uint32_t i = 0; i < kNumFillers; ++i) {
            char* f = new char[filler_len + 1];
            for (uint32_t j = 0; j < filler_len; ++j)
                f[j] = 97 + rand() % kLettersInAlphabet;
            f[filler_len] = '\0';
            fillers_.push_back(f);
        }
    }

    void CBTClient::GenerateUniformPAOs(
            std::vector<PartialAgg*>& paos, uint32_t number_of_paos) {

        uint32_t num_full_loops_ =
                (int)floor(Conv26(log2(kNumUniqKeys)));
        uint32_t part_loop_ = (int)ceil(kNumUniqKeys /
                pow(26, num_full_loops_));

        char* word = new char[kKeyLen + 1];
        assert(number_of_paos < kMaxPAOs);
        Token t;
        for (uint32_t i = 0; i < number_of_paos; ++i) {
            for (uint32_t j=0; j < num_full_loops_; j++)
                word[j] = 97 + rand() % kLettersInAlphabet;
            word[num_full_loops_] = 97 + rand() % part_loop_;
            word[num_full_loops_ + 1] = '\0';

            uint32_t filler_number = HashUtil::MurmurHash(word,
                    strlen(word), 42) % kNumFillers;
//            fprintf(stderr, "%d, %s\n", filler_number, fillers_[filler_number]);

            strncat(word, fillers_[filler_number], kKeyLen -
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

    void CBTClient::GeneratePowerLawPAOs(std::vector<PartialAgg*>& paos,
                uint32_t number_of_paos) {
        ZipfGenerator zg(20, 26);

        uint32_t num_full_loops_ =
                (int)floor(Conv26(log2(kNumUniqKeys)));
        uint32_t part_loop_ = (int)ceil(kNumUniqKeys /
                pow(26, num_full_loops_));

        char* word = new char[kKeyLen + 1];
        assert(number_of_paos < kMaxPAOs);
        Token t;
        for (uint32_t i = 0; i < number_of_paos; ++i) {
            for (uint32_t j=0; j < num_full_loops_; j++)
                word[j] = 97 + zg();
            word[num_full_loops_] = 97 + zg() % part_loop_;
            word[num_full_loops_ + 1] = '\0';

            uint32_t filler_number = HashUtil::MurmurHash(word,
                    strlen(word), 42) % kNumFillers;
//            fprintf(stderr, "%d, %s\n", filler_number, fillers_[filler_number]);

            strncat(word, fillers_[filler_number], kKeyLen -
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
        std::string soname(SX(SRV_PATH)"/wc_proto.so");
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
            assert(static_cast<ProtobufOperations*>(to_)->serialize(
                    static_cast<PartialAgg*>(*it), cs));
        }
    }

    void CBTClient::DeletePAOs(const std::vector<PartialAgg*>& paos) {
        std::vector<PartialAgg*>::const_iterator it = paos.begin();
        for ( ; it != paos.end(); ++it) {
           to_->destroyPAO(*it);
        }
    }
} // cbtservice



int main (int argc, char* argv[])
{
    // Define gflags options
    google::ParseCommandLineFlags(&argc, &argv, true);
    string usage("Usage:\n");
    usage += argv[0] + std::string(" --unique <u> --length <l>");
    google::SetUsageMessage(usage);

    if (!FLAGS_unique || !FLAGS_length) {
        google::ShowUsageWithFlags(argv[0]);
        exit(EXIT_FAILURE);
    }

    uint32_t uniq = FLAGS_unique;
    uint32_t len = FLAGS_length;
    cbtservice::InputDistribution dist = cbtservice::UNIFORM;

    if (FLAGS_powerlaw) {
        dist = cbtservice::POWERLAW;
    }

    cbtservice::CBTClient* client = new cbtservice::CBTClient(uniq, len, dist);
    client->Run();
    return 0;
}
