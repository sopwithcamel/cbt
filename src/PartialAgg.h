#ifndef PARTIALAGG_H
#define PARTIALAGG_H
#include <string>
#include <sstream>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define REGISTER_PAO(x) extern "C"\
	size_t __libminni_pao_create(Token* tok, PartialAgg** p_list)\
	{\
		return x::create(tok, p_list);\
	}\
	extern "C" void __libminni_pao_destroy(x* pao)\
	{\
		delete pao;\
	}

class Token;
class PartialAgg {
  public:
    enum SerializationMethod {
        PROTOBUF,
        BOOST,
        HAND
    };
    static uint64_t createCtr;
    static uint64_t destCtr;
  public:
	PartialAgg() {}
	virtual ~PartialAgg() {}
    /* Get key */
    virtual const std::string& key() const = 0;
	virtual void merge(PartialAgg* add_agg) = 0;
	static size_t create(Token*& t, PartialAgg** p_list) { return 0; }
    virtual SerializationMethod getSerializationMethod() const = 0;
};

#endif
