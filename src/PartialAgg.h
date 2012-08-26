#ifndef PARTIALAGG_H
#define PARTIALAGG_H
#include <string>
#include <sstream>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define REGISTER(x)\
        extern "C" Operations* __libminni_create_ops()\
        {\
            return new x();\
        }


class Token;

class PartialAgg {
  protected:
    /* don't allow PartialAgg objects to be created */
	PartialAgg() {}
    ~PartialAgg() {}
};

class Operations {
  public:
    static uint64_t createCtr;
    static uint64_t destCtr;
  public:
    enum SerializationMethod {
        PROTOBUF,
        BOOST,
        HAND
    };
    virtual ~Operations() = 0;
    virtual const char* getKey(PartialAgg* p) const = 0;
    virtual bool sameKey(PartialAgg* p1, PartialAgg* p2) const = 0;
    virtual size_t createPAO(Token* t, PartialAgg** p_list) const = 0;
    virtual bool destroyPAO(PartialAgg* p) const = 0;
	virtual bool merge(PartialAgg* v, PartialAgg* merge) const = 0;
    virtual SerializationMethod getSerializationMethod() const = 0;
    virtual uint32_t getSerializedSize(PartialAgg* p) const = 0;
	/* serialize into string/buffer */
	virtual bool serialize(PartialAgg* p,
            std::string* output) const = 0;
	virtual bool serialize(PartialAgg* p,
            char* output, size_t size) const = 0;
	/* deserialize from string/buffer */
	virtual bool deserialize(PartialAgg* p,
            const std::string& input) const = 0;
	virtual bool deserialize(PartialAgg* p,
            const char* input, size_t size) const = 0;
};

#endif
