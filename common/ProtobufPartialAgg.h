#ifndef PROTOBUF_PARTIALAGG_H
#define PROTOBUF_PARTIALAGG_H
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include "PartialAgg.h"

class Token;
class ProtobufOperations : public Operations {
  public:
	ProtobufOperations() {}
	virtual ~ProtobufOperations() {}

    SerializationMethod getSerializationMethod() const { return PROTOBUF; }
    virtual void* getValue(PartialAgg* p) const {
        return NULL;
    }
    virtual void setValue(PartialAgg* p, void* v) const {
    }

	/* serialize into file */
	virtual bool serialize(PartialAgg* p,
            google::protobuf::io::CodedOutputStream* output) const = 0;
	/* deserialize from file */
	virtual bool deserialize(PartialAgg* p,
            google::protobuf::io::CodedInputStream* input) const = 0;
};

#endif
