cbt
===

# Dependencies #
## For CBT ##
* [Google protobufs](http://code.google.com/p/protobuf/)
* [Google test](http://code.google.com/p/googletest/)
* [Snappy](http://code.google.com/p/snappy/)
* [jemalloc](http://www.canonware.com/jemalloc/)
* [Google perftools](http://code.google.com/p/gperftools/wiki/GooglePerformanceTools)
* Pthreads

## Additional for server ##
* [ZeroMQ](http://www.zeromq.org/intro:get-the-software)
* [libdl](http://www.s-gms.ms.edus.si/cgi-bin/man-cgi?libdl+3LIB)
* [tcmalloc](http://code.google.com/p/gperftools/)
* [Google flags](http://code.google.com/p/gflags/)

# Installation on Ubuntu #
## For CBT ##
	sudo apt-get install scons cppcheck libgtest-dev libprotobuf-dev libsnappy-dev libjemalloc-dev libgoogle-perftools-dev
## For server ##
    sudo apt-get install libzmq-dev
## For liting ##
1. git clone git://cayenne.cc.gt.atl.ga.us/cbt.git
2. cd cbt
3. git checkout liting
4. remove #include <gperftools/heap-profiler.h> in Server.cpp
5. copy wc_proto.so into the service directory
6. run: scons -c service; scons service
