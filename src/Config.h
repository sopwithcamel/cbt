#ifndef CTCONFIG_H
#define CTCONFIG_H

//#define ENABLE_ASSERT_CHECKS
//#define CT_NODE_DEBUG
//#define ENABLE_INTEGRITY_CHECK
//#define ENABLE_COUNTERS
//#define ENABLE_PAGING
#define ENABLE_COMPRESSION
//#define PIPELINED_IMPL

#ifdef ENABLE_COMPRESSION
//  #define ENABLE_SPECIALIZED_COMPRESSION
#endif  // ENABLE_COMPRESSION

#ifndef PIPELINED_IMPL
/* This option structures each buffer as consisting of multiple fragments. A
 * new fragment is created from each spill of the parent. The advantage of this
 * method is that previously-created fragments stay compressed in memory until
 * the buffer fills up (when all fragments are decompressed */
#define STRUCTURED_BUFFER
#endif  // PIPELINED_IMPL

#endif // CTCONFIG_H
