/********************************************************************
 * Copyright (c) 2013 - 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef _HDFS_LIBHDFS3_CLIENT_HDFS_H_
#define _HDFS_LIBHDFS3_CLIENT_HDFS_H_

#include <errno.h> /* for EINTERNAL, etc. */
#include <fcntl.h> /* for O_RDONLY, O_WRONLY */
#include <stdint.h> /* for uint64_t, etc. */
#include <time.h> /* for time_t */
#ifndef O_RDONLY
#define O_RDONLY 1
#endif

#ifndef O_WRONLY
#define O_WRONLY 2
#endif

#ifndef INVALID_FILE_ID
#define INVALID_FILE_ID  (0)
#endif

#ifndef EINTERNAL
#define EINTERNAL 255
#endif
#define NOERROR 0
/** All APIs set errno to meaningful values */

#ifdef __cplusplus
extern "C" {
#endif
/**
 * Some utility decls used in libhdfs.
 */
typedef int32_t tSize; /// size of data for read/write io ops
typedef int64_t tTime; /// time type in seconds
typedef int64_t tOffset; /// offset within the file
typedef uint16_t tPort; /// port

typedef enum tObjectKind {
    kObjectKindFile = 'F', kObjectKindDirectory = 'D',
} tObjectKind;

struct StreamFileSystemInternalWrapper;
typedef struct StreamFileSystemInternalWrapper * streamFS;

struct StreamFileInternalWrapper;
typedef struct StreamFileInternalWrapper * streamFile;

struct streamBuilder;
struct StreamStatistics;

/***************************************************************
*                                      open flag define
*    posix defined open flag below :
*    8 / 40  / 100 / 400/ 800 / 8000 / 10004 / 20000 / 40000 / 80000 / 100000
*    200000 / 1000 / 80 / 100000
*    可见高位字节未占用，此处使用高位的字节进行划分
*
*  高位字节分配:
*  0             1          2           3           4        5        6        7
*  ----------      ------       -------     ----------------------------------
*  io priority            isWal       isCache              reserved
*
***************************************************************/
#define STREAM_IO_PRIORITY_ULTRA_HIGH  0x00000000
#define STREAM_IO_PRIORITY_HIGH        0x01000000
#define STREAM_IO_PRIORITY_NORMAL      0x02000000
#define STREAM_IO_PRIORITY_LOW         0x03000000

#define STREAM_WAL_FILE_FLAG           0x04000000
#define STREAM_NEED_CACHE_FLAG         0x08000000

int32_t inline getIoPriority(int32_t flag)
{
    return ((flag >> 24) & 0x3);
}

bool inline getIsWal(int32_t flag)
{
    return (bool)((flag >> 26) & 1);
}

bool inline getIsCache(int32_t flag)
{
    return (bool)((flag >> 27) & 1);
}

int transRateLimiterPriToPlogPri(int32_t ioPriority);

/**
 * Return error information of last failed operation.
 *
 * @return 			A not NULL const string point of last error information.
 * 					Caller can only read this message and keep it unchanged. No need to free it.
 * 					If last operation finished successfully, the returned message is undefined.
 */
const char * streamGetLastError();
/**
 * init plog client for next use, because it's can only be initial once within a process.
 *
 * @return 			0:init ok.
                                  1:init failed
 */
int streamInit();



/**
 * Return stream counted statistics.
 *
 *@param doClear        1: clear the statistics to zero and return; others input int or NULL: return current streamStatistics.
 * @return 			A Struct which contains all monitor statistics.
 *
 */
//StreamStatistics * streamStatus(int doClear);


/**
 * Determine if a file is open for read.
 *
 * @param file     The HDFS file
 * @return         1 if the file is open for read; 0 otherwise
 */
int streamFileIsOpenForRead(streamFile file);

/**
 * Determine if a file is open for write.
 *
 * @param file     The HDFS file
 * @return         1 if the file is open for write; 0 otherwise
 */
int streamFileIsOpenForWrite(streamFile file);

/**
 * streamConnectAsUser - Connect to a hdfs file system as a specific user
 * Connect to the hdfs.
 * @param nn   The NameNode.  See streamBuilderSetNameNode for details.
 * @param port The port on which the server is listening.
 * @param user the user name (this is hadoop domain user). Or NULL is equivelant to hhdfsConnect(host, port)
 * @return Returns a handle to the filesystem or NULL on error.
 * @deprecated Use streamBuilderConnect instead.
 */
streamFS streamConnectAsUser(const char * nn, tPort port, const char * user);

/**
 * streamConnect - Connect to a hdfs file system.
 * Connect to the hdfs.
 * @param nn   The NameNode.  See streamBuilderSetNameNode for details.
 * @param port The port on which the server is listening.
 * @return Returns a handle to the filesystem or NULL on error.
 * @deprecated Use streamBuilderConnect instead.
 */
streamFS streamConnect(const char * nn, tPort port);

/**
 * streamConnect - Connect to an hdfs file system.
 *
 * Forces a new instance to be created
 *
 * @param nn     The NameNode.  See streamBuilderSetNameNode for details.
 * @param port   The port on which the server is listening.
 * @param user   The user name to use when connecting
 * @return       Returns a handle to the filesystem or NULL on error.
 * @deprecated   Use streamBuilderConnect instead.
 */
streamFS streamConnectAsUserNewInstance(const char * nn, tPort port,
                                    const char * user);

/**
 * streamConnect - Connect to an hdfs file system.
 *
 * Forces a new instance to be created
 *
 * @param nn     The NameNode.  See streamBuilderSetNameNode for details.
 * @param port   The port on which the server is listening.
 * @return       Returns a handle to the filesystem or NULL on error.
 * @deprecated   Use streamBuilderConnect instead.
 */
streamFS streamConnectNewInstance(const char * nn, tPort port);

/**
 * Connect to HDFS using the parameters defined by the builder.
 *
 * The HDFS builder will be freed, whether or not the connection was
 * successful.
 *
 * Every successful call to streamBuilderConnect should be matched with a call
 * to streamDisconnect, when the streamFS is no longer needed.
 *
 * @param bld    The HDFS builder
 * @return       Returns a handle to the filesystem, or NULL on error.
 */
streamFS streamBuilderConnect(struct streamBuilder * bld);

/**
 * Create an HDFS builder.
 *
 * @return The HDFS builder, or NULL on error.
 */
struct streamBuilder * streamNewBuilder(void);

/**
 * Do nothing, we always create a new instance
 *
 * @param bld The HDFS builder
 */
void streamBuilderSetForceNewInstance(struct streamBuilder * bld);

/**
 * Set the HDFS NameNode to connect to.
 *
 * @param bld  The HDFS builder
 * @param nn   The NameNode to use.
 *
 *             If the string given is 'default', the default NameNode
 *             configuration will be used (from the XML configuration files)
 *
 *             If NULL is given, a LocalFileSystem will be created.
 *
 *             If the string starts with a protocol type such as file:// or
 *             hdfs://, this protocol type will be used.  If not, the
 *             hdfs:// protocol type will be used.
 *
 *             You may specify a NameNode port in the usual way by
 *             passing a string of the format hdfs://<hostname>:<port>.
 *             Alternately, you may set the port with
 *             streamBuilderSetNameNodePort.  However, you must not pass the
 *             port in two different ways.
 */
void streamBuilderSetNameNode(struct streamBuilder * bld, const char * nn);

/**
 * Set the port of the HDFS NameNode to connect to.
 *
 * @param bld The HDFS builder
 * @param port The port.
 */
void streamBuilderSetNameNodePort(struct streamBuilder * bld, tPort port);

/**
 * Set the username to use when connecting to the HDFS cluster.
 *
 * @param bld The HDFS builder
 * @param userName The user name.  The string will be shallow-copied.
 */
void streamBuilderSetUserName(struct streamBuilder * bld, const char * userName);

/**
 * Set the path to the Kerberos ticket cache to use when connecting to
 * the HDFS cluster.
 *
 * @param bld The HDFS builder
 * @param kerbTicketCachePath The Kerberos ticket cache path.  The string
 *                            will be shallow-copied.
 */
void streamBuilderSetKerbTicketCachePath(struct streamBuilder * bld,
                                       const char * kerbTicketCachePath);

/**
 * Set the token used to authenticate
 *
 * @param bld The HDFS builder
 * @param token The token used to authenticate
 */
void streamBuilderSetToken(struct streamBuilder * bld, const char * token);

/**
 * Free an HDFS builder.
 *
 * It is normally not necessary to call this function since
 * streamBuilderConnect frees the builder.
 *
 * @param bld The HDFS builder
 */
void streamFreeBuilder(struct streamBuilder * bld);

/**
 * Set a configuration string for an streamBuilder.
 *
 * @param key      The key to set.
 * @param val      The value, or NULL to set no value.
 *                 This will be shallow-copied.  You are responsible for
 *                 ensuring that it remains valid until the builder is
 *                 freed.
 *
 * @return         0 on success; nonzero error code otherwise.
 */
int streamBuilderConfSetStr(struct streamBuilder * bld, const char * key,
                          const char * val);

/**
 * update configuration
 * 
 * @return true if key and value is valid
 * */
bool streamConfUpdate(const char *key, const char *value);

/**
 * Get a configuration string.
 *
 * @param key      The key to find
 * @param val      (out param) The value.  This will be set to NULL if the
 *                 key isn't found.  You must free this string with
 *                 streamConfStrFree.
 *
 * @return         0 on success; nonzero error code otherwise.
 *                 Failure to find the key is not an error.
 */
int streamConfGetStr(const char * key, char ** val);

/**
 * Get a configuration integer.
 *
 * @param key      The key to find
 * @param val      (out param) The value.  This will NOT be changed if the
 *                 key isn't found.
 *
 * @return         0 on success; nonzero error code otherwise.
 *                 Failure to find the key is not an error.
 */
int streamConfGetInt(const char * key, int32_t * val);

/**
 * Get a configuration integer.
 *
 * @param key      The key to find
 * @param val      (out param) The value.  This will NOT be changed if the
 *                 key isn't found.
 *
 * @return         0 on success; nonzero error code otherwise.
 *                 Failure to find the key is not an error.
 */
int streamConfGetInt64(const char * key, int64_t * val);


/**
 * Free a configuration string found with streamConfGetStr.
 *
 * @param val      A configuration string obtained from streamConfGetStr
 */
void streamConfStrFree(char * val);

/**
 * streamDisconnect - Disconnect from the hdfs file system.
 * Disconnect from hdfs.
 * @param fs The configured filesystem handle.
 * @return Returns 0 on success, -1 on error.
 *         Even if there is an error, the resources associated with the
 *         streamFS will be freed.
 */
int streamDisconnect(streamFS fs);

/**
 * streamOpenFile - Open a hdfs file in given mode.
 * @param fs The configured filesystem handle.
 * @param path The full path to the file.
 * @param flags - an | of bits/fcntl.h file flags - supported flags are O_RDONLY, O_WRONLY (meaning create or overwrite i.e., implies O_TRUNCAT),
 * O_WRONLY|O_APPEND and O_SYNC. Other flags are generally ignored other than (O_RDWR || (O_EXCL & O_CREAT)) which return NULL and set errno equal ENOTSUP.
 * @param bufferSize Size of buffer for read/write - pass 0 if you want
 * to use the default configured values.
 * @param replication Block replication - pass 0 if you want to use
 * the default configured values.
 * @param blocksize Size of block - pass 0 if you want to use the
 * default configured values.
 * @param lockPath the path of lock which should hold during open a file, default is "".
 * @return Returns the handle to the open file or NULL on error.
 */
streamFile streamOpenFile(streamFS fs, const char * path, int flags, int bufferSize, uint16_t* mode, bool createParent, short replication, tOffset blocksize);
streamFile streamOpenFileV2(streamFS fs, const char * path, int flags, int bufferSize, uint16_t* mode, bool createParent, short replication, tOffset blocksize, const char * lockPath);
streamFile streamOpenFileV3(streamFS fs, const char * path, int flags, int bufferSize, uint16_t* mode, bool createParent, short replication, tOffset blocksize, const char * lockPath, const char * storagePolicyName);

/**
 * streamCloseFile - Close an open file.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Returns 0 on success, -1 on error.
 *         On error, errno will be set appropriately.
 *         If the hdfs file was valid, the memory associated with it will
 *         be freed at the end of this call, even if there was an I/O
 *         error.
 */
int streamCloseFile(streamFS fs, streamFile file);
int streamCloseFileV2(streamFS fs, streamFile file, const char * lockPath);

/**
 * streamExists - Checks if a given path exsits on the filesystem
 * @param fs The configured filesystem handle.
 * @param path The path to look for
 * @return Returns 0 on success, -1 on error.
 */
int streamExists(streamFS fs, const char * path);

/**
 * streamSeek - Seek to given offset in file.
 * This works only for files opened in read-only mode.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param desiredPos Offset into the file to seek into.
 * @return Returns 0 on success, -1 on error.
 */
int streamSeek(streamFS fs, streamFile file, tOffset desiredPos);

/**
 * streamTell - Get the current offset in the file, in bytes.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Current offset, -1 on error.
 */
tOffset streamTell(streamFS fs, streamFile file);

/**
 * streamRead - Read data from an open file.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param buffer The buffer to copy read bytes into.
 * @param length The length of the buffer.
 * @return      On success, a positive number indicating how many bytes
 *              were read.
 *              On end-of-file, 0.
 *              On error, -1.  Errno will be set to the error code.
 *              Just like the POSIX read function, streamRead will return -1
 *              and set errno to EINTR if data is temporarily unavailable,
 *              but we are not yet at the end of the file.
 */
tSize streamRead(streamFS fs, streamFile file, void * buffer, tSize length);

/**
 * streamRead - Read data from an open file dedicated offset, it's atomic op, same as posix pread.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param buffer The buffer to copy read bytes into.
 * @param off The offset of read start pos.
 * @param length The length of the buffer.
 * @return      On success, a positive number indicating how many bytes
 *              were read.
 *              On end-of-file, 0.
 *              On error, -1.  Errno will be set to the error code.
 *              Just like the POSIX read function, streamRead will return -1
 *              and set errno to EINTR if data is temporarily unavailable,
 *              but we are not yet at the end of the file.
 */
tSize streamPread(streamFS fs, streamFile file, void * buffer, tOffset off, tSize length);

/**
 * streamWrite - Write data into an open file.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param buffer The data.
 * @param length The no. of bytes to write.
 * @return Returns the number of bytes written, -1 on error.
 */
tSize streamWrite(streamFS fs, streamFile file, const void * buffer, tSize length);

/**
 * streamWrite - Flush the data.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Returns 0 on success, -1 on error.
 */
int streamFlush(streamFS fs, streamFile file);

/**
 * streamHFlush - Flush out the data in client's user buffer. After the
 * return of this call, new readers will see the data.
 * @param fs configured filesystem handle
 * @param file file handle
 * @return 0 on success, -1 on error and sets errno
 */
int streamHFlush(streamFS fs, streamFile file);

/**
 * streamSync - Flush out and sync the data in client's user buffer. After the
 * return of this call, new readers will see the data.
 * @param fs configured filesystem handle
 * @param file file handle
 * @return 0 on success, -1 on error and sets errno
 */
int streamSync(streamFS fs, streamFile file, bool updateLength);
int streamSyncV2(streamFS fs, streamFile file, bool updateLength, const char * lockPath);

/**
 * streamAvailable - Number of bytes that can be read from this
 * input stream without blocking.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Returns available bytes; -1 on error.
 */
int streamAvailable(streamFS fs, streamFile file);

/**
 * streamCopy - Copy file from one filesystem to another.
 * @param srcFS The handle to source filesystem.
 * @param src The path of source file.
 * @param dstFS The handle to destination filesystem.
 * @param dst The path of destination file.
 * @return Returns 0 on success, -1 on error.
 */
int streamCopy(streamFS srcFS, const char * src, streamFS dstFS, const char * dst);

/**
 * streamCopyDfvObs - Copy file from obs/dfv to dfv/obs.
 * @param fs The handle to source filesystem.
 * @param src The path of source file.
 * @param dst The path of destination file.
 * @param dstStoragePolicyName  Mark the destination storage type.
 * @param overwriteDest Whether to overwrite the target file.
 * @return Returns 0 on success, -1 on error.
 */
int streamCopyDfvObs(streamFS fs, const char *src, const char *dst, const char * dstStoragePolicyName, const char * lockPath, bool overwriteDest);
/**
 * streamMove - Move file from one filesystem to another.
 * @param srcFS The handle to source filesystem.
 * @param src The path of source file.
 * @param dstFS The handle to destination filesystem.
 * @param dst The path of destination file.
 * @return Returns 0 on success, -1 on error.
 */
int streamMove(streamFS srcFS, const char * src, streamFS dstFS, const char * dst);

/**
 * streamDelete - Delete file.
 * @param fs The configured filesystem handle.
 * @param path The path of the file.
 * @param recursive if path is a directory and set to
 * non-zero, the directory is deleted else throws an exception. In
 * case of a file the recursive argument is irrelevant.
 * @param lockPath the path of lock which should hold during change fs metadata, default is "".
 * @return Returns 0 on success, -1 on error.
 */
int streamDelete(streamFS fs, const char * path, int recursive);
int streamDeleteV2(streamFS fs, const char * path, int recursive, const char * lockPath);

/**
 * streamSetQuota - set quota.
 * @param fs The configured filesystem handle.
 * @param path
 * @param namespaceQuota
 * @param storageSpaceQuota
 *  (1) 0 or more will set the quota to that value,
 *  (2) QUOTA_DONT_SET implies the quota will not be changed
 *  (3) QUOTA_RESET implies the quota will be reset
 * @return Returns 0 on success, -1 on error.
 */
int streamSetQuota(streamFS fs, const char* path, uint64_t namespaceQuota, uint64_t storageSpaceQuota);
int streamSetQuotaV2(streamFS fs, const char* path, uint64_t namespaceQuota, uint64_t storageSpaceQuota, const char * lockPath);

/**
 * streamAllowSnapshot - allow create snapshot in path.
 * @param fs The configured filesystem handle.
 * @param path
 * @return Returns 0 on success, -1 on error.
 */
int streamAllowSnapshot(streamFS fs, const char* path);
int streamAllowSnapshotV2(streamFS fs, const char* path, const char * lockPath);

/**
 * streamDisallowSnapshot - disallow create snapshot in path.
 * @param fs The configured filesystem handle.
 * @param path
 * @return Returns 0 on success, -1 on error.
 */
int streamDisallowSnapshot(streamFS fs, const char* path);
int streamDisallowSnapshotV2(streamFS fs, const char* path, const char * lockPath);

/**
 * streamCreateSnapshot - create snapshot with name.
 * @param fs The configured filesystem handle.
 * @param path
 * @param snapshotName
 * @param storageSpaceQuota
 * @return Returns 0 on success, -1 on error.
 */
char* streamCreateSnapshot(streamFS fs, const char* path, const char* snapshotName);
char* streamCreateSnapshotV2(streamFS fs, const char* path, const char* snapshotName, const char * lockPath);

/**
 * streamFreeSnapshotPath - release snapshot path resource.
 * @param path The return value of createsnapshot.
 * @return none.
 */
void streamFreeSnapshotPath(char* path);

/**
 * streamDeleteSnapshot - set quota.
 * @param fs The configured filesystem handle.
 * @param path
 * @param snapshotName
 * @param storageSpaceQuota
 * @return Returns 0 on success, -1 on error.
 */
int streamDeleteSnapshot(streamFS fs, const char* path, const char* snapshotName);
int streamDeleteSnapshotV2(streamFS fs, const char* path, const char* snapshotName, const char * lockPath);

/**
 * streamRename - Rename file.
 * @param fs The configured filesystem handle.
 * @param oldPath The path of the source file.
 * @param newPath The path of the destination file.
 * @param lockPath the path of lock which should hold during change fs metadata, default is "".
 * @return Returns 0 on success, -1 on error.
 */
int streamRename(streamFS fs, const char * oldPath, const char * newPath, bool overwriteDest);
int streamRenameV2(streamFS fs, const char * oldPath, const char * newPath, bool overwriteDest, const char * lockPath);

/**
 * streamSealFile - force seal a file and remove it's lease.
 * @param pah The path of the source file.
 * @param lockPath the path of lock which should hold during seal file, default is "".
 * @return Returns 0 on success, -1 on error.
 */
int streamSealFile(streamFS fs, const char *path);
int streamSealFileV2(streamFS fs, const char *path, const char * lockPath);

/**
 * streamGetWorkingDirectory - Get the current working directory for
 * the given filesystem.
 * @param fs The configured filesystem handle.
 * @param buffer The user-buffer to copy path of cwd into.
 * @param bufferSize The length of user-buffer.
 * @return Returns buffer, NULL on error.
 */
char * streamGetWorkingDirectory(streamFS fs, char * buffer, size_t bufferSize);

/**
 * streamSetWorkingDirectory - Set the working directory. All relative
 * paths will be resolved relative to it.
 * @param fs The configured filesystem handle.
 * @param path The path of the new 'cwd'.
 * @return Returns 0 on success, -1 on error.
 */
int streamSetWorkingDirectory(streamFS fs, const char * path);

/**
 * streamCreateDirectory - Make the given file and all non-existent
 * parents into directories.
 * @param fs The configured filesystem handle.
 * @param path The path of the directory.
 * @param lockPath the path of lock which should hold during change fs metadata, default is "".
 * @return Returns 0 on success, -1 on error.
 */
int streamCreateDirectory(streamFS fs, const char * path);
int streamCreateDirectoryV2(streamFS fs, const char * path, const char * lockPath);

/**
 * streamFileInfo - Information about a file/directory.
 */
typedef struct {
    uint64_t mFileId;
    tObjectKind mKind; /* file or directory */
    char * mName; /* the name of the file */
    tTime mLastMod; /* the last modification time for the file in milliseconds */
    tOffset mSize; /* the size of the file in bytes */
    short mReplication; /* the count of replicas */
    tOffset mBlockSize; /* the block size for the file */
    char * mOwner; /* the owner of the file */
    char * mGroup; /* the group associated with the file */
    short mPermissions; /* the permissions associated with the file */
    tTime mLastAccess; /* the last access time for the file in milliseconds */
} streamFileInfo;


/**
 *  * streamStatInfo - Information about a file/directory.
 *   */
typedef struct {
    uint64_t streamOpenCount;
    uint64_t streamOpenCostAvgTime;
    uint64_t streamOpenCostTime;
    uint64_t streamCloseCount;
    uint64_t streamCloseCostAvgTime;
    uint64_t streamCloseCostTime;
    uint64_t streamExistCount;
    uint64_t streamExistCostAvgTime;
    uint64_t streamExistCostTime;
    uint64_t streamSeekCount;
    uint64_t streamSeekCostAvgTime;
    uint64_t streamSeekCostTime;
    uint64_t streamPreadCount;
    uint64_t streamPreadCostAvgTime;
    uint64_t streamPreadCostTime;
    uint64_t streamReadCount;
    uint64_t streamReadCostAvgTime;
    uint64_t streamReadCostTime;
    uint64_t streamWriteCount;
    uint64_t streamWriteCostAvgTime;
    uint64_t streamWriteCostTime;
    uint64_t streamFlushCount;
    uint64_t streamFlushCostAvgTime;
    uint64_t streamFlushCostTime;
    uint64_t streamSyncCount;
    uint64_t streamSyncCostAvgTime;
    uint64_t streamSyncCostTime;
    uint64_t streamAvailableCount;
    uint64_t streamAvailableCostAvgTime;
    uint64_t streamAvailableCostTime;
    uint64_t streamDeleteCount;
    uint64_t streamDeleteCostAvgTime;
    uint64_t streamDeleteCostTime;
    uint64_t streamRenameCount;
    uint64_t streamRenameCostAvgTime;
    uint64_t streamRenameCostTime;
    uint64_t streamCreateSnapshotCount;
    uint64_t streamCreateSnapshotCostAvgTime;
    uint64_t streamCreateSnapshotCostTime;
    uint64_t streamDeleteSnapshotCount;
    uint64_t streamDeleteSnapshotCostAvgTime;
    uint64_t streamDeleteSnapshotCostTime;
    uint64_t streamCreateDirectoryCount;
    uint64_t streamCreateDirectoryCostAvgTime;
    uint64_t streamCreateDirectoryCostTime;
    uint64_t streamListDirectoryCount;
    uint64_t streamListDirectoryCostAvgTime;
    uint64_t streamListDirectoryCostTime;
    uint64_t streamGetContentSummaryCount;
    uint64_t streamGetContentSummaryCostAvgTime;
    uint64_t streamGetContentSummaryCostTime;
    uint64_t plogReadSuccessCount;
    uint64_t plogReadFailedCount;
    uint64_t plogReadCostAvgTime;
    uint64_t plogReadCostTime;
    uint64_t plogGetSuccessCount;
    uint64_t plogGetFailedCount;
    uint64_t plogGetCostAvgTime;
    uint64_t plogGetCostTime;
    uint64_t plogSealSuccessCount;
    uint64_t plogSealFailedCount;
    uint64_t plogSealCostAvgTime;
    uint64_t plogSealCostTime;
    uint64_t plogAppendSuccessCount;
    uint64_t plogAppendFailedCount;
    uint64_t plogAppendCostAvgTime;
    uint64_t plogAppendCostTime;
    uint64_t plogDeleteSuccessCount;
    uint64_t plogDeleteFailedCount;
    uint64_t plogDeleteCostAvgTime;
    uint64_t plogDeleteCostTime;
    uint64_t streamReadSizeSum;
    uint64_t streamPreadSizeSum;
    uint64_t streamWriteSizeSum;
    uint64_t plogRealtimeBandwidth;
    uint64_t plogRealtimeIOPS;
    uint64_t plogBandwidthMax;
    uint64_t plogIOPSMax;
    uint64_t plogIOPSWaitThreads;
    uint64_t plogBandwidthWaitThreads;
    uint64_t plogIOPSWaitThreadsTime;
    uint64_t plogIOPSWaitThreadsTotal;
    uint64_t plogBandwidthWaitThreadsTime;
    uint64_t plogBandwidthWaitThreadsTotal;
} streamStatInfo;

streamStatInfo streamGetStatInfo(int doClear);

// the max histogram type in stream statistics.
extern const uint32_t STREAM_HISTOGRAM_TYPE_MAX;

extern const uint32_t STREAM_TICKER_TYPE_MAX;

typedef struct st_stream_histogram {
  const char *name;             // histogram name
  uint32_t type;                // histogram enum type
  uint64_t count;               // execution times
  uint64_t sum;                 // the sum of execution micros
  uint64_t max;                 // max of execution micros
  double avg;                   // average of execution micros
  double percentile99;          // 99 percent of execution micros
} stream_histogram_t;

typedef struct st_stream_ticker {
    const char *name;
    uint64_t sum;
} stream_ticker_t;



/**
 * Fill the given histograms array by iterate the statistics in Histograms enum order as much as possible
 * @param histograms the output histogram array. It should be malloc and free by caller.
 * @param size the size of the given histogram array.
 * @param exclude_zero whether exclude empty histogram
 * @return the number of histogram generated
 */
uint32_t stream_get_hist(stream_histogram_t *histograms, uint32_t size, bool exclude_zero);

uint32_t stream_get_ticker(stream_ticker_t *ticker_sum, uint32_t size);

uint32_t stream_get_ticker_by_type(stream_ticker_t *ticker_sum, uint32_t ticker_type);

uint32_t stream_get_hist_by_type(stream_histogram_t *histograms, uint32_t hist_type, bool exclude_zero);

void stream_reset_hist();

/**
 * streamListDirectory - Get list of files/directories for a given
 * directory-path. streamFreeFileInfo should be called to deallocate memory.
 * @param fs The configured filesystem handle.
 * @param path The path of the directory.
 * @param numEntries Set to the number of files/directories in path.
 * @accurateLength get accurate length
 * @type, 0:all(dfv,obs), 1:dfv, 2:obs
 * @return Returns a dynamically-allocated array of streamFileInfo
 * objects; NULL on error.
 */
streamFileInfo * streamListDirectory(streamFS fs, const char * path, int * numEntries);
streamFileInfo * streamListDirectoryV2(streamFS fs, const char * path, int * numEntries, bool accurateLength);
streamFileInfo * streamListDirectoryV3(streamFS fs, const char * path, int * numEntries, bool accurateLength, int type);
/**
 * streamGetPathInfo - Get information about a path as a (dynamically
 * allocated) single streamFileInfo struct. streamFreeFileInfo should be
 * called when the pointer is no longer needed.
 * @param fs The configured filesystem handle.
 * @param path The path of the file.
 * @return Returns a dynamically-allocated streamFileInfo object;
 * NULL on error.
 */
streamFileInfo * streamGetPathInfo(streamFS fs, const char * path);

/**
 * streamCheckObsType - check path's storage type
 * @param fs The configured filesystem handle.
 * @param path The path of the file.
 * @return 0:DFV,1:obs type,-1:error.
 */
int streamCheckObsType(streamFS fs, const char * path);
/**
 * streamFreeFileInfo - Free up the streamFileInfo array (including fields)
 * @param infos The array of dynamically-allocated streamFileInfo
 * objects.
 * @param numEntries The size of the array.
 */
void streamFreeFileInfo(streamFileInfo * infos, int numEntries);

/**
 *  * streamGetFileId - Get fileId about a file
 *  * @param fs The configured filesystem handle.
 *  * @param path The path of the file.
 *  * @return Returns fileId
 */
uint64_t streamGetFileId(streamFS fs, const char * path);

typedef struct {
    int64_t length;
    int64_t dfvLength;
    int64_t obsLength;
    int64_t fileCount;
    int64_t directoryCount;
    int64_t quota;
    int64_t spaceConsumed;
    int64_t spaceQuota;
    int numOfTypes;
    int64_t* typeConsumed;
    int64_t* typeQuota;
}streamContentSummary;

/**
 * streamFreeContentSummary - Free up the streamFileInfo array (including fields)
 * @param infos The array of dynamically-allocated streamFileInfo
 * objects.
 */
void streamFreeContentSummary(streamContentSummary * summary);

/**
 * streamGetDefaultBlockSize - Get the default blocksize.
 *
 * @param fs            The configured filesystem handle.
 * @deprecated          Use hdfsGetDefaultBlockSizeAtPath instead.
 *
 * @return              Returns the default blocksize, or -1 on error.
 */
tOffset streamGetDefaultBlockSize(streamFS fs);


/**
 * streamGetCapacity - Return the raw capacity of the filesystem.
 * @param fs The configured filesystem handle.
 * @return Returns the raw-capacity; -1 on error.
 */
tOffset streamGetCapacity(streamFS fs);

/**
 * streamGetUsed - Return the total raw size of all files in the filesystem.
 * @param fs The configured filesystem handle.
 * @return Returns the total-size; -1 on error.
 */
tOffset streamGetUsed(streamFS fs);

/**
 * Change the user and/or group of a file or directory.
 *
 * @param fs            The configured filesystem handle.
 * @param path          the path to the file or directory
 * @param owner         User string.  Set to NULL for 'no change'
 * @param group         Group string.  Set to NULL for 'no change'
 * @return              0 on success else -1
 */
int streamChown(streamFS fs, const char * path, const char * owner, const char * group);
int streamChownV2(streamFS fs, const char * path, const char * owner, const char * group, const char * lockPath);

/**
 * streamChmod
 * @param fs The configured filesystem handle.
 * @param path the path to the file or directory
 * @param mode the bitmask to set it to
 * @return 0 on success else -1
 */
int streamChmod(streamFS fs, const char * path, short mode);
int streamChmodV2(streamFS fs, const char * path, short mode, const char * lockPath);

/**
 * streamUtime
 * @param fs The configured filesystem handle.
 * @param path the path to the file or directory
 * @param mtime new modification time or -1 for no change
 * @param atime new access time or -1 for no change
 * @return 0 on success else -1
 */
int streamUtime(streamFS fs, const char * path, tTime mtime, tTime atime);
int streamUtimeV2(streamFS fs, const char * path, tTime mtime, tTime atime, const char * lockPath);

/**
 * streamTruncate - Truncate the file in the indicated path to the indicated size.
 * @param fs The configured filesystem handle.
 * @param path the path to the file.
 * @param pos the position the file will be truncated to.
 * @param shouldWait output value, true if and client does not need to wait for block recovery,
 * false if client needs to wait for block recovery.
 */
int streamTruncate(streamFS fs, const char * path, tOffset pos, int * shouldWait);
int streamTruncateV2(streamFS fs, const char * path, tOffset pos, int * shouldWait, const char * lockPath);

streamContentSummary* streamGetContentSummary(streamFS fs, const char * path);

/*
 * streamGetLeaseHolderIp
 * caller must free return value if s is not NULL
 */
char * streamGetLeaseHolderIp(const streamFS fs,const char *  path);

int64_t getSize(const streamFS fs);

/**
 * streamRecoverLease - Start the lease recovery of a file.
 * @param fs The file system
 * @param path the path to the file.
 * @return 0 if the file is already closed else -1
 */
int streamRecoverLease(streamFS fs, const char * path);
int streamRecoverLeaseV2(streamFS fs, const char * path, const char * lockPath);
int streamRecoverLeaseByClientIP(streamFS fs, const char * src, const char * holderIp);

typedef struct Namenode {
    char * rpc_addr;    // namenode rpc address and port, such as "host:9000"
    char * http_addr;   // namenode http address and port, such as "host:50070"
} Namenode;

/**
 * If hdfs is configured with HA namenode, return all namenode informations as an array.
 * Else return NULL.
 *
 * Using configure file which is given by environment parameter LIBHDFS3_CONF
 * or "hdfs-client.xml" in working directory.
 *
 * @param nameservice hdfs name service id.
 * @param size output the size of returning array.
 *
 * @return return an array of all namenode information.
 */
Namenode * streamGetHANamenodes(const char * nameservice, int * size);

/**
 * If hdfs is configured with HA namenode, return all namenode informations as an array.
 * Else return NULL.
 *
 * @param conf the path of configure file.
 * @param nameservice hdfs name service id.
 * @param size output the size of returning array.
 *
 * @return return an array of all namenode information.
 */
Namenode * streamGetHANamenodesWithConfig(const char * conf, const char * nameservice, int * size);

/**
 * Free the array returned by hdfsGetConfiguredNamenodes()
 *
 * @param the array return by hdfsGetConfiguredNamenodes()
 */
void streamFreeNamenodeInformation(Namenode * namenodes, int size);

int streamCheckLease(streamFS fs, const char * path);

int streamReloadConf(streamFS fs);

int streamEnableRateLimiter(bool enable);

uint64_t streamGetPlogAppendSuccessCount();

uint64_t streamGetPlogAppendErrorCount();

#ifdef __cplusplus
}
#endif

#endif /* _HDFS_LIBHDFS3_CLIENT_HDFS_H_ */
