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
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileWithSnapshotFeature;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_QUOTA_BY_STORAGETYPE_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.CRYPTO_XATTR_ENCRYPTION_ZONE;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.CRYPTO_XATTR_FILE_ENCRYPTION_INFO;
import static org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot.CURRENT_STATE_ID;
import static org.apache.hadoop.util.Time.now;

/**
 * Both FSDirectory and FSNamesystem manage the state of the namespace.
 * FSDirectory is a pure in-memory data structure, all of whose operations
 * happen entirely in memory. In contrast, FSNamesystem persists the operations
 * to the disk.
 * @see org.apache.hadoop.hdfs.server.namenode.FSNamesystem
 **/
@InterfaceAudience.Private
public class FSDirectory implements Closeable { // 对目录树的增删改查（内存里的，不记editlog）
  static final Logger LOG = LoggerFactory.getLogger(FSDirectory.class);
  private static INodeDirectory createRoot(FSNamesystem namesystem) {
    final INodeDirectory r = new INodeDirectory(
        INodeId.ROOT_INODE_ID,
        INodeDirectory.ROOT_NAME,
        namesystem.createFsOwnerPermissions(new FsPermission((short) 0755)),
        0L);
    r.addDirectoryWithQuotaFeature(
        new DirectoryWithQuotaFeature.Builder().
            nameSpaceQuota(DirectoryWithQuotaFeature.DEFAULT_NAMESPACE_QUOTA). // 设置命名空间quota，默认无限
            storageSpaceQuota(DirectoryWithQuotaFeature.DEFAULT_STORAGE_SPACE_QUOTA). // 设置存储空间quota，默认无限
            build()); // 给根目录添加quota特性
    r.addSnapshottableFeature(); // 给根目录添加snapshot特性
    r.setSnapshotQuota(0); // 但不允许使用
    return r;
  }

  @VisibleForTesting
  static boolean CHECK_RESERVED_FILE_NAMES = true;
  public final static String DOT_RESERVED_STRING = ".reserved";
  public final static String DOT_RESERVED_PATH_PREFIX = Path.SEPARATOR
      + DOT_RESERVED_STRING;
  public final static byte[] DOT_RESERVED = 
      DFSUtil.string2Bytes(DOT_RESERVED_STRING);
  private final static String RAW_STRING = "raw";
  private final static byte[] RAW = DFSUtil.string2Bytes(RAW_STRING);
  public final static String DOT_INODES_STRING = ".inodes";
  public final static byte[] DOT_INODES = 
      DFSUtil.string2Bytes(DOT_INODES_STRING);

  // 整个文件系统的根节点
  INodeDirectory rootDir;
  private final FSNamesystem namesystem;
  private volatile boolean skipQuotaCheck = false; //skip while consuming edits
  private final int maxComponentLength;
  private final int maxDirItems;
  private final int lsLimit;  // max list limit
  private final int contentCountLimit; // max content summary counts per run
  private final long contentSleepMicroSec;
  // 维护 inodeID -> INode 的映射关系
  private final INodeMap inodeMap; // Synchronized by dirLock
  private long yieldCount = 0; // keep track of lock yield count.

  private final int inodeXAttrsLimit; //inode xattrs max limit

  // lock to protect the directory and BlockMap
  private final ReentrantReadWriteLock dirLock;

  private final boolean isPermissionEnabled;
  /**
   * Support for ACLs is controlled by a configuration flag. If the
   * configuration flag is false, then the NameNode will reject all
   * ACL-related operations.
   */
  private final boolean aclsEnabled;
  private final boolean xattrsEnabled;
  private final int xattrMaxSize;

  // precision of access times.
  private final long accessTimePrecision;
  // whether setStoragePolicy is allowed.
  private final boolean storagePolicyEnabled;
  // whether quota by storage type is allowed
  private final boolean quotaByStorageTypeEnabled;

  private final String fsOwnerShortUserName;  // 启动NN进程的用户
  private final String supergroup; // 配置的supergroup
  private final INodeId inodeId;

  private final FSEditLog editLog;

  private INodeAttributeProvider attributeProvider; // 权限控制，默认是777权限控制方式

  public void setINodeAttributeProvider(INodeAttributeProvider provider) {
    attributeProvider = provider;
  }

  // utility methods to acquire and release read lock and write lock
  void readLock() {
    this.dirLock.readLock().lock();
  }

  void readUnlock() {
    this.dirLock.readLock().unlock();
  }

  void writeLock() {
    this.dirLock.writeLock().lock();
  }

  void writeUnlock() {
    this.dirLock.writeLock().unlock();
  }

  boolean hasWriteLock() {
    return this.dirLock.isWriteLockedByCurrentThread();
  }

  boolean hasReadLock() {
    return this.dirLock.getReadHoldCount() > 0 || hasWriteLock();
  }

  public int getReadHoldCount() {
    return this.dirLock.getReadHoldCount();
  }

  public int getWriteHoldCount() {
    return this.dirLock.getWriteHoldCount();
  }

  @VisibleForTesting
  public final EncryptionZoneManager ezManager;

  /**
   * Caches frequently used file names used in {@link INode} to reuse 
   * byte[] objects and reduce heap usage.
   */
  private final NameCache<ByteArray> nameCache;

  FSDirectory(FSNamesystem ns, Configuration conf) throws IOException {
    this.dirLock = new ReentrantReadWriteLock(true); // fair
    this.inodeId = new INodeId();
    rootDir = createRoot(ns);
    inodeMap = INodeMap.newInstance(rootDir);
    this.isPermissionEnabled = conf.getBoolean(
      DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY,
      DFSConfigKeys.DFS_PERMISSIONS_ENABLED_DEFAULT);
    this.fsOwnerShortUserName =
      UserGroupInformation.getCurrentUser().getShortUserName(); // 启动NN进程的用户
    this.supergroup = conf.get(
      DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
      DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
    this.aclsEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY,
        DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_DEFAULT);
    LOG.info("ACLs enabled? " + aclsEnabled);
    this.xattrsEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY,
        DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_DEFAULT);
    LOG.info("XAttrs enabled? " + xattrsEnabled);
    this.xattrMaxSize = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_DEFAULT);
    Preconditions.checkArgument(xattrMaxSize >= 0,
                                "Cannot set a negative value for the maximum size of an xattr (%s).",
                                DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY);
    final String unlimited = xattrMaxSize == 0 ? " (unlimited)" : "";
    LOG.info("Maximum size of an xattr: " + xattrMaxSize + unlimited);

    this.accessTimePrecision = conf.getLong(
        DFS_NAMENODE_ACCESSTIME_PRECISION_KEY,
        DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT);

    this.storagePolicyEnabled =
        conf.getBoolean(DFS_STORAGE_POLICY_ENABLED_KEY,
                        DFS_STORAGE_POLICY_ENABLED_DEFAULT);

    this.quotaByStorageTypeEnabled =
        conf.getBoolean(DFS_QUOTA_BY_STORAGETYPE_ENABLED_KEY,
                        DFS_QUOTA_BY_STORAGETYPE_ENABLED_DEFAULT);

    int configuredLimit = conf.getInt(
        DFSConfigKeys.DFS_LIST_LIMIT, DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT);
    this.lsLimit = configuredLimit>0 ?
        configuredLimit : DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT; // 一次ls可以获取到文件数
    this.contentCountLimit = conf.getInt(
        DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_KEY,
        DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_DEFAULT);
    this.contentSleepMicroSec = conf.getLong(
        DFSConfigKeys.DFS_CONTENT_SUMMARY_SLEEP_MICROSEC_KEY,
        DFSConfigKeys.DFS_CONTENT_SUMMARY_SLEEP_MICROSEC_DEFAULT);
    
    // filesystem limits
    this.maxComponentLength = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_DEFAULT);
    this.maxDirItems = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_DEFAULT);
    this.inodeXAttrsLimit = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_DEFAULT);

    Preconditions.checkArgument(this.inodeXAttrsLimit >= 0,
        "Cannot set a negative limit on the number of xattrs per inode (%s).",
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY);
    // We need a maximum maximum because by default, PB limits message sizes
    // to 64MB. This means we can only store approximately 6.7 million entries
    // per directory, but let's use 6.4 million for some safety.
    final int MAX_DIR_ITEMS = 64 * 100 * 1000;
    Preconditions.checkArgument(
        maxDirItems > 0 && maxDirItems <= MAX_DIR_ITEMS, "Cannot set "
            + DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY
            + " to a value less than 1 or greater than " + MAX_DIR_ITEMS);

    int threshold = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_NAME_CACHE_THRESHOLD_KEY,
        DFSConfigKeys.DFS_NAMENODE_NAME_CACHE_THRESHOLD_DEFAULT);
    NameNode.LOG.info("Caching file names occuring more than " + threshold
        + " times");
    nameCache = new NameCache<ByteArray>(threshold);
    namesystem = ns;
    this.editLog = ns.getEditLog();
    ezManager = new EncryptionZoneManager(this, conf);
  }
    
  FSNamesystem getFSNamesystem() {
    return namesystem;
  }

  private BlockManager getBlockManager() {
    return getFSNamesystem().getBlockManager();
  }

  /** @return the root directory inode. */
  public INodeDirectory getRoot() {
    return rootDir;
  }

  public BlockStoragePolicySuite getBlockStoragePolicySuite() {
    return getBlockManager().getStoragePolicySuite();
  }

  boolean isPermissionEnabled() {
    return isPermissionEnabled;
  }
  boolean isAclsEnabled() {
    return aclsEnabled;
  }
  boolean isXattrsEnabled() {
    return xattrsEnabled;
  }
  int getXattrMaxSize() { return xattrMaxSize; }
  boolean isStoragePolicyEnabled() {
    return storagePolicyEnabled;
  }
  boolean isAccessTimeSupported() {
    return accessTimePrecision > 0;
  }
  boolean isQuotaByStorageTypeEnabled() {
    return quotaByStorageTypeEnabled;
  }


  int getLsLimit() {
    return lsLimit;
  }

  int getContentCountLimit() {
    return contentCountLimit;
  }

  long getContentSleepMicroSec() {
    return contentSleepMicroSec;
  }

  int getInodeXAttrsLimit() {
    return inodeXAttrsLimit;
  }

  FSEditLog getEditLog() {
    return editLog;
  }

  /**
   * Shutdown the filestore
   */
  @Override
  public void close() throws IOException {}

  void markNameCacheInitialized() {
    writeLock();
    try {
      nameCache.initialized();
    } finally {
      writeUnlock();
    }
  }

  boolean shouldSkipQuotaChecks() {
    return skipQuotaCheck;
  }

  /** Enable quota verification */
  void enableQuotaChecks() {
    skipQuotaCheck = false;
  }

  /** Disable quota verification */
  void disableQuotaChecks() {
    skipQuotaCheck = true;
  }

  private static INodeFile newINodeFile(long id, PermissionStatus permissions,
      long mtime, long atime, short replication, long preferredBlockSize) {
    return newINodeFile(id, permissions, mtime, atime, replication,
        preferredBlockSize, (byte)0);
  }

  private static INodeFile newINodeFile(long id, PermissionStatus permissions,
      long mtime, long atime, short replication, long preferredBlockSize,
      byte storagePolicyId) {
    return new INodeFile(id, null, permissions, mtime, atime,
        BlockInfoContiguous.EMPTY_ARRAY, replication, preferredBlockSize,
        storagePolicyId);
  }

  /**
   * Add the given filename to the fs.
   * @return the new INodesInPath instance that contains the new INode
   */
  INodesInPath addFile(INodesInPath existing, String localName, PermissionStatus
      permissions, short replication, long preferredBlockSize,
      String clientName, String clientMachine)
    throws FileAlreadyExistsException, QuotaExceededException,
      UnresolvedLinkException, SnapshotAccessControlException, AclException { // 创建文件

    long modTime = now();
    INodeFile newNode = newINodeFile(allocateNewInodeId(), permissions, modTime,
        modTime, replication, preferredBlockSize); // 创建inode
    newNode.setLocalName(localName.getBytes(Charsets.UTF_8));
    newNode.toUnderConstruction(clientName, clientMachine);

    INodesInPath newiip;
    writeLock();
    try {
      newiip = addINode(existing, newNode); // 添加到父目录
    } finally {
      writeUnlock();
    }
    if (newiip == null) {
      NameNode.stateChangeLog.info("DIR* addFile: failed to add " +
          existing.getPath() + "/" + localName);
      return null;
    }

    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* addFile: " + localName + " is added");
    }
    return newiip;
  }

  INodeFile addFileForEditLog(long id, INodesInPath existing, byte[] localName,
      PermissionStatus permissions, List<AclEntry> aclEntries,
      List<XAttr> xAttrs, short replication, long modificationTime, long atime,
      long preferredBlockSize, boolean underConstruction, String clientName,
      String clientMachine, byte storagePolicyId) {
    final INodeFile newNode;
    assert hasWriteLock();
    if (underConstruction) {
      newNode = newINodeFile(id, permissions, modificationTime,
          modificationTime, replication, preferredBlockSize, storagePolicyId);
      newNode.toUnderConstruction(clientName, clientMachine);
    } else {
      newNode = newINodeFile(id, permissions, modificationTime, atime,
          replication, preferredBlockSize, storagePolicyId);
    }

    newNode.setLocalName(localName);
    try {
      INodesInPath iip = addINode(existing, newNode);
      if (iip != null) {
        if (aclEntries != null) {
          AclStorage.updateINodeAcl(newNode, aclEntries, CURRENT_STATE_ID);
        }
        if (xAttrs != null) {
          XAttrStorage.updateINodeXAttrs(newNode, xAttrs, CURRENT_STATE_ID);
        }
        return newNode;
      }
    } catch (IOException e) {
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
            "DIR* FSDirectory.unprotectedAddFile: exception when add "
                + existing.getPath() + " to the file system", e);
      }
    }
    return null;
  }

  /**
   * Add a block to the file. Returns a reference to the added block.
   */
  BlockInfoContiguous addBlock(String path, INodesInPath inodesInPath,
      Block block, DatanodeStorageInfo[] targets) throws IOException { // 给指定的inode添加一个block
    writeLock();
    try {
      final INodeFile fileINode = inodesInPath.getLastINode().asFile();
      Preconditions.checkState(fileINode.isUnderConstruction());

      // check quota limits and updated space consumed
      updateCount(inodesInPath, 0, fileINode.getPreferredBlockSize(),
          fileINode.getBlockReplication(), true); // 检查并更新quota

      // associate new last block for the file
      BlockInfoContiguousUnderConstruction blockInfo = // UC状态的block
        new BlockInfoContiguousUnderConstruction(
            block,
            fileINode.getFileReplication(),
            BlockUCState.UNDER_CONSTRUCTION,
            targets); // 申请block的时候，就有期望的副本位置信息
      getBlockManager().addBlockCollection(blockInfo, fileINode); // 添加到blocksMap
      fileINode.addBlock(blockInfo); // 添加到文件的block列表

      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* FSDirectory.addBlock: "
            + path + " with " + block
            + " block is added to the in-memory "
            + "file system");
      }
      return blockInfo;
    } finally {
      writeUnlock();
    }
  }

  /**
   * Remove a block from the file.
   * @return Whether the block exists in the corresponding file
   */
  boolean removeBlock(String path, INodesInPath iip, INodeFile fileNode,
      Block block) throws IOException {
    Preconditions.checkArgument(fileNode.isUnderConstruction());
    writeLock();
    try {
      return unprotectedRemoveBlock(path, iip, fileNode, block);
    } finally {
      writeUnlock();
    }
  }
  
  boolean unprotectedRemoveBlock(String path, INodesInPath iip,
      INodeFile fileNode, Block block) throws IOException {
    // modify file-> block and blocksMap
    // fileNode should be under construction
    boolean removed = fileNode.removeLastBlock(block); // 移除最后一个block
    if (!removed) {
      return false;
    }
    getBlockManager().removeBlockFromMap(block); // 内存中删除block

    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSDirectory.removeBlock: "
          +path+" with "+block
          +" block is removed from the file system");
    }

    // update space consumed
    updateCount(iip, 0, -fileNode.getPreferredBlockSize(),
        fileNode.getBlockReplication(), true); // 检查并更新quota
    return true;
  }

  /**
   * This is a wrapper for resolvePath(). If the path passed
   * is prefixed with /.reserved/raw, then it checks to ensure that the caller
   * has super user privileges.
   *
   * @param pc The permission checker used when resolving path.
   * @param path The path to resolve.
   * @param pathComponents path components corresponding to the path
   * @return if the path indicates an inode, return path after replacing up to
   *         <inodeid> with the corresponding path of the inode, else the path
   *         in {@code src} as is. If the path refers to a path in the "raw"
   *         directory, return the non-raw pathname.
   * @throws FileNotFoundException
   * @throws AccessControlException
   */
  String resolvePath(FSPermissionChecker pc, String path, byte[][] pathComponents)
      throws FileNotFoundException, AccessControlException { // 普通path直接返回，如果是保留目录，需要解析出真实path
    if (isReservedRawName(path) && isPermissionEnabled) {
      pc.checkSuperuserPrivilege(); // /.reserved/raw 目录需要超级用户权限
    }
    return resolvePath(path, pathComponents, this);
  }

  /**
   * @return true if the path is a non-empty directory; otherwise, return false.
   */
  boolean isNonEmptyDirectory(INodesInPath inodesInPath) { // 检查目录下，是否有元素
    readLock();
    try {
      final INode inode = inodesInPath.getLastINode();
      if (inode == null || !inode.isDirectory()) {
        //not found or not a directory
        return false;
      }
      final int s = inodesInPath.getPathSnapshotId();
      return !inode.asDirectory().getChildrenList(s).isEmpty();
    } finally {
      readUnlock();
    }
  }

  /**
   * Check whether the filepath could be created
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  boolean isValidToCreate(String src, INodesInPath iip)
      throws SnapshotAccessControlException {
    String srcs = normalizePath(src);
    return srcs.startsWith("/") && !srcs.endsWith("/") &&
        iip.getLastINode() == null;
  }

  /**
   * Check whether the path specifies a directory
   */
  boolean isDir(String src) throws UnresolvedLinkException {
    src = normalizePath(src);
    readLock();
    try {
      INode node = getINode(src, false);
      return node != null && node.isDirectory();
    } finally {
      readUnlock();
    }
  }

  /** Updates namespace, storagespace and typespaces consumed for all
   * directories until the parent directory of file represented by path.
   *
   * @param iip the INodesInPath instance containing all the INodes for
   *            updating quota usage
   * @param nsDelta the delta change of namespace
   * @param ssDelta the delta change of storage space consumed without replication
   * @param replication the replication factor of the block consumption change
   * @throws QuotaExceededException if the new count violates any quota limit
   * @throws FileNotFoundException if path does not exist.
   */
  void updateSpaceConsumed(INodesInPath iip, long nsDelta, long ssDelta, short replication)
    throws QuotaExceededException, FileNotFoundException,
    UnresolvedLinkException, SnapshotAccessControlException {
    writeLock();
    try {
      if (iip.getLastINode() == null) {
        throw new FileNotFoundException("Path not found: " + iip.getPath());
      }
      updateCount(iip, nsDelta, ssDelta, replication, true);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Update the quota usage after deletion. The quota update is only necessary
   * when image/edits have been loaded and the file/dir to be deleted is not
   * contained in snapshots.
   */
  // 使用的场景有：delete、rename
  void updateCountForDelete(final INode inode, final INodesInPath iip) { // 更新quota
    if (getFSNamesystem().isImageLoaded() &&
        !inode.isInLatestSnapshot(iip.getLatestSnapshotId())) {
      QuotaCounts counts = inode.computeQuotaUsage(getBlockStoragePolicySuite()); // 计算quota的增减值
      unprotectedUpdateCount(iip, iip.length() - 1, counts.negation());
    }
  }

  /**
   * Update usage count without replication factor change
   */
  // 通过指定delta，更新quota
  // 使用的场景有：addBlock, abandonBlock
  void updateCount(INodesInPath iip, long nsDelta, long ssDelta, short replication,
      boolean checkQuota) throws QuotaExceededException { // 检查并更新quota
    final INodeFile fileINode = iip.getLastINode().asFile();
    EnumCounters<StorageType> typeSpaceDeltas =  // 获取存储类型quota
      getStorageTypeDeltas(fileINode.getStoragePolicyID(), ssDelta,
          replication, replication);
    updateCount(iip, iip.length() - 1,
      new QuotaCounts.Builder().nameSpace(nsDelta).storageSpace(ssDelta * replication).
          typeSpaces(typeSpaceDeltas).build(),
        checkQuota); // 检查并更新quota
  }

  /**
   * Update usage count with replication factor change due to setReplication
   */
  void updateCount(INodesInPath iip, long nsDelta, long ssDelta, short oldRep,
      short newRep, boolean checkQuota) throws QuotaExceededException {
    final INodeFile fileINode = iip.getLastINode().asFile();
    EnumCounters<StorageType> typeSpaceDeltas =
        getStorageTypeDeltas(fileINode.getStoragePolicyID(), ssDelta, oldRep, newRep);
    updateCount(iip, iip.length() - 1,
        new QuotaCounts.Builder().nameSpace(nsDelta).
            storageSpace(ssDelta * (newRep - oldRep)).
            typeSpaces(typeSpaceDeltas).build(),
        checkQuota);
  }

  /** update count of each inode with quota
   * 
   * @param iip inodes in a path
   * @param numOfINodes the number of inodes to update starting from index 0
   * @param counts the count of space/namespace/type usage to be update
   * @param checkQuota if true then check if quota is exceeded
   * @throws QuotaExceededException if the new count violates any quota limit
   */
  // 存储空间quota考虑了副本数
  void updateCount(INodesInPath iip, int numOfINodes,
                    QuotaCounts counts, boolean checkQuota)
                    throws QuotaExceededException { // 检查并更新quota
    assert hasWriteLock();
    if (!namesystem.isImageLoaded()) {
      //still initializing. do not check or update quotas.
      return;
    }
    if (numOfINodes > iip.length()) {
      numOfINodes = iip.length();
    }
    if (checkQuota && !skipQuotaCheck) {
      verifyQuota(iip, numOfINodes, counts, null);// 每级父目录都要检查
    }
    unprotectedUpdateCount(iip, numOfINodes, counts); // 每级父目录都要更新
  }
  
  /** 
   * update quota of each inode and check to see if quota is exceeded. 
   * See {@link #updateCount(INodesInPath, int, QuotaCounts, boolean)}
   */ 
   void updateCountNoQuotaCheck(INodesInPath inodesInPath,
      int numOfINodes, QuotaCounts counts) { // 只更新quota不检查
    assert hasWriteLock();
    try {
      updateCount(inodesInPath, numOfINodes, counts, false);
    } catch (QuotaExceededException e) {
      NameNode.LOG.error("BUG: unexpected exception ", e);
    }
  }
  
  /**
   * updates quota without verification
   * callers responsibility is to make sure quota is not exceeded
   */
  static void unprotectedUpdateCount(INodesInPath inodesInPath,
      int numOfINodes, QuotaCounts counts) { // 每级父目录都要更新
    for(int i=0; i < numOfINodes; i++) { // 正向更新
      if (inodesInPath.getINode(i).isQuotaSet()) { // a directory with quota 此目录是否开启了quota
        inodesInPath.getINode(i).asDirectory().getDirectoryWithQuotaFeature()
            .addSpaceConsumed2Cache(counts); // 增量更新
      }
    }
  }

  public EnumCounters<StorageType> getStorageTypeDeltas(byte storagePolicyID,
      long dsDelta, short oldRep, short newRep) { // 获取存储类型quota
    EnumCounters<StorageType> typeSpaceDeltas = // 存储类型quota
        new EnumCounters<StorageType>(StorageType.class);
    // Storage type and its quota are only available when storage policy is set
    if (storagePolicyID != BlockStoragePolicySuite.ID_UNSPECIFIED) {
      BlockStoragePolicy storagePolicy = getBlockManager().getStoragePolicy(storagePolicyID);

      if (oldRep != newRep) {
        List<StorageType> oldChosenStorageTypes =
            storagePolicy.chooseStorageTypes(oldRep);

        for (StorageType t : oldChosenStorageTypes) {
          if (!t.supportTypeQuota()) {
            continue;
          }
          Preconditions.checkArgument(dsDelta > 0);
          typeSpaceDeltas.add(t, -dsDelta);
        }
      }

      List<StorageType> newChosenStorageTypes =
          storagePolicy.chooseStorageTypes(newRep);

      for (StorageType t : newChosenStorageTypes) {
        if (!t.supportTypeQuota()) {
          continue;
        }
        typeSpaceDeltas.add(t, dsDelta);
      }
    }
    return typeSpaceDeltas;
  }

  /** Return the name of the path represented by inodes at [0, pos] */
  static String getFullPathName(INode[] inodes, int pos) { // 获取path字符串
    StringBuilder fullPathName = new StringBuilder();
    if (inodes[0].isRoot()) {
      if (pos == 0) return Path.SEPARATOR;
    } else {
      fullPathName.append(inodes[0].getLocalName());
    }
    
    for (int i=1; i<=pos; i++) {
      fullPathName.append(Path.SEPARATOR_CHAR).append(inodes[i].getLocalName());
    }
    return fullPathName.toString();
  }

  /**
   * @return the relative path of an inode from one of its ancestors,
   *         represented by an array of inodes.
   */
  private static INode[] getRelativePathINodes(INode inode, INode ancestor) { // 获取path inode数组
    // calculate the depth of this inode from the ancestor
    int depth = 0; // path深度
    for (INode i = inode; i != null && !i.equals(ancestor); i = i.getParent()) {
      depth++;
    }
    INode[] inodes = new INode[depth]; // path inode数组

    // fill up the inodes in the path from this inode to root
    for (int i = 0; i < depth; i++) {
      if (inode == null) {
        NameNode.stateChangeLog.warn("Could not get full path."
            + " Corresponding file might have deleted already.");
        return null;
      }
      inodes[depth-i-1] = inode;
      inode = inode.getParent();
    }
    return inodes;
  }
  
  private static INode[] getFullPathINodes(INode inode) {
    return getRelativePathINodes(inode, null);
  }
  
  /** Return the full path name of the specified inode */
  static String getFullPathName(INode inode) {
    INode[] inodes = getFullPathINodes(inode); // 获取path inode数组
    // inodes can be null only when its called without holding lock
    return inodes == null ? "" : getFullPathName(inodes, inodes.length - 1); // 获取path字符串
  }

  /**
   * Add the given child to the namespace.
   * @param existing the INodesInPath containing all the ancestral INodes
   * @param child the new INode to add
   * @return a new INodesInPath instance containing the new child INode. Null
   * if the adding fails.
   * @throws QuotaExceededException is thrown if it violates quota limit
   */
  INodesInPath addINode(INodesInPath existing, INode child)
      throws QuotaExceededException, UnresolvedLinkException {
    cacheName(child); // 缓存path name
    writeLock();
    try {
      return addLastINode(existing, child, true); // 添加到父目录
    } finally {
      writeUnlock();
    }
  }

  /**
   * Verify quota for adding or moving a new INode with required 
   * namespace and storagespace to a given position.
   *  
   * @param iip INodes corresponding to a path
   * @param pos position where a new INode will be added
   * @param deltas needed namespace, storagespace and storage types
   * @param commonAncestor Last node in inodes array that is a common ancestor
   *          for a INode that is being moved from one location to the other.
   *          Pass null if a node is not being moved.
   * @throws QuotaExceededException if quota limit is exceeded.
   */
  static void verifyQuota(INodesInPath iip, int pos, QuotaCounts deltas,
                          INode commonAncestor) throws QuotaExceededException { // 检查quota是否超了
    if (deltas.getNameSpace() <= 0 && deltas.getStorageSpace() <= 0
        && deltas.getTypeSpaces().allLessOrEqual(0L)) { // quota delta小于等于0，就不用检查了
      // if quota is being freed or not being consumed
      return;
    }

    // check existing components in the path
    for(int i = (pos > iip.length() ? iip.length(): pos) - 1; i >= 0; i--) { // 反向，每级父目录都要检查quota
      if (commonAncestor == iip.getINode(i)) {
        // Stop checking for quota when common ancestor is reached
        return;
      }
      final DirectoryWithQuotaFeature q
          = iip.getINode(i).asDirectory().getDirectoryWithQuotaFeature();
      if (q != null) { // a directory with quota 目录是否设置了quota
        try {
          q.verifyQuota(deltas); // 检查是否超quota
        } catch (QuotaExceededException e) {
          List<INode> inodes = iip.getReadOnlyINodes();
          final String path = getFullPathName(inodes.toArray(new INode[inodes.size()]), i);
          e.setPathName(path);
          throw e;
        }
      }
    }
  }

  /** Verify if the inode name is legal. */
  void verifyINodeName(byte[] childName) throws HadoopIllegalArgumentException { // 检查name是否是保留字符
    if (Arrays.equals(HdfsConstants.DOT_SNAPSHOT_DIR_BYTES, childName)) {
      String s = "\"" + HdfsConstants.DOT_SNAPSHOT_DIR + "\" is a reserved name.";
      if (!namesystem.isImageLoaded()) {
        s += "  Please rename it before upgrade.";
      }
      throw new HadoopIllegalArgumentException(s);
    }
  }

  /**
   * Verify child's name for fs limit.
   *
   * @param childName byte[] containing new child name
   * @param parentPath String containing parent path
   * @throws PathComponentTooLongException child's name is too long.
   */
  void verifyMaxComponentLength(byte[] childName, String parentPath)
      throws PathComponentTooLongException { // 判断name是否超长
    if (maxComponentLength == 0) {
      return;
    }

    final int length = childName.length;
    if (length > maxComponentLength) {
      final PathComponentTooLongException e = new PathComponentTooLongException(
          maxComponentLength, length, parentPath,
          DFSUtil.bytes2String(childName));
      if (namesystem.isImageLoaded()) {
        throw e;
      } else {
        // Do not throw if edits log is still being processed
        NameNode.LOG.error("ERROR in FSDirectory.verifyINodeName", e);
      }
    }
  }

  /**
   * Verify children size for fs limit.
   *
   * @throws MaxDirectoryItemsExceededException too many children.
   */
  void verifyMaxDirItems(INodeDirectory parent, String parentPath)
      throws MaxDirectoryItemsExceededException { // 一个目录下的child数量是否超上限
    final int count = parent.getChildrenList(CURRENT_STATE_ID).size();
    if (count >= maxDirItems) {
      final MaxDirectoryItemsExceededException e
          = new MaxDirectoryItemsExceededException(maxDirItems, count);
      if (namesystem.isImageLoaded()) {
        e.setPathName(parentPath);
        throw e;
      } else {
        // Do not throw if edits log is still being processed
        NameNode.LOG.error("FSDirectory.verifyMaxDirItems: "
            + e.getLocalizedMessage());
      }
    }
  }

  /**
   * Add a child to the end of the path specified by INodesInPath.
   * @return an INodesInPath instance containing the new INode
   */
  @VisibleForTesting
  public INodesInPath addLastINode(INodesInPath existing, INode inode,
      boolean checkQuota) throws QuotaExceededException { // 添加到父目录的child列表，并修改child的parent字段
    assert existing.getLastINode() != null &&
        existing.getLastINode().isDirectory();

    final int pos = existing.length();
    // Disallow creation of /.reserved. This may be created when loading
    // editlog/fsimage during upgrade since /.reserved was a valid name in older
    // release. This may also be called when a user tries to create a file
    // or directory /.reserved.
    if (pos == 1 && existing.getINode(0) == rootDir && isReservedName(inode)) {
      throw new HadoopIllegalArgumentException(
          "File name \"" + inode.getLocalName() + "\" is reserved and cannot "
              + "be created. If this is during upgrade change the name of the "
              + "existing file or directory to another name before upgrading "
              + "to the new release.");
    }
    final INodeDirectory parent = existing.getINode(pos - 1).asDirectory();
    // The filesystem limits are not really quotas, so this check may appear
    // odd. It's because a rename operation deletes the src, tries to add
    // to the dest, if that fails, re-adds the src from whence it came.
    // The rename code disables the quota when it's restoring to the
    // original location because a quota violation would cause the the item
    // to go "poof".  The fs limits must be bypassed for the same reason.
    if (checkQuota) {
      final String parentPath = existing.getPath();
      verifyMaxComponentLength(inode.getLocalNameBytes(), parentPath); // 判断name是否超长
      verifyMaxDirItems(parent, parentPath); // 一个目录下的child数量是否超上限
    }
    // always verify inode name
    verifyINodeName(inode.getLocalNameBytes()); // 检查name是否是保留字符

    // 新建文件或目录，只会 nsQuota+1
    // rename场景 存储空间quota delta计算的是整个文件的大小
    final QuotaCounts counts = inode.computeQuotaUsage(getBlockStoragePolicySuite()); // 计算quota增减值
    updateCount(existing, pos, counts, checkQuota); // 检查并更新quota

    boolean isRename = (inode.getParent() != null);
    boolean added;
    try {
      // 添加到父节点
      added = parent.addChild(inode, true, existing.getLatestSnapshotId());
    } catch (QuotaExceededException e) {
      updateCountNoQuotaCheck(existing, pos, counts.negation()); // 添加失败则把刚才加的quota再减回去 nsQuota-1
      throw e;
    }
    if (!added) {
      updateCountNoQuotaCheck(existing, pos, counts.negation());
      return null;
    } else {
      if (!isRename) {
        AclStorage.copyINodeDefaultAcl(inode);
      }
      addToInodeMap(inode); // 添加到inodeMap
    }
    return INodesInPath.append(existing, inode, inode.getLocalNameBytes());
  }

  INodesInPath addLastINodeNoQuotaCheck(INodesInPath existing, INode i) {
    try {
      return addLastINode(existing, i, false);
    } catch (QuotaExceededException e) {
      NameNode.LOG.warn("FSDirectory.addChildNoQuotaCheck - unexpected", e);
    }
    return null;
  }

  /**
   * Remove the last inode in the path from the namespace.
   * Note: the caller needs to update the ancestors' quota count.
   *
   * @return -1 for failing to remove;
   *          0 for removing a reference whose referred inode has other 
   *            reference nodes;
   *          1 otherwise.
   */
  @VisibleForTesting
  public long removeLastINode(final INodesInPath iip) { // 移除最后一级inode
    final int latestSnapshot = iip.getLatestSnapshotId();
    final INode last = iip.getLastINode(); // 待移除inode
    final INodeDirectory parent = iip.getINode(-2).asDirectory(); // 待移除inode的父目录
    if (!parent.removeChild(last, latestSnapshot)) { // 删除child
      return -1;
    }

    return (!last.isInLatestSnapshot(latestSnapshot)
        && INodeReference.tryRemoveReference(last) > 0) ? 0 : 1;
  }

  static String normalizePath(String src) { // 标准化path，不能以'/'结尾
    if (src.length() > 1 && src.endsWith("/")) {
      src = src.substring(0, src.length() - 1);
    }
    return src;
  }

  @VisibleForTesting
  public long getYieldCount() {
    return yieldCount;
  }

  void addYieldCount(long value) {
    yieldCount += value;
  }

  public INodeMap getINodeMap() {
    return inodeMap;
  }

  /**
   * FSEditLogLoader implementation.
   * Unlike FSNamesystem.truncate, this will not schedule block recovery.
   */
  void unprotectedTruncate(String src, String clientName, String clientMachine,
                           long newLength, long mtime, Block truncateBlock)
      throws UnresolvedLinkException, QuotaExceededException,
      SnapshotAccessControlException, IOException {
    INodesInPath iip = getINodesInPath(src, true);
    INodeFile file = iip.getLastINode().asFile();
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    boolean onBlockBoundary =
        unprotectedTruncate(iip, newLength, collectedBlocks, mtime, null);

    if(! onBlockBoundary) {
      BlockInfoContiguous oldBlock = file.getLastBlock();
      Block tBlk =
      getFSNamesystem().prepareFileForTruncate(iip,
          clientName, clientMachine, file.computeFileSize() - newLength,
          truncateBlock);
      assert Block.matchingIdAndGenStamp(tBlk, truncateBlock) &&
          tBlk.getNumBytes() == truncateBlock.getNumBytes() :
          "Should be the same block.";
      if(oldBlock.getBlockId() != tBlk.getBlockId() &&
         !file.isBlockInLatestSnapshot(oldBlock)) {
        getBlockManager().removeBlockFromMap(oldBlock);
      }
    }
    assert onBlockBoundary == (truncateBlock == null) :
      "truncateBlock is null iff on block boundary: " + truncateBlock;
    getFSNamesystem().removeBlocksAndUpdateSafemodeTotal(collectedBlocks);
  }

  boolean truncate(INodesInPath iip, long newLength,
                   BlocksMapUpdateInfo collectedBlocks,
                   long mtime, QuotaCounts delta)
      throws IOException {
    writeLock();
    try {
      return unprotectedTruncate(iip, newLength, collectedBlocks, mtime, delta);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Truncate has the following properties:
   * 1.) Any block deletions occur now.
   * 2.) INode length is truncated now – new clients can only read up to
   * the truncated length.
   * 3.) INode will be set to UC and lastBlock set to UNDER_RECOVERY.
   * 4.) NN will trigger DN truncation recovery and waits for DNs to report.
   * 5.) File is considered UNDER_RECOVERY until truncation recovery completes.
   * 6.) Soft and hard Lease expiration require truncation recovery to complete.
   *
   * @return true if on the block boundary or false if recovery is need
   */
  boolean unprotectedTruncate(INodesInPath iip, long newLength,
                              BlocksMapUpdateInfo collectedBlocks,
                              long mtime, QuotaCounts delta) throws IOException {
    assert hasWriteLock();
    INodeFile file = iip.getLastINode().asFile();
    int latestSnapshot = iip.getLatestSnapshotId();
    file.recordModification(latestSnapshot, true);

    verifyQuotaForTruncate(iip, file, newLength, delta); // 检查quota

    long remainingLength =
        file.collectBlocksBeyondMax(newLength, collectedBlocks); // 先删除多余的block
    file.excludeSnapshotBlocks(latestSnapshot, collectedBlocks);
    file.setModificationTime(mtime);
    // return whether on a block boundary
    return (remainingLength - newLength) == 0; // 通过删除多余的block，是否就能达到预期的truncate
  }

  private void verifyQuotaForTruncate(INodesInPath iip, INodeFile file,
      long newLength, QuotaCounts delta) throws QuotaExceededException {
    if (!getFSNamesystem().isImageLoaded() || shouldSkipQuotaChecks()) {
      // Do not check quota if edit log is still being processed
      return;
    }
    final long diff = file.computeQuotaDeltaForTruncate(newLength);
    final short repl = file.getBlockReplication();
    delta.addStorageSpace(diff * repl);
    final BlockStoragePolicy policy = getBlockStoragePolicySuite()
        .getPolicy(file.getStoragePolicyID());
    List<StorageType> types = policy.chooseStorageTypes(repl);
    for (StorageType t : types) {
      if (t.supportTypeQuota()) {
        delta.addTypeSpace(t, diff);
      }
    }
    if (diff > 0) {
      readLock();
      try {
        verifyQuota(iip, iip.length() - 1, delta, null);
      } finally {
        readUnlock();
      }
    }
  }

  /**
   * This method is always called with writeLock of FSDirectory held.
   */
  public final void addToInodeMap(INode inode) {
    if (inode instanceof INodeWithAdditionalFields) { // 添加到inodeMap
      inodeMap.put(inode);
      if (!inode.isSymlink()) {
        final XAttrFeature xaf = inode.getXAttrFeature();
        addEncryptionZone((INodeWithAdditionalFields) inode, xaf);
      }
    }
  }

  private void addEncryptionZone(INodeWithAdditionalFields inode,
      XAttrFeature xaf) {
    if (xaf == null) {
      return;
    }
    final List<XAttr> xattrs = xaf.getXAttrs();
    for (XAttr xattr : xattrs) {
      final String xaName = XAttrHelper.getPrefixName(xattr);
      if (CRYPTO_XATTR_ENCRYPTION_ZONE.equals(xaName)) {
        try {
          final HdfsProtos.ZoneEncryptionInfoProto ezProto =
              HdfsProtos.ZoneEncryptionInfoProto.parseFrom(
                  xattr.getValue());
          ezManager.unprotectedAddEncryptionZone(inode.getId(),
              PBHelper.convert(ezProto.getSuite()),
              PBHelper.convert(ezProto.getCryptoProtocolVersion()),
              ezProto.getKeyName());
        } catch (InvalidProtocolBufferException e) {
          NameNode.LOG.warn("Error parsing protocol buffer of " +
              "EZ XAttr " + xattr.getName());
        }
      }
    }
  }
  
  /**
   * This is to handle encryption zone for rootDir when loading from
   * fsimage, and should only be called during NN restart.
   */
  public final void addRootDirToEncryptionZone(XAttrFeature xaf) {
    addEncryptionZone(rootDir, xaf);
  }

  /**
   * This method is always called with writeLock of FSDirectory held.
   */
  public final void removeFromInodeMap(List<? extends INode> inodes) { // 从inodeMap中删除
    if (inodes != null) {
      for (INode inode : inodes) {
        if (inode != null && inode instanceof INodeWithAdditionalFields) {
          inodeMap.remove(inode);
          ezManager.removeEncryptionZone(inode.getId());
        }
      }
    }
  }
  
  /**
   * Get the inode from inodeMap based on its inode id.
   * @param id The given id
   * @return The inode associated with the given id
   */
  public INode getInode(long id) {
    readLock();
    try {
      return inodeMap.get(id);
    } finally {
      readUnlock();
    }
  }
  
  @VisibleForTesting
  int getInodeMapSize() {
    return inodeMap.size();
  }

  long totalInodes() { // 50070页面上的files来源
    readLock();
    try {
      return rootDir.getDirectoryWithQuotaFeature().getSpaceConsumed()
          .getNameSpace();
    } finally {
      readUnlock();
    }
  }

  /**
   * Reset the entire namespace tree.
   */
  void reset() {
    writeLock();
    try {
      rootDir = createRoot(getFSNamesystem());
      inodeMap.clear();
      addToInodeMap(rootDir);
      nameCache.reset();
      inodeId.setCurrentValue(INodeId.LAST_RESERVED_ID);
    } finally {
      writeUnlock();
    }
  }

  boolean isInAnEZ(INodesInPath iip)
      throws UnresolvedLinkException, SnapshotAccessControlException {
    readLock();
    try {
      return ezManager.isInAnEZ(iip);
    } finally {
      readUnlock();
    }
  }

  String getKeyName(INodesInPath iip) {
    readLock();
    try {
      return ezManager.getKeyName(iip);
    } finally {
      readUnlock();
    }
  }

  XAttr createEncryptionZone(String src, CipherSuite suite,
      CryptoProtocolVersion version, String keyName)
    throws IOException {
    writeLock();
    try {
      return ezManager.createEncryptionZone(src, suite, version, keyName);
    } finally {
      writeUnlock();
    }
  }

  EncryptionZone getEZForPath(INodesInPath iip) {
    readLock();
    try {
      return ezManager.getEZINodeForPath(iip);
    } finally {
      readUnlock();
    }
  }

  BatchedListEntries<EncryptionZone> listEncryptionZones(long prevId)
      throws IOException {
    readLock();
    try {
      return ezManager.listEncryptionZones(prevId);
    } finally {
      readUnlock();
    }
  }

  /**
   * Set the FileEncryptionInfo for an INode.
   */
  void setFileEncryptionInfo(String src, FileEncryptionInfo info)
      throws IOException {
    // Make the PB for the xattr
    final HdfsProtos.PerFileEncryptionInfoProto proto =
        PBHelper.convertPerFileEncInfo(info);
    final byte[] protoBytes = proto.toByteArray();
    final XAttr fileEncryptionAttr =
        XAttrHelper.buildXAttr(CRYPTO_XATTR_FILE_ENCRYPTION_INFO, protoBytes);
    final List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
    xAttrs.add(fileEncryptionAttr);

    writeLock();
    try {
      FSDirXAttrOp.unprotectedSetXAttrs(this, src, xAttrs,
                                        EnumSet.of(XAttrSetFlag.CREATE));
    } finally {
      writeUnlock();
    }
  }

  /**
   * This function combines the per-file encryption info (obtained
   * from the inode's XAttrs), and the encryption info from its zone, and
   * returns a consolidated FileEncryptionInfo instance. Null is returned
   * for non-encrypted files.
   *
   * @param inode inode of the file
   * @param snapshotId ID of the snapshot that
   *                   we want to get encryption info from
   * @param iip inodes in the path containing the file, passed in to
   *            avoid obtaining the list of inodes again; if iip is
   *            null then the list of inodes will be obtained again
   * @return consolidated file encryption info; null for non-encrypted files
   */
  FileEncryptionInfo getFileEncryptionInfo(INode inode, int snapshotId,
      INodesInPath iip) throws IOException {
    if (!inode.isFile()) {
      return null;
    }
    readLock();
    try {
      EncryptionZone encryptionZone = getEZForPath(iip);
      if (encryptionZone == null) {
        // not an encrypted file
        return null;
      } else if(encryptionZone.getPath() == null
          || encryptionZone.getPath().isEmpty()) {
        if (NameNode.LOG.isDebugEnabled()) {
          NameNode.LOG.debug("Encryption zone " +
              encryptionZone.getPath() + " does not have a valid path.");
        }
      }

      final CryptoProtocolVersion version = encryptionZone.getVersion();
      final CipherSuite suite = encryptionZone.getSuite();
      final String keyName = encryptionZone.getKeyName();

      XAttr fileXAttr = FSDirXAttrOp.unprotectedGetXAttrByName(inode,
                                                               snapshotId,
                                                               CRYPTO_XATTR_FILE_ENCRYPTION_INFO);

      if (fileXAttr == null) {
        NameNode.LOG.warn("Could not find encryption XAttr for file " +
            iip.getPath() + " in encryption zone " + encryptionZone.getPath());
        return null;
      }

      try {
        HdfsProtos.PerFileEncryptionInfoProto fileProto =
            HdfsProtos.PerFileEncryptionInfoProto.parseFrom(
                fileXAttr.getValue());
        return PBHelper.convert(fileProto, suite, version, keyName);
      } catch (InvalidProtocolBufferException e) {
        throw new IOException("Could not parse file encryption info for " +
            "inode " + inode, e);
      }
    } finally {
      readUnlock();
    }
  }

  static INode resolveLastINode(INodesInPath iip) throws FileNotFoundException {
    INode inode = iip.getLastINode();
    if (inode == null) {
      throw new FileNotFoundException("cannot find " + iip.getPath());
    }
    return inode;
  }

  /**
   * Caches frequently used file names to reuse file name objects and
   * reduce heap size.
   */
  void cacheName(INode inode) { // 缓存path name
    // Name is cached only for files
    if (!inode.isFile()) {
      return;
    }
    ByteArray name = new ByteArray(inode.getLocalNameBytes());
    name = nameCache.put(name);
    if (name != null) {
      inode.setLocalName(name.getBytes());
    }
  }
  
  void shutdown() {
    nameCache.reset();
    inodeMap.clear();
  }
  
  /**
   * Given an INode get all the path complents leading to it from the root.
   * If an Inode corresponding to C is given in /A/B/C, the returned
   * patch components will be {root, A, B, C}.
   * Note that this method cannot handle scenarios where the inode is in a
   * snapshot.
   */
  public static byte[][] getPathComponents(INode inode) {
    List<byte[]> components = new ArrayList<byte[]>();
    components.add(0, inode.getLocalNameBytes());
    while(inode.getParent() != null) {
      components.add(0, inode.getParent().getLocalNameBytes());
      inode = inode.getParent();
    }
    return components.toArray(new byte[components.size()][]);
  }

  /**
   * @return path components for reserved path, else null.
   *
   *  保留目录(/.reserved/...)，按 / 拆分，每级目录使用byte数组表示
   *  非保留目录，返回null
   */
  static byte[][] getPathComponentsForReservedPath(String src) {
    return !isReservedName(src) ? null : INode.getPathComponents(src);
  }

  /** Check if a given inode name is reserved */
  public static boolean isReservedName(INode inode) {
    return CHECK_RESERVED_FILE_NAMES
            && Arrays.equals(inode.getLocalNameBytes(), DOT_RESERVED);
  }

  /** Check if a given path is reserved */
  public static boolean isReservedName(String src) { // 是否是保留目录 /.reserved/...
    return src.startsWith(DOT_RESERVED_PATH_PREFIX + Path.SEPARATOR);
  }

  static boolean isReservedRawName(String src) {
    return src.startsWith(DOT_RESERVED_PATH_PREFIX +
        Path.SEPARATOR + RAW_STRING);
  }

  /**
   * Resolve a /.reserved/... path to a non-reserved path.
   * <p/>
   * There are two special hierarchies under /.reserved/:
   * <p/>
   * /.reserved/.inodes/<inodeid> performs a path lookup by inodeid,
   * <p/>
   * /.reserved/raw/... returns the encrypted (raw) bytes of a file in an
   * encryption zone. For instance, if /ezone is an encryption zone, then
   * /ezone/a refers to the decrypted file and /.reserved/raw/ezone/a refers to
   * the encrypted (raw) bytes of /ezone/a.
   * <p/>
   * Pathnames in the /.reserved/raw directory that resolve to files not in an
   * encryption zone are equivalent to the corresponding non-raw path. Hence,
   * if /a/b/c refers to a file that is not in an encryption zone, then
   * /.reserved/raw/a/b/c is equivalent (they both refer to the same
   * unencrypted file).
   * 
   * @param src path that is being processed
   * @param pathComponents path components corresponding to the path
   * @param fsd FSDirectory
   * @return if the path indicates an inode, return path after replacing up to
   *         <inodeid> with the corresponding path of the inode, else the path
   *         in {@code src} as is. If the path refers to a path in the "raw"
   *         directory, return the non-raw pathname.
   * @throws FileNotFoundException if inodeid is invalid
   *
   * /.reserved/... 下面有两种特殊的path：
   * 1、/.reserved/.inodes/$inodeId：（老版）保存snapshot INodeFileUC文件的path，访问此path将会返回$inodeId的path
   * 参见HDFS-5428
   * 2、/.reserved/raw/...：保存被加密的文件内容
   * 比如：/ezone/a是加密文件（/ezone是一个encryption zone）
   * 访问/ezone/a：返回的是解密之后的内容
   * 访问/.reserved/raw/ezone/a：返回的是加密的内容
   *
   * 在/.reserved/目录下，但不是上面描述的这两种情况，都直接返回src
   */
  static String resolvePath(String src, byte[][] pathComponents,
      FSDirectory fsd) throws FileNotFoundException { // 普通path直接返回，如果是保留目录，需要解析出真实path
    final int nComponents = (pathComponents == null) ? // path层级数
        0 : pathComponents.length;
    if (nComponents <= 2) {
      return src; // 普通目录或 /.reserved下的非保留目录，直接返回
    }
    if (!Arrays.equals(DOT_RESERVED, pathComponents[1])) { // byte数组的比较效率更好，为何不用String.equals()方法？
      /* This is not a /.reserved/ path so do nothing. */
      return src; // 普通目录，直接返回
    }

    if (Arrays.equals(DOT_INODES, pathComponents[2])) { // /.reserved/.inodes/
      /* It's a /.reserved/.inodes path. */
      if (nComponents > 3) {
        return resolveDotInodesPath(src, pathComponents, fsd); // 通过inodeId查找文件路径
      } else {
        return src;
      }
    } else if (Arrays.equals(RAW, pathComponents[2])) { // /.reserved/raw
      /* It's /.reserved/raw so strip off the /.reserved/raw prefix. */
      if (nComponents == 3) {
        return Path.SEPARATOR;
      } else {
        // 返回真实path，比如访问/.reserved/raw/ezone/a，返回/ezone/a
        return constructRemainingPath("", pathComponents, 3);
      }
    } else {
      /* It's some sort of /.reserved/<unknown> path. Ignore it. */
      return src;
    }
  }

  // 通过inodeId查找文件路径，访问这种path：/.reserved/.inodes/<inodeid>
  private static String resolveDotInodesPath(String src,
      byte[][] pathComponents, FSDirectory fsd)
      throws FileNotFoundException {
    final String inodeId = DFSUtil.bytes2String(pathComponents[3]);
    final long id;
    try {
      id = Long.parseLong(inodeId);
    } catch (NumberFormatException e) {
      throw new FileNotFoundException("Invalid inode path: " + src);
    }
    if (id == INodeId.ROOT_INODE_ID && pathComponents.length == 4) {
      return Path.SEPARATOR;
    }
    INode inode = fsd.getInode(id);
    if (inode == null) {
      throw new FileNotFoundException(
          "File for given inode path does not exist: " + src);
    }
    
    // Handle single ".." for NFS lookup support.
    if ((pathComponents.length > 4)
        && DFSUtil.bytes2String(pathComponents[4]).equals("..")) {
      INode parent = inode.getParent();
      if (parent == null || parent.getId() == INodeId.ROOT_INODE_ID) {
        // inode is root, or its parent is root.
        return Path.SEPARATOR;
      } else {
        return parent.getFullPathName();
      }
    }

    String path = "";
    if (id != INodeId.ROOT_INODE_ID) {
      path = inode.getFullPathName();
    }
    return constructRemainingPath(path, pathComponents, 4);
  }

  private static String constructRemainingPath(String pathPrefix,
      byte[][] pathComponents, int startAt) {

    StringBuilder path = new StringBuilder(pathPrefix);
    for (int i = startAt; i < pathComponents.length; i++) {
      path.append(Path.SEPARATOR).append(
          DFSUtil.bytes2String(pathComponents[i]));
    }
    if (NameNode.LOG.isDebugEnabled()) {
      NameNode.LOG.debug("Resolved path is " + path);
    }
    return path.toString();
  }

  INode getINode4DotSnapshot(String src) throws UnresolvedLinkException {
    Preconditions.checkArgument(
        src.endsWith(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR),
        "%s does not end with %s", src, HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR);

    final String dirPath = normalizePath(src.substring(0,
        src.length() - HdfsConstants.DOT_SNAPSHOT_DIR.length()));

    final INode node = this.getINode(dirPath);
    if (node != null && node.isDirectory()
        && node.asDirectory().isSnapshottable()) {
      return node;
    }
    return null;
  }

  INodesInPath getExistingPathINodes(byte[][] components)
      throws UnresolvedLinkException {
    return INodesInPath.resolve(rootDir, components, false);
  }

  /**
   * Get {@link INode} associated with the file / directory.
   */
  public INodesInPath getINodesInPath4Write(String src)
      throws UnresolvedLinkException, SnapshotAccessControlException {
    return getINodesInPath4Write(src, true);
  }

  /**
   * Get {@link INode} associated with the file / directory.
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  public INode getINode4Write(String src) throws UnresolvedLinkException,
      SnapshotAccessControlException {
    return getINodesInPath4Write(src, true).getLastINode();
  }

  /** @return the {@link INodesInPath} containing all inodes in the path. */
  public INodesInPath getINodesInPath(String path, boolean resolveLink)
      throws UnresolvedLinkException { // path解析为INodesInPath对象
    final byte[][] components = INode.getPathComponents(path);
    return INodesInPath.resolve(rootDir, components, resolveLink);
  }

  /** @return the last inode in the path. */
  INode getINode(String path, boolean resolveLink)
      throws UnresolvedLinkException {
    return getINodesInPath(path, resolveLink).getLastINode();
  }

  /**
   * Get {@link INode} associated with the file / directory.
   */
  public INode getINode(String src) throws UnresolvedLinkException {
    return getINode(src, true);
  }

  /**
   * @return the INodesInPath of the components in src
   * @throws UnresolvedLinkException if symlink can't be resolved
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  INodesInPath getINodesInPath4Write(String src, boolean resolveLink)
          throws UnresolvedLinkException, SnapshotAccessControlException {  // 通过path，正序获取每个层级的inode对象
    final byte[][] components = INode.getPathComponents(src); // 根据 / 切分path，每个部分用byte数组表示
    INodesInPath inodesInPath = INodesInPath.resolve(rootDir, components, // path数组 转为 inode数组
        resolveLink);
    if (inodesInPath.isSnapshot()) {
      throw new SnapshotAccessControlException(
              "Modification on a read-only snapshot is disallowed");
    }
    return inodesInPath;
  }

  FSPermissionChecker getPermissionChecker()
    throws AccessControlException {
    try {
      return getPermissionChecker(fsOwnerShortUserName, supergroup,
          NameNode.getRemoteUser());
    } catch (IOException e) {
      throw new AccessControlException(e);
    }
  }

  @VisibleForTesting
  FSPermissionChecker getPermissionChecker(String fsOwner, String superGroup,
      UserGroupInformation ugi) throws AccessControlException {
    return new FSPermissionChecker(
        fsOwner, superGroup, ugi,
        attributeProvider == null ?
            DefaultINodeAttributesProvider.DEFAULT_PROVIDER
            : attributeProvider);
  }

  void checkOwner(FSPermissionChecker pc, INodesInPath iip)
      throws AccessControlException {
    checkPermission(pc, iip, true, null, null, null, null);
  }

  void checkPathAccess(FSPermissionChecker pc, INodesInPath iip,
      FsAction access) throws AccessControlException {
    checkPermission(pc, iip, false, null, null, access, null);
  }
  void checkParentAccess(FSPermissionChecker pc, INodesInPath iip,
      FsAction access) throws AccessControlException {
    checkPermission(pc, iip, false, null, access, null, null);
  }

  void checkAncestorAccess(FSPermissionChecker pc, INodesInPath iip,
      FsAction access) throws AccessControlException {
    checkPermission(pc, iip, false, access, null, null, null);
  }

  void checkTraverse(FSPermissionChecker pc, INodesInPath iip)
      throws AccessControlException {
    checkPermission(pc, iip, false, null, null, null, null);
  }

  /**
   * Check whether current user have permissions to access the path. For more
   * details of the parameters, see
   * {@link FSPermissionChecker#checkPermission}.
   */
  void checkPermission(FSPermissionChecker pc, INodesInPath iip,
      boolean doCheckOwner, FsAction ancestorAccess, FsAction parentAccess,
      FsAction access, FsAction subAccess)
    throws AccessControlException {
    checkPermission(pc, iip, doCheckOwner, ancestorAccess,
        parentAccess, access, subAccess, false);
  }

  /**
   * Check whether current user have permissions to access the path. For more
   * details of the parameters, see
   * {@link FSPermissionChecker#checkPermission}.
   */
  void checkPermission(FSPermissionChecker pc, INodesInPath iip,
      boolean doCheckOwner, FsAction ancestorAccess, FsAction parentAccess,
      FsAction access, FsAction subAccess, boolean ignoreEmptyDir)
      throws AccessControlException {
    if (!pc.isSuperUser()) {
      readLock();
      try {
        pc.checkPermission(iip, doCheckOwner, ancestorAccess,
            parentAccess, access, subAccess, ignoreEmptyDir);
      } finally {
        readUnlock();
      }
    }
  }

  HdfsFileStatus getAuditFileInfo(INodesInPath iip)
      throws IOException { // 如果是外部来的请求，则返回HdfsFileStatus对象
    return (namesystem.isAuditEnabled() && namesystem.isExternalInvocation())
        ? FSDirStatAndListingOp.getFileInfo(this, iip.getPath(), iip, false,
            false) : null;
  }

  /**
   * Verify that parent directory of src exists.
   */
  void verifyParentDir(INodesInPath iip, String src)
      throws FileNotFoundException, ParentNotDirectoryException { // 检查父节点是否存在，不存在则抛异常
    Path parent = new Path(src).getParent();
    if (parent != null) {
      final INode parentNode = iip.getINode(-2);
      if (parentNode == null) { // 父节点不存在
        throw new FileNotFoundException("Parent directory doesn't exist: "
            + parent);
      } else if (!parentNode.isDirectory() && !parentNode.isSymlink()) { // 父节点既不是目录，也不是软链
        throw new ParentNotDirectoryException("Parent path is not a directory: "
            + parent);
      }
    }
  }

  /** Allocate a new inode ID. */
  long allocateNewInodeId() {
    return inodeId.nextValue();
  }

  /** @return the last inode ID. */
  public long getLastInodeId() {
    return inodeId.getCurrentValue();
  }

  /**
   * Set the last allocated inode id when fsimage or editlog is loaded.
   */
  void resetLastInodeId(long newValue) throws IOException {
    try {
      inodeId.skipTo(newValue);
    } catch(IllegalStateException ise) {
      throw new IOException(ise);
    }
  }

  /** Should only be used for tests to reset to any value */
  void resetLastInodeIdWithoutChecking(long newValue) {
    inodeId.setCurrentValue(newValue);
  }

  INodeAttributes getAttributes(String fullPath, byte[] path,
      INode node, int snapshot) {
    INodeAttributes nodeAttrs = node;
    if (attributeProvider != null) {
      nodeAttrs = node.getSnapshotINode(snapshot);
      fullPath = fullPath + (fullPath.endsWith(Path.SEPARATOR) ? ""
                                                               : Path.SEPARATOR)
          + DFSUtil.bytes2String(path);
      nodeAttrs = attributeProvider.getAttributes(fullPath, nodeAttrs);
    } else {
      nodeAttrs = node.getSnapshotINode(snapshot);
    }
    return nodeAttrs;
  }

}
