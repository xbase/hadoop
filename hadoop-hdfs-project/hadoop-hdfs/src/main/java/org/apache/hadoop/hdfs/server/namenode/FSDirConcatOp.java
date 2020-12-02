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

import com.google.common.base.Preconditions;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotException;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.List;

import static org.apache.hadoop.util.Time.now;

/**
 * Restrictions for a concat operation:
 * <pre>
 * 1. the src file and the target file are in the same dir
 * 2. all the source files are not in snapshot
 * 3. any source file cannot be the same with the target file
 * 4. source files cannot be under construction or empty
 * 5. source file's preferred block size cannot be greater than the target file
 * </pre>
 */
class FSDirConcatOp {

  static HdfsFileStatus concat(FSDirectory fsd, String target, String[] srcs,
    boolean logRetryCache) throws IOException {
    Preconditions.checkArgument(!target.isEmpty(), "Target file name is empty");
    Preconditions.checkArgument(srcs != null && srcs.length > 0,
      "No sources given");
    assert srcs != null;
    if (FSDirectory.LOG.isDebugEnabled()) {
      FSDirectory.LOG.debug("concat {} to {}", Arrays.toString(srcs), target);
    }

    final INodesInPath targetIIP = fsd.getINodesInPath4Write(target); // 获取target iip对象
    // write permission for the target
    FSPermissionChecker pc = null;
    if (fsd.isPermissionEnabled()) { // 检查权限
      pc = fsd.getPermissionChecker();
      fsd.checkPathAccess(pc, targetIIP, FsAction.WRITE);
    }

    // target和srcs都没有要求，最后一个块是满块
    // check the target
    verifyTargetFile(fsd, target, targetIIP); // 检查target是否合法
    // check the srcs
    INodeFile[] srcFiles = verifySrcFiles(fsd, srcs, targetIIP, pc); // 检查srcs是否合法

    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.concat: " +
          Arrays.toString(srcs) + " to " + target);
    }

    long timestamp = now();
    fsd.writeLock();
    try {
      unprotectedConcat(fsd, targetIIP, srcFiles, timestamp); // 合并srcs的block到target
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logConcat(target, srcs, timestamp, logRetryCache);
    return fsd.getAuditFileInfo(targetIIP);
  }

  // 检查target是否合法：
  // 1、target必须已经存在
  // 2、target不能加密
  // 3、不能正在构建中
  private static void verifyTargetFile(FSDirectory fsd, final String target,
      final INodesInPath targetIIP) throws IOException {
    // check the target
    if (fsd.getEZForPath(targetIIP) != null) { // 加密相关
      throw new HadoopIllegalArgumentException(
          "concat can not be called for files in an encryption zone.");
    }
    final INodeFile targetINode = INodeFile.valueOf(targetIIP.getLastINode(),
        target); // target必须已经存在
    if(targetINode.isUnderConstruction()) {
      throw new HadoopIllegalArgumentException("concat: target file "
          + target + " is under construction");
    }
  }

  // 检查srcs是否合法
  // 1、src和target必须在同一个目录
  // 2、src不能是snapshot
  // 3、src和target不能相同
  // 4、src不能正在构建中或没有block
  // 5、src的期望block size不能大于target
  // 6、srcs中不能有相同的文件
  private static INodeFile[] verifySrcFiles(FSDirectory fsd, String[] srcs,
      INodesInPath targetIIP, FSPermissionChecker pc) throws IOException {
    // to make sure no two files are the same
    Set<INodeFile> si = new LinkedHashSet<>();
    final INodeFile targetINode = targetIIP.getLastINode().asFile();
    final INodeDirectory targetParent = targetINode.getParent(); // target的目录
    // now check the srcs
    for(String src : srcs) {
      final INodesInPath iip = fsd.getINodesInPath4Write(src); // src iip对象
      // permission check for srcs
      if (pc != null) { // 检查权限
        fsd.checkPathAccess(pc, iip, FsAction.READ); // read the file
        fsd.checkParentAccess(pc, iip, FsAction.WRITE); // for delete
      }
      final INode srcINode = iip.getLastINode();
      final INodeFile srcINodeFile = INodeFile.valueOf(srcINode, src);
      // make sure the src file and the target file are in the same dir
      if (srcINodeFile.getParent() != targetParent) { // src和target必须在同一个目录
        throw new HadoopIllegalArgumentException("Source file " + src
            + " is not in the same directory with the target "
            + targetIIP.getPath());
      }
      // make sure all the source files are not in snapshot
      if (srcINode.isInLatestSnapshot(iip.getLatestSnapshotId())) { // src不能是snapshot
        throw new SnapshotException("Concat: the source file " + src
            + " is in snapshot");
      }
      // check if the file has other references.
      if (srcINode.isReference() && ((INodeReference.WithCount)
          srcINode.asReference().getReferredINode()).getReferenceCount() > 1) { // src不能是snapshot
        throw new SnapshotException("Concat: the source file " + src
            + " is referred by some other reference in some snapshot.");
      }
      // source file cannot be the same with the target file
      if (srcINode == targetINode) {  // src和target不能相同
        throw new HadoopIllegalArgumentException("concat: the src file " + src
            + " is the same with the target file " + targetIIP.getPath());
      }
      // source file cannot be under construction or empty
      if(srcINodeFile.isUnderConstruction() || srcINodeFile.numBlocks() == 0) {  // src不能正在构建中或没有block
        throw new HadoopIllegalArgumentException("concat: source file " + src
            + " is invalid or empty or underConstruction");
      }
      // source file's preferred block size cannot be greater than the target
      // file
      if (srcINodeFile.getPreferredBlockSize() >
          targetINode.getPreferredBlockSize()) { // src的期望block size不能大于target
        throw new HadoopIllegalArgumentException("concat: source file " + src
            + " has preferred block size " + srcINodeFile.getPreferredBlockSize()
            + " which is greater than the target file's preferred block size "
            + targetINode.getPreferredBlockSize());
      }
      si.add(srcINodeFile);
    }

    // make sure no two files are the same
    if(si.size() < srcs.length) { // srcs中不能有相同的文件
      // it means at least two files are the same
      throw new HadoopIllegalArgumentException(
          "concat: at least two of the source files are the same");
    }
    return si.toArray(new INodeFile[si.size()]);
  }

  // 计算quota delta，只有target和src的副本数不一样时，才需要更新quota
  private static QuotaCounts computeQuotaDeltas(FSDirectory fsd,
      INodeFile target, INodeFile[] srcList) {
    QuotaCounts deltas = new QuotaCounts.Builder().build();
    final short targetRepl = target.getBlockReplication(); // target期望的副本数
    for (INodeFile src : srcList) {
      short srcRepl = src.getBlockReplication(); // src期望的副本数
      long fileSize = src.computeFileSize(); // src的文件大小
      if (targetRepl != srcRepl) { // 只有target和src的副本数不一样时，才需要更新quota
        deltas.addStorageSpace(fileSize * (targetRepl - srcRepl)); // 存储空间quota delta
        BlockStoragePolicy bsp =
            fsd.getBlockStoragePolicySuite().getPolicy(src.getStoragePolicyID());
        if (bsp != null) { // 存储类型quota delte
          List<StorageType> srcTypeChosen = bsp.chooseStorageTypes(srcRepl);
          for (StorageType t : srcTypeChosen) {
            if (t.supportTypeQuota()) {
              deltas.addTypeSpace(t, -fileSize);
            }
          }
          List<StorageType> targetTypeChosen = bsp.chooseStorageTypes(targetRepl);
          for (StorageType t : targetTypeChosen) {
            if (t.supportTypeQuota()) {
              deltas.addTypeSpace(t, fileSize);
            }
          }
        }
      }
    }
    return deltas;
  }

  private static void verifyQuota(FSDirectory fsd, INodesInPath targetIIP,
      QuotaCounts deltas) throws QuotaExceededException {
    if (!fsd.getFSNamesystem().isImageLoaded() || fsd.shouldSkipQuotaChecks()) {
      // Do not check quota if editlog is still being processed
      return;
    }
    FSDirectory.verifyQuota(targetIIP, targetIIP.length() - 1, deltas, null);
  }

  /**
   * Concat all the blocks from srcs to trg and delete the srcs files
   * @param fsd FSDirectory
   */
  static void unprotectedConcat(FSDirectory fsd, INodesInPath targetIIP,
      INodeFile[] srcList, long timestamp) throws IOException { // 合并srcs的block到target
    assert fsd.hasWriteLock();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSNamesystem.concat to "
          + targetIIP.getPath());
    }

    final INodeFile trgInode = targetIIP.getLastINode().asFile();
    QuotaCounts deltas = computeQuotaDeltas(fsd, trgInode, srcList); // 计算quota delta，只有target和src的副本数不一样时，才需要更新quota
    verifyQuota(fsd, targetIIP, deltas); // 检查quota是否超了

    // the target file can be included in a snapshot
    trgInode.recordModification(targetIIP.getLatestSnapshotId()); // snapshot相关
    INodeDirectory trgParent = targetIIP.getINode(-2).asDirectory(); // target的目录
    trgInode.concatBlocks(srcList); // 合并srcs的block到target

    // since we are in the same dir - we can use same parent to remove files
    int count = 0;
    for (INodeFile nodeToRemove : srcList) { // 只删除srcs inode
      if(nodeToRemove != null) {
        nodeToRemove.setBlocks(null); // 移除block list的引用
        nodeToRemove.getParent().removeChild(nodeToRemove); // 删除此inode从parent的children列表
        fsd.getINodeMap().remove(nodeToRemove); // 删除此inode从inodeMap
        count++;
      }
    }

    trgInode.setModificationTime(timestamp, targetIIP.getLatestSnapshotId()); // 更新target的mtime
    trgParent.updateModificationTime(timestamp, targetIIP.getLatestSnapshotId()); // 更新target目录的mtime
    // update quota on the parent directory with deltas
    FSDirectory.unprotectedUpdateCount(targetIIP, targetIIP.length() - 1, deltas); // 更新quota
  }
}
