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
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager.AccessMode;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplicasMap.Reason;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo.AddBlockResult;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingDataNodeMessages.ReportedBlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.LightWeightGSet;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps information related to the blocks stored in the Hadoop cluster.
 */
@InterfaceAudience.Private
public class BlockManager {

  public static final Logger LOG = LoggerFactory.getLogger(BlockManager.class);
  public static final Logger blockLog = NameNode.blockStateChangeLog;

  private static final String QUEUE_REASON_CORRUPT_STATE =
    "it has the wrong state or generation stamp";

  private static final String QUEUE_REASON_FUTURE_GENSTAMP =
    "generation stamp is in the future";

  private final Namesystem namesystem;

  private final DatanodeManager datanodeManager;
  private final HeartbeatManager heartbeatManager;
  private final BlockTokenSecretManager blockTokenSecretManager;

  private final PendingDataNodeMessages pendingDNMessages =
    new PendingDataNodeMessages(); // Standby节点在没有收到edit信息之前，先收到了DN的块汇报，把这些块存到此集合中

  private volatile long pendingReplicationBlocksCount = 0L;
  private volatile long corruptReplicaBlocksCount = 0L;
  private volatile long underReplicatedBlocksCount = 0L;
  private volatile long scheduledReplicationBlocksCount = 0L; // replicationMonitor一次生成的块复制任务数
  private final AtomicLong excessBlocksCount = new AtomicLong(0L);
  private final AtomicLong postponedMisreplicatedBlocksCount = new AtomicLong(0L);
  private final long startupDelayBlockDeletionInMs;
  
  /** Used by metrics */
  public long getPendingReplicationBlocksCount() {
    return pendingReplicationBlocksCount;
  }
  /** Used by metrics */
  public long getUnderReplicatedBlocksCount() {
    return underReplicatedBlocksCount;
  }
  /** Used by metrics */
  public long getCorruptReplicaBlocksCount() {
    return corruptReplicaBlocksCount;
  }
  /** Used by metrics */
  public long getScheduledReplicationBlocksCount() {
    return scheduledReplicationBlocksCount;
  }
  /** Used by metrics */
  public long getPendingDeletionBlocksCount() {
    return invalidateBlocks.numBlocks();
  }
  /** Used by metrics */
  public long getStartupDelayBlockDeletionInMs() {
    return startupDelayBlockDeletionInMs;
  }
  /** Used by metrics */
  public long getExcessBlocksCount() {
    return excessBlocksCount.get();
  }
  /** Used by metrics */
  public long getPostponedMisreplicatedBlocksCount() {
    return postponedMisreplicatedBlocksCount.get();
  }
  /** Used by metrics */
  public int getPendingDataNodeMessageCount() {
    return pendingDNMessages.count();
  }

  /**replicationRecheckInterval is how often namenode checks for new replication work*/
  private final long replicationRecheckInterval;
  
  /**
   * Mapping: Block -> { BlockCollection, datanodes, self ref }
   * Updated only in response to client-sent information.
   */
  final BlocksMap blocksMap;

  /** Replication thread. */
  final Daemon replicationThread = new Daemon(new ReplicationMonitor());
  
  /** Store blocks -> datanodedescriptor(s) map of corrupt replicas */
  final CorruptReplicasMap corruptReplicas = new CorruptReplicasMap(); // 损坏副本的集合

  /** Blocks to be invalidated. */
  private final InvalidateBlocks invalidateBlocks; // 等待删除副本集合
  
  /**
   * After a failover, over-replicated blocks may not be handled
   * until all of the replicas have done a block report to the
   * new active. This is to make sure that this NameNode has been
   * notified of all block deletions that might have been pending
   * when the failover happened.
   */
  // 推迟操作的副本集合
  // 由于副本数多余期望值、损坏等原因，而需要删除的block，NN HA切换时，为避免误删除，先保存在这里

  // 如果一个block中有一个replica所在的dn storage为stale状态，那么这个block不允许执行删除操作，以防止误删除
  // 切主的时候，会把所有DN的storage标记为stale状态
  // 直到DN发送过一次心跳和全量块汇报，才会把此DN的storage的stale状态改为false

  // ReplicationMonitor会周期扫描此集合中的block，并处理
  private final Set<Block> postponedMisreplicatedBlocks = Sets.newHashSet();

  /**
   * Maps a StorageID to the set of blocks that are "extra" for this
   * DataNode. We'll eventually remove these extras.
   */
  public final Map<String, LightWeightLinkedSet<Block>> excessReplicateMap =
    new TreeMap<String, LightWeightLinkedSet<Block>>(); // 副本数多于期望值

  /**
   * Store set of Blocks that need to be replicated 1 or more times.
   * We also store pending replication-orders.
   */
  // 副本数少于期望值，待复制集合
  public final UnderReplicatedBlocks neededReplications = new UnderReplicatedBlocks();

  @VisibleForTesting
  final PendingReplicationBlocks pendingReplications; // 已经生成复制请求的数据块

  /** The maximum number of replicas allowed for a block */
  public final short maxReplication;
  /**
   * The maximum number of outgoing replication streams a given node should have
   * at one time considering all but the highest priority replications needed.
    */
  int maxReplicationStreams;
  /**
   * The maximum number of outgoing replication streams a given node should have
   * at one time.
   */
  int replicationStreamsHardLimit;
  /** Minimum copies needed or else write is disallowed */
  public final short minReplication;
  /** Default number of replicas */
  public final int defaultReplication;
  /** value returned by MAX_CORRUPT_FILES_RETURNED */
  final int maxCorruptFilesReturned;

  final float blocksInvalidateWorkPct;
  final int blocksReplWorkMultiplier;
  
  // whether or not to issue block encryption keys.
  final boolean encryptDataTransfer;
  
  // Max number of blocks to log info about during a block report.
  private final long maxNumBlocksToLog;

  /**
   * When running inside a Standby node, the node may receive block reports
   * from datanodes before receiving the corresponding namespace edits from
   * the active NameNode. Thus, it will postpone them for later processing,
   * instead of marking the blocks as corrupt.
   */
  private boolean shouldPostponeBlocksFromFuture = false;

  /**
   * Process replication queues asynchronously to allow namenode safemode exit
   * and failover to be faster. HDFS-5496
   */
  private Daemon replicationQueuesInitializer = null;
  /**
   * Number of blocks to process asychronously for replication queues
   * initialization once aquired the namesystem lock. Remaining blocks will be
   * processed again after aquiring lock again.
   */
  private int numBlocksPerIteration;
  /**
   * Progress of the Replication queues initialisation.
   */
  private double replicationQueuesInitProgress = 0.0;

  /** for block replicas placement */
  private BlockPlacementPolicy blockplacement;
  private final BlockStoragePolicySuite storagePolicySuite;

  /** Check whether name system is running before terminating */
  private boolean checkNSRunning = true;

  public BlockManager(final Namesystem namesystem, final Configuration conf)
    throws IOException {
    this.namesystem = namesystem;
    datanodeManager = new DatanodeManager(this, namesystem, conf);
    heartbeatManager = datanodeManager.getHeartbeatManager();

    startupDelayBlockDeletionInMs = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_KEY,
        DFSConfigKeys.DFS_NAMENODE_STARTUP_DELAY_BLOCK_DELETION_SEC_DEFAULT) * 1000L;
    invalidateBlocks = new InvalidateBlocks(
        datanodeManager.blockInvalidateLimit, startupDelayBlockDeletionInMs);

    // Compute the map capacity by allocating 2% of total memory
    blocksMap = new BlocksMap(
        LightWeightGSet.computeCapacity(2.0, "BlocksMap"));
    blockplacement = BlockPlacementPolicy.getInstance(
      conf, datanodeManager.getFSClusterStats(),
      datanodeManager.getNetworkTopology(),
      datanodeManager.getHost2DatanodeMap());
    storagePolicySuite = BlockStoragePolicySuite.createDefaultSuite();
    pendingReplications = new PendingReplicationBlocks(conf.getInt(
      DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY,
      DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_DEFAULT) * 1000L);

    blockTokenSecretManager = createBlockTokenSecretManager(conf);

    this.maxCorruptFilesReturned = conf.getInt(
      DFSConfigKeys.DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED_KEY,
      DFSConfigKeys.DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED);
    this.defaultReplication = conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 
                                          DFSConfigKeys.DFS_REPLICATION_DEFAULT);

    final int maxR = conf.getInt(DFSConfigKeys.DFS_REPLICATION_MAX_KEY, 
                                 DFSConfigKeys.DFS_REPLICATION_MAX_DEFAULT);
    final int minR = conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY,
                                 DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT);
    if (minR <= 0)
      throw new IOException("Unexpected configuration parameters: "
          + DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY
          + " = " + minR + " <= 0");
    if (maxR > Short.MAX_VALUE)
      throw new IOException("Unexpected configuration parameters: "
          + DFSConfigKeys.DFS_REPLICATION_MAX_KEY
          + " = " + maxR + " > " + Short.MAX_VALUE);
    if (minR > maxR)
      throw new IOException("Unexpected configuration parameters: "
          + DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY
          + " = " + minR + " > "
          + DFSConfigKeys.DFS_REPLICATION_MAX_KEY
          + " = " + maxR);
    this.minReplication = (short)minR;
    this.maxReplication = (short)maxR;

    this.maxReplicationStreams =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY,
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_DEFAULT);
    this.replicationStreamsHardLimit =
        conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY,
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_DEFAULT);

    this.blocksInvalidateWorkPct = DFSUtil.getInvalidateWorkPctPerIteration(conf);
    this.blocksReplWorkMultiplier = DFSUtil.getReplWorkMultiplier(conf);

    this.replicationRecheckInterval = 
      conf.getInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 
                  DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT) * 1000L;
    
    this.encryptDataTransfer =
        conf.getBoolean(DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY,
            DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_DEFAULT);
    
    this.maxNumBlocksToLog =
        conf.getLong(DFSConfigKeys.DFS_MAX_NUM_BLOCKS_TO_LOG_KEY,
            DFSConfigKeys.DFS_MAX_NUM_BLOCKS_TO_LOG_DEFAULT);
    this.numBlocksPerIteration = conf.getInt(
        DFSConfigKeys.DFS_BLOCK_MISREPLICATION_PROCESSING_LIMIT,
        DFSConfigKeys.DFS_BLOCK_MISREPLICATION_PROCESSING_LIMIT_DEFAULT);
    
    LOG.info("defaultReplication         = " + defaultReplication);
    LOG.info("maxReplication             = " + maxReplication);
    LOG.info("minReplication             = " + minReplication);
    LOG.info("maxReplicationStreams      = " + maxReplicationStreams);
    LOG.info("replicationRecheckInterval = " + replicationRecheckInterval);
    LOG.info("encryptDataTransfer        = " + encryptDataTransfer);
    LOG.info("maxNumBlocksToLog          = " + maxNumBlocksToLog);
  }

  private static BlockTokenSecretManager createBlockTokenSecretManager(
      final Configuration conf) {
    final boolean isEnabled = conf.getBoolean( // 默认情况下，BlockTokenSecretManager是关闭的
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, 
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_DEFAULT);
    LOG.info(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY + "=" + isEnabled);

    if (!isEnabled) {
      if (UserGroupInformation.isSecurityEnabled()) {
        LOG.error("Security is enabled but block access tokens " +
            "(via " + DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY + ") " +
            "aren't enabled. This may cause issues " +
            "when clients attempt to talk to a DataNode.");
      }
      return null;
    }

    final long updateMin = conf.getLong(
        DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY, 
        DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_DEFAULT);
    final long lifetimeMin = conf.getLong(
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, 
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_DEFAULT);
    final String encryptionAlgorithm = conf.get(
        DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY);
    LOG.info(DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY
        + "=" + updateMin + " min(s), "
        + DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY
        + "=" + lifetimeMin + " min(s), "
        + DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY
        + "=" + encryptionAlgorithm);
    
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    boolean isHaEnabled = HAUtil.isHAEnabled(conf, nsId);

    if (isHaEnabled) {
      String thisNnId = HAUtil.getNameNodeId(conf, nsId);
      String otherNnId = HAUtil.getNameNodeIdOfOtherNode(conf, nsId);
      return new BlockTokenSecretManager(updateMin*60*1000L,
          lifetimeMin*60*1000L, thisNnId.compareTo(otherNnId) < 0 ? 0 : 1, null,
          encryptionAlgorithm);
    } else {
      return new BlockTokenSecretManager(updateMin*60*1000L,
          lifetimeMin*60*1000L, 0, null, encryptionAlgorithm);
    }
  }

  public BlockStoragePolicy getStoragePolicy(final String policyName) {
    return storagePolicySuite.getPolicy(policyName);
  }

  public BlockStoragePolicy getStoragePolicy(final byte policyId) {
    return storagePolicySuite.getPolicy(policyId);
  }

  public BlockStoragePolicy[] getStoragePolicies() {
    return storagePolicySuite.getAllPolicies();
  }

  public void setBlockPoolId(String blockPoolId) {
    if (isBlockTokenEnabled()) {
      blockTokenSecretManager.setBlockPoolId(blockPoolId);
    }
  }

  public BlockStoragePolicySuite getStoragePolicySuite() {
    return storagePolicySuite;
  }

  /** get the BlockTokenSecretManager */
  @VisibleForTesting
  public BlockTokenSecretManager getBlockTokenSecretManager() {
    return blockTokenSecretManager;
  }

  /** Allow silent termination of replication monitor for testing */
  @VisibleForTesting
  void enableRMTerminationForTesting() {
    checkNSRunning = false;
  }

  private boolean isBlockTokenEnabled() {
    return blockTokenSecretManager != null;
  }

  /** Should the access keys be updated? */
  boolean shouldUpdateBlockKey(final long updateTime) throws IOException {
    return isBlockTokenEnabled()? blockTokenSecretManager.updateKeys(updateTime)
        : false;
  }

  public void activate(Configuration conf) {
    pendingReplications.start();
    datanodeManager.activate(conf);
    this.replicationThread.start();
  }

  public void close() {
    try {
      replicationThread.interrupt();
      replicationThread.join(3000);
    } catch (InterruptedException ie) {
    }
    datanodeManager.close();
    pendingReplications.stop();
    blocksMap.close();
  }

  /** @return the datanodeManager */
  public DatanodeManager getDatanodeManager() {
    return datanodeManager;
  }

  @VisibleForTesting
  public BlockPlacementPolicy getBlockPlacementPolicy() {
    return blockplacement;
  }

  /** Set BlockPlacementPolicy */
  public void setBlockPlacementPolicy(BlockPlacementPolicy newpolicy) {
    if (newpolicy == null) {
      throw new HadoopIllegalArgumentException("newpolicy == null");
    }
    this.blockplacement = newpolicy;
  }

  /** Dump meta data to out. */
  public void metaSave(PrintWriter out) {
    assert namesystem.hasWriteLock();
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    datanodeManager.fetchDatanodes(live, dead, false);
    out.println("Live Datanodes: " + live.size());
    out.println("Dead Datanodes: " + dead.size());
    //
    // Dump contents of neededReplication
    //
    synchronized (neededReplications) {
      out.println("Metasave: Blocks waiting for replication: " + 
                  neededReplications.size());
      for (Block block : neededReplications) {
        dumpBlockMeta(block, out);
      }
    }
    
    // Dump any postponed over-replicated blocks
    out.println("Mis-replicated blocks that have been postponed:");
    for (Block block : postponedMisreplicatedBlocks) {
      dumpBlockMeta(block, out);
    }

    // Dump blocks from pendingReplication
    pendingReplications.metaSave(out);

    // Dump blocks that are waiting to be deleted
    invalidateBlocks.dump(out);

    // Dump all datanodes
    getDatanodeManager().datanodeDump(out);
  }
  
  /**
   * Dump the metadata for the given block in a human-readable
   * form.
   */
  private void dumpBlockMeta(Block block, PrintWriter out) {
    List<DatanodeDescriptor> containingNodes =
                                      new ArrayList<DatanodeDescriptor>();
    List<DatanodeStorageInfo> containingLiveReplicasNodes =
      new ArrayList<DatanodeStorageInfo>();
    
    NumberReplicas numReplicas = new NumberReplicas();
    // source node returned is not used
    chooseSourceDatanode(block, containingNodes,
        containingLiveReplicasNodes, numReplicas,
        UnderReplicatedBlocks.LEVEL);
    
    // containingLiveReplicasNodes can include READ_ONLY_SHARED replicas which are 
    // not included in the numReplicas.liveReplicas() count
    assert containingLiveReplicasNodes.size() >= numReplicas.liveReplicas();
    int usableReplicas = numReplicas.liveReplicas() +
                         numReplicas.decommissionedReplicas();
    
    if (block instanceof BlockInfoContiguous) {
      BlockCollection bc = ((BlockInfoContiguous) block).getBlockCollection();
      String fileName = (bc == null) ? "[orphaned]" : bc.getName();
      out.print(fileName + ": ");
    }
    // l: == live:, d: == decommissioned c: == corrupt e: == excess
    out.print(block + ((usableReplicas > 0)? "" : " MISSING") + 
              " (replicas:" +
              " l: " + numReplicas.liveReplicas() +
              " d: " + numReplicas.decommissionedReplicas() +
              " c: " + numReplicas.corruptReplicas() +
              " e: " + numReplicas.excessReplicas() + ") "); 

    Collection<DatanodeDescriptor> corruptNodes = 
                                  corruptReplicas.getNodes(block);
    
    for (DatanodeStorageInfo storage : blocksMap.getStorages(block)) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      String state = "";
      if (corruptNodes != null && corruptNodes.contains(node)) {
        state = "(corrupt)";
      } else if (node.isDecommissioned() || 
          node.isDecommissionInProgress()) {
        state = "(decommissioned)";
      }
      
      if (storage.areBlockContentsStale()) {
        state += " (block deletions maybe out of date)";
      }
      out.print(" " + node + state + " : ");
    }
    out.println("");
  }

  /** @return maxReplicationStreams */
  public int getMaxReplicationStreams() {
    return maxReplicationStreams;
  }

  /**
   * @return true if the block has minimum replicas
   */
  public boolean checkMinReplication(Block block) {
    return (countNodes(block).liveReplicas() >= minReplication);
  }

  /**
   * Commit a block of a file
   * 
   * @param block block to be committed
   * @param commitBlock - contains client reported block length and generation
   * @return true if the block is changed to committed state.
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  // block：分配时（addBlock、recovery等），NN记录的block信息
  // commitBlock：client提交的待commit的block信息
  private static boolean commitBlock(
      final BlockInfoContiguousUnderConstruction block, final Block commitBlock)
      throws IOException {
    if (block.getBlockUCState() == BlockUCState.COMMITTED)
      return false;
    assert block.getNumBytes() <= commitBlock.getNumBytes() : // 检查block的size
      "commitBlock length is less than the stored one "
      + commitBlock.getNumBytes() + " vs. " + block.getNumBytes();
    if(block.getGenerationStamp() != commitBlock.getGenerationStamp()) { // 检查block的GS
      throw new IOException("Commit block with mismatching GS. NN has " +
        block + ", client submits " + commitBlock);
    }
    block.commitBlock(commitBlock); // commit一个Block
    return true;
  }
  
  /**
   * Commit the last block of the file and mark it as complete if it has
   * meets the minimum replication requirement
   * 
   * @param bc block collection
   * @param commitBlock - contains client reported block length and generation
   * @return true if the last block is changed to committed state.
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  public boolean commitOrCompleteLastBlock(BlockCollection bc,
      Block commitBlock) throws IOException { // commit一个Block，如果达到最小副本数，则complete这个block
    if(commitBlock == null) // client上报的last block
      return false; // not committing, this is a block allocation retry
    BlockInfoContiguous lastBlock = bc.getLastBlock(); // NN记录的last block
    if(lastBlock == null)
      return false; // no blocks in file yet
    if(lastBlock.isComplete())
      return false; // already completed (e.g. by syncBlock)
    
    final boolean b = commitBlock(
        (BlockInfoContiguousUnderConstruction) lastBlock, commitBlock); // commit一个Block
    if(countNodes(lastBlock).liveReplicas() >= minReplication) // 是否达到最小副本数
      completeBlock(bc, bc.numBlocks()-1, false); // complete一个Block
    return b;
  }

  /**
   * Convert a specified block of the file to a complete block.
   * @param bc file
   * @param blkIndex  block index in the file
   * @throws IOException if the block does not have at least a minimal number
   * of replicas reported from data-nodes.
   */
  // complete下标为blkIndex的block
  // 只有complete的时候，才会把block从UC状态转为正常状态
  // addBlock、complete file、block report、commitBlockSynchronization(block recovery之后)，都会判断是否可以complete block
  private BlockInfoContiguous completeBlock(final BlockCollection bc,
      final int blkIndex, boolean force) throws IOException {
    if(blkIndex < 0)
      return null;
    BlockInfoContiguous curBlock = bc.getBlocks()[blkIndex];
    if(curBlock.isComplete()) //如果已经complete，直接返回
      return curBlock;
    // 检查是否满足complete条件：
    // 1、满足最小副本数
    // 2、block状态为COMMITTED
    BlockInfoContiguousUnderConstruction ucBlock =
        (BlockInfoContiguousUnderConstruction) curBlock;
    int numNodes = ucBlock.numNodes();
    if (!force && numNodes < minReplication)
      throw new IOException("Cannot complete block: " +
          "block does not satisfy minimal replication requirement.");
    if(!force && ucBlock.getBlockUCState() != BlockUCState.COMMITTED)
      throw new IOException(
          "Cannot complete block: block has not been COMMITTED by the client");
    BlockInfoContiguous completeBlock = ucBlock.convertToCompleteBlock();
    // replace penultimate block in file
    bc.setBlock(blkIndex, completeBlock);
    
    // Since safe-mode only counts complete blocks, and we now have
    // one more complete block, we need to adjust the total up, and
    // also count it as safe, if we have at least the minimum replica
    // count. (We may not have the minimum replica count yet if this is
    // a "forced" completion when a file is getting closed by an
    // OP_CLOSE edit on the standby).
    namesystem.adjustSafeModeBlockTotals(0, 1); // 安全模式相关
    namesystem.incrementSafeBlockCount(
        Math.min(numNodes, minReplication));
    
    // replace block in the blocksMap
    return blocksMap.replaceBlock(completeBlock); // 更新此Block信息
  }

  private BlockInfoContiguous completeBlock(final BlockCollection bc,
      final BlockInfoContiguous block, boolean force) throws IOException {
    BlockInfoContiguous[] fileBlocks = bc.getBlocks();
    for(int idx = 0; idx < fileBlocks.length; idx++)
      if(fileBlocks[idx] == block) {
        return completeBlock(bc, idx, force);
      }
    return block;
  }
  
  /**
   * Force the given block in the given file to be marked as complete,
   * regardless of whether enough replicas are present. This is necessary
   * when tailing edit logs as a Standby.
   */
  // 强制commit、complete一个block
  public BlockInfoContiguous forceCompleteBlock(final BlockCollection bc,
      final BlockInfoContiguousUnderConstruction block) throws IOException {
    block.commitBlock(block);
    return completeBlock(bc, block, true);
  }

  
  /**
   * Convert the last block of the file to an under construction block.<p>
   * The block is converted only if the file has blocks and the last one
   * is a partial block (its size is less than the preferred block size).
   * The converted block is returned to the client.
   * The client uses the returned block locations to form the data pipeline
   * for this block.<br>
   * The methods returns null if there is no partial block at the end.
   * The client is supposed to allocate a new block with the next call.
   *
   * @param bc file
   * @param bytesToRemove num of bytes to remove from block
   * @return the last block locations if the block is partial or null otherwise
   */
  public LocatedBlock convertLastBlockToUnderConstruction(
      BlockCollection bc, long bytesToRemove) throws IOException { // 如果最后一个块没写满，则返回最后一个块，否则返回null
    BlockInfoContiguous oldBlock = bc.getLastBlock();
    if(oldBlock == null ||
       bc.getPreferredBlockSize() == oldBlock.getNumBytes() - bytesToRemove) // 说明最后一个块写满了？
      return null;
    assert oldBlock == getStoredBlock(oldBlock) :
      "last block of the file is not in blocksMap";

    DatanodeStorageInfo[] targets = getStorages(oldBlock); // 获取所有副本信息

    BlockInfoContiguousUnderConstruction ucBlock =
        bc.setLastBlock(oldBlock, targets);
    blocksMap.replaceBlock(ucBlock); // 更新Block信息

    // Remove block from replication queue.
    NumberReplicas replicas = countNodes(ucBlock); // 计算副本个数
    neededReplications.remove(ucBlock, replicas.liveReplicas(),
        replicas.decommissionedReplicas(), getReplication(ucBlock));  // 从 需要补充副本 队列中移除
    pendingReplications.remove(ucBlock); // 从 等待复制 队列中移除

    // remove this block from the list of pending blocks to be deleted. 
    for (DatanodeStorageInfo storage : targets) {
      invalidateBlocks.remove(storage.getDatanodeDescriptor(), oldBlock); // 从 等待删除 队列中移除
    }
    
    // Adjust safe-mode totals, since under-construction blocks don't
    // count in safe-mode.
    namesystem.adjustSafeModeBlockTotals( // UC状态的block，不计入blockSafe和blockTotal
        // decrement safe if we had enough
        targets.length >= minReplication ? -1 : 0,
        // always decrement total blocks
        -1);

    final long fileLength = bc.computeContentSummary(getStoragePolicySuite()).getLength(); // 文件的长度
    final long pos = fileLength - ucBlock.getNumBytes(); // 从pos位置开始，追加写文件
    return createLocatedBlock(ucBlock, pos, AccessMode.WRITE);
  }

  /**
   * Get all valid locations of the block
   */
  private List<DatanodeStorageInfo> getValidLocations(Block block) {
    final List<DatanodeStorageInfo> locations
        = new ArrayList<DatanodeStorageInfo>(blocksMap.numNodes(block));
    for(DatanodeStorageInfo storage : blocksMap.getStorages(block)) {
      // filter invalidate replicas
      if(!invalidateBlocks.contains(storage.getDatanodeDescriptor(), block)) {
        locations.add(storage);
      }
    }
    return locations;
  }
  
  private List<LocatedBlock> createLocatedBlockList(
      final BlockInfoContiguous[] blocks,
      final long offset, final long length, final int nrBlocksToReturn,
      final AccessMode mode) throws IOException { // 根据offset和length，返回相应的block
    int curBlk = 0; // client需要的offset落在第几个block上
    long curPos = 0; // offset of the first byte of the block in the file 此block在文件中的offset
    long blkSize = 0; // block size
    int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length; // block个数
    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
      blkSize = blocks[curBlk].getNumBytes();
      assert blkSize > 0 : "Block of size 0";
      if (curPos + blkSize > offset) {
        break;
      }
      curPos += blkSize;
    }

    if (nrBlocks > 0 && curBlk == nrBlocks)   // offset >= end of file
      return Collections.<LocatedBlock>emptyList();

    long endOff = offset + length;
    List<LocatedBlock> results = new ArrayList<LocatedBlock>(blocks.length);
    do {
      results.add(createLocatedBlock(blocks[curBlk], curPos, mode));
      curPos += blocks[curBlk].getNumBytes();
      curBlk++;
    } while (curPos < endOff // client需要的length
          && curBlk < blocks.length
          && results.size() < nrBlocksToReturn);
    return results;
  }

  private LocatedBlock createLocatedBlock(final BlockInfoContiguous[] blocks,
      final long endPos, final AccessMode mode) throws IOException {
    int curBlk = 0;
    long curPos = 0;
    int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
      long blkSize = blocks[curBlk].getNumBytes();
      if (curPos + blkSize >= endPos) {
        break;
      }
      curPos += blkSize;
    }
    
    return createLocatedBlock(blocks[curBlk], curPos, mode);
  }
  
  private LocatedBlock createLocatedBlock(final BlockInfoContiguous blk, final long pos,
    final BlockTokenSecretManager.AccessMode mode) throws IOException {
    final LocatedBlock lb = createLocatedBlock(blk, pos);
    if (mode != null) {
      setBlockToken(lb, mode);
    }
    return lb;
  }

  /** @return a LocatedBlock for the given block */
  // 创建返回给client的block对象
  // 1、正在构建中的block，返回所有的副本
  // 2、完成的block，如果所有的副本都损坏了，则返回所有的副本；否则返回未损坏的副本
  private LocatedBlock createLocatedBlock(final BlockInfoContiguous blk, final long pos
      ) throws IOException {
    if (blk instanceof BlockInfoContiguousUnderConstruction) { // UC状态的block
      if (blk.isComplete()) {
        throw new IOException(
            "blk instanceof BlockInfoUnderConstruction && blk.isComplete()"
            + ", blk=" + blk);
      }
      final BlockInfoContiguousUnderConstruction uc =
          (BlockInfoContiguousUnderConstruction) blk;
      final DatanodeStorageInfo[] storages = uc.getExpectedStorageLocations();
      final ExtendedBlock eb = new ExtendedBlock(namesystem.getBlockPoolId(), blk);
      return new LocatedBlock(eb, storages, pos, false);
    }

    // get block locations
    final int numCorruptNodes = countNodes(blk).corruptReplicas(); // 损坏的副本个数
    final int numCorruptReplicas = corruptReplicas.numCorruptReplicas(blk);
    if (numCorruptNodes != numCorruptReplicas) {
      LOG.warn("Inconsistent number of corrupt replicas for "
          + blk + " blockMap has " + numCorruptNodes
          + " but corrupt replicas map has " + numCorruptReplicas);
    }

    final int numNodes = blocksMap.numNodes(blk); // 实际副本数
    final boolean isCorrupt = numCorruptNodes == numNodes; // 是否所有的副本都损坏
    final int numMachines = isCorrupt ? numNodes: numNodes - numCorruptNodes;
    final DatanodeStorageInfo[] machines = new DatanodeStorageInfo[numMachines]; // 未损坏的副本
    int j = 0;
    if (numMachines > 0) { // 所有的副本都损坏时，返回所有的副本；否则返回未损坏的副本
      for(DatanodeStorageInfo storage : blocksMap.getStorages(blk)) {
        final DatanodeDescriptor d = storage.getDatanodeDescriptor();
        final boolean replicaCorrupt = corruptReplicas.isReplicaCorrupt(blk, d);
        if (isCorrupt || (!replicaCorrupt))
          machines[j++] = storage;
      }
    }
    assert j == machines.length :
      "isCorrupt: " + isCorrupt + 
      " numMachines: " + numMachines +
      " numNodes: " + numNodes +
      " numCorrupt: " + numCorruptNodes +
      " numCorruptRepls: " + numCorruptReplicas;
    final ExtendedBlock eb = new ExtendedBlock(namesystem.getBlockPoolId(), blk);
    return new LocatedBlock(eb, machines, pos, isCorrupt);
  }

  /** Create a LocatedBlocks. */
  public LocatedBlocks createLocatedBlocks(final BlockInfoContiguous[] blocks,
      final long fileSizeExcludeBlocksUnderConstruction,
      final boolean isFileUnderConstruction, final long offset,
      final long length, final boolean needBlockToken,
      final boolean inSnapshot, FileEncryptionInfo feInfo)
      throws IOException {
    assert namesystem.hasReadLock();
    if (blocks == null) {
      return null;
    } else if (blocks.length == 0) { // 此文件没有block
      return new LocatedBlocks(0, isFileUnderConstruction,
          Collections.<LocatedBlock>emptyList(), null, false, feInfo);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("blocks = " + java.util.Arrays.asList(blocks));
      }
      final AccessMode mode = needBlockToken? AccessMode.READ: null;
      final List<LocatedBlock> locatedblocks = createLocatedBlockList(
          blocks, offset, length, Integer.MAX_VALUE, mode); // 根据offset和length，返回相应的block

      final LocatedBlock lastlb; // 最后一个block
      final boolean isComplete;
      if (!inSnapshot) {
        final BlockInfoContiguous last = blocks[blocks.length - 1];
        final long lastPos = last.isComplete()?
            fileSizeExcludeBlocksUnderConstruction - last.getNumBytes()
            : fileSizeExcludeBlocksUnderConstruction;
        lastlb = createLocatedBlock(last, lastPos, mode);
        isComplete = last.isComplete();
      } else {
        lastlb = createLocatedBlock(blocks,
            fileSizeExcludeBlocksUnderConstruction, mode);
        isComplete = true;
      }
      return new LocatedBlocks(
          fileSizeExcludeBlocksUnderConstruction, isFileUnderConstruction,
          locatedblocks, lastlb, isComplete, feInfo);
    }
  }

  /** @return current access keys. */
  public ExportedBlockKeys getBlockKeys() {
    return isBlockTokenEnabled()? blockTokenSecretManager.exportKeys()
        : ExportedBlockKeys.DUMMY_KEYS;
  }

  /** Generate a block token for the located block. */
  public void setBlockToken(final LocatedBlock b,
      final BlockTokenSecretManager.AccessMode mode) throws IOException {
    if (isBlockTokenEnabled()) {
      // Use cached UGI if serving RPC calls.
      b.setBlockToken(blockTokenSecretManager.generateToken(
          NameNode.getRemoteUser().getShortUserName(),
          b.getBlock(), EnumSet.of(mode)));
    }    
  }

  void addKeyUpdateCommand(final List<DatanodeCommand> cmds,
      final DatanodeDescriptor nodeinfo) {
    // check access key update
    if (isBlockTokenEnabled() && nodeinfo.needKeyUpdate) {
      cmds.add(new KeyUpdateCommand(blockTokenSecretManager.exportKeys()));
      nodeinfo.needKeyUpdate = false;
    }
  }
  
  public DataEncryptionKey generateDataEncryptionKey() {
    if (isBlockTokenEnabled() && encryptDataTransfer) {
      return blockTokenSecretManager.generateDataEncryptionKey();
    } else {
      return null;
    }
  }

  /**
   * Clamp the specified replication between the minimum and the maximum
   * replication levels.
   */
  public short adjustReplication(short replication) {
    return replication < minReplication? minReplication
        : replication > maxReplication? maxReplication: replication;
  }

  /**
   * Check whether the replication parameter is within the range
   * determined by system configuration.
   */
   public void verifyReplication(String src,
                          short replication,
                          String clientName) throws IOException { // 副本数是否在有效范围内

    if (replication >= minReplication && replication <= maxReplication) {
      //common case. avoid building 'text'
      return;
    }
    
    String text = "file " + src 
      + ((clientName != null) ? " on client " + clientName : "")
      + ".\n"
      + "Requested replication " + replication;

    if (replication > maxReplication)
      throw new IOException(text + " exceeds maximum " + maxReplication);

    if (replication < minReplication)
      throw new IOException(text + " is less than the required minimum " +
                            minReplication);
  }

  /**
   * Check if a block is replicated to at least the minimum replication.
   */
  public boolean isSufficientlyReplicated(BlockInfoContiguous b) {
    // Compare against the lesser of the minReplication and number of live DNs.
    final int replication =
        Math.min(minReplication, getDatanodeManager().getNumLiveDataNodes());
    return countNodes(b).liveReplicas() >= replication;
  }

  /**
   * return a list of blocks & their locations on <code>datanode</code> whose
   * total size is <code>size</code>
   * 
   * @param datanode on which blocks are located
   * @param size total size of blocks
   */
  public BlocksWithLocations getBlocks(DatanodeID datanode, long size
      ) throws IOException {
    namesystem.checkOperation(OperationCategory.READ);
    namesystem.readLock();
    try {
      namesystem.checkOperation(OperationCategory.READ);
      return getBlocksWithLocations(datanode, size);  
    } finally {
      namesystem.readUnlock();
    }
  }

  /** Get all blocks with location information from a datanode. */
  private BlocksWithLocations getBlocksWithLocations(final DatanodeID datanode,
      final long size) throws UnregisteredNodeException {
    final DatanodeDescriptor node = getDatanodeManager().getDatanode(datanode);
    if (node == null) {
      blockLog.warn("BLOCK* getBlocks: Asking for blocks from an" +
          " unrecorded node {}", datanode);
      throw new HadoopIllegalArgumentException(
          "Datanode " + datanode + " not found.");
    }

    int numBlocks = node.numBlocks();
    if(numBlocks == 0) {
      return new BlocksWithLocations(new BlockWithLocations[0]);
    }
    Iterator<BlockInfoContiguous> iter = node.getBlockIterator();
    int startBlock = DFSUtil.getRandom().nextInt(numBlocks); // starting from a random block
    // skip blocks
    for(int i=0; i<startBlock; i++) {
      iter.next();
    }
    List<BlockWithLocations> results = new ArrayList<BlockWithLocations>();
    long totalSize = 0;
    BlockInfoContiguous curBlock;
    while(totalSize<size && iter.hasNext()) {
      curBlock = iter.next();
      if(!curBlock.isComplete())  continue;
      totalSize += addBlock(curBlock, results);
    }
    if(totalSize<size) {
      iter = node.getBlockIterator(); // start from the beginning
      for(int i=0; i<startBlock&&totalSize<size; i++) {
        curBlock = iter.next();
        if(!curBlock.isComplete())  continue;
        totalSize += addBlock(curBlock, results);
      }
    }

    return new BlocksWithLocations(
        results.toArray(new BlockWithLocations[results.size()]));
  }

   
  /** Remove the blocks associated to the given datanode. */
  void removeBlocksAssociatedTo(final DatanodeDescriptor node) { // 从NN内存中，移除此DN的所有block信息
    final Iterator<? extends Block> it = node.getBlockIterator();
    while(it.hasNext()) { // 移除此DN的所有block信息
      removeStoredBlock(it.next(), node);
    }
    // Remove all pending DN messages referencing this DN.
    pendingDNMessages.removeAllMessagesForDatanode(node);

    node.resetBlocks();
    invalidateBlocks.remove(node);
  }

  /** Remove the blocks associated to the given DatanodeStorageInfo. */
  void removeBlocksAssociatedTo(final DatanodeStorageInfo storageInfo) { // 从NN内存中，移除此Storage的所有block信息
    assert namesystem.hasWriteLock();
    final Iterator<? extends Block> it = storageInfo.getBlockIterator();
    DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();
    while(it.hasNext()) { // 移除此Storage的所有block信息
      Block block = it.next();
      removeStoredBlock(block, node);
      invalidateBlocks.remove(node, block);
    }
    namesystem.checkSafeMode();
  }

  /**
   * Adds block to list of blocks which will be invalidated on specified
   * datanode and log the operation
   */
  void addToInvalidates(final Block block, final DatanodeInfo datanode) {
    if (!namesystem.isPopulatingReplQueues()) {
      return;
    }
    invalidateBlocks.add(block, datanode, true); // 添加到待删除集合
  }

  /**
   * Adds block to list of blocks which will be invalidated on all its
   * datanodes.
   */
  private void addToInvalidates(Block b) { // 添加到invalidate队列
    if (!namesystem.isPopulatingReplQueues()) {
      return;
    }
    StringBuilder datanodes = new StringBuilder();
    for(DatanodeStorageInfo storage : blocksMap.getStorages(b, State.NORMAL)) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      invalidateBlocks.add(b, node, false);
      datanodes.append(node).append(" ");
    }
    if (datanodes.length() != 0) {
      blockLog.info("BLOCK* addToInvalidates: {} {}", b, datanodes.toString());
    }
  }

  /**
   * Remove all block invalidation tasks under this datanode UUID;
   * used when a datanode registers with a new UUID and the old one
   * is wiped.
   */
  void removeFromInvalidates(final DatanodeInfo datanode) {
    if (!namesystem.isPopulatingReplQueues()) {
      return;
    }
    invalidateBlocks.remove(datanode);
  }

  /**
   * Mark the block belonging to datanode as corrupt
   * @param blk Block to be marked as corrupt
   * @param dn Datanode which holds the corrupt replica
   * @param storageID if known, null otherwise.
   * @param reason a textual reason why the block should be marked corrupt,
   * for logging purposes
   */
  public void findAndMarkBlockAsCorrupt(final ExtendedBlock blk,
      final DatanodeInfo dn, String storageID, String reason) throws IOException {
    assert namesystem.hasWriteLock();
    final BlockInfoContiguous storedBlock = getStoredBlock(blk.getLocalBlock());// 通过blocksMap获取BlockInfoContiguous对象
    if (storedBlock == null) {
      // Check if the replica is in the blockMap, if not
      // ignore the request for now. This could happen when BlockScanner
      // thread of Datanode reports bad block before Block reports are sent
      // by the Datanode on startup
      blockLog.info("BLOCK* findAndMarkBlockAsCorrupt: {} not found", blk);
      return;
    }

    DatanodeDescriptor node = getDatanodeManager().getDatanode(dn);// 通过datanodeMap获取DatanodeDescriptor
    if (node == null) {
      throw new IOException("Cannot mark " + blk
          + " as corrupt because datanode " + dn + " (" + dn.getDatanodeUuid()
          + ") does not exist");
    }
    
    markBlockAsCorrupt(new BlockToMarkCorrupt(storedBlock,
            blk.getGenerationStamp(), reason, Reason.CORRUPTION_REPORTED),
        storageID == null ? null : node.getStorageInfo(storageID),
        node);
  }

  /**
   * 
   * @param b
   * @param storageInfo storage that contains the block, if known. null otherwise.
   * @throws IOException
   */
  // 当发现损坏副本的时候，需要先复制，再删除
  private void markBlockAsCorrupt(BlockToMarkCorrupt b,
      DatanodeStorageInfo storageInfo,
      DatanodeDescriptor node) throws IOException {

    BlockCollection bc = b.corrupted.getBlockCollection();
    if (bc == null) { // inode == null，说明此block不属于任何文件
      blockLog.info("BLOCK markBlockAsCorrupt: {} cannot be marked as" +
          " corrupt as it does not belong to any file", b);
      addToInvalidates(b.corrupted, node); // 添加到待删除队列
      return;
    } 

    // Add replica to the data-node if it is not already there
    if (storageInfo != null) {
      storageInfo.addBlock(b.stored);
    }

    // Add this replica to corruptReplicas Map
    corruptReplicas.addToCorruptReplicasMap(b.corrupted, node, b.reason,
        b.reasonCode); // 添加到corruptReplicas集合中

    NumberReplicas numberOfReplicas = countNodes(b.stored); // 统计各种情况的副本个数
    boolean hasEnoughLiveReplicas = numberOfReplicas.liveReplicas() >= bc // 副本数达到期望副本数
        .getBlockReplication();
    boolean minReplicationSatisfied =
        numberOfReplicas.liveReplicas() >= minReplication; // 正常的副本满足最小副本数
    boolean hasMoreCorruptReplicas = minReplicationSatisfied && // 删除损坏的副本之后，正常的副本 是否充足
        (numberOfReplicas.liveReplicas() + numberOfReplicas.corruptReplicas()) >
        bc.getBlockReplication();
    boolean corruptedDuringWrite = minReplicationSatisfied && // 发现 正在写的副本 是损坏的
        (b.stored.getGenerationStamp() > b.corrupted.getGenerationStamp());
    // case 1: have enough number of live replicas
    // case 2: corrupted replicas + live replicas > Replication factor
    // case 3: Block is marked corrupt due to failure while writing. In this
    //         case genstamp will be different than that of valid block.
    // In all these cases we can delete the replica.
    // In case of 3, rbw block will be deleted and valid block can be replicated
    // 下面的逻辑说明：当发现损坏副本的时候，需要先复制，再删除
    if (hasEnoughLiveReplicas || hasMoreCorruptReplicas
        || corruptedDuringWrite) {
      // the block is over-replicated so invalidate the replicas immediately
      invalidateBlock(b, node); // 副本数充足，添加到待删除集合
    } else if (namesystem.isPopulatingReplQueues()) {
      // add the block to neededReplication
      updateNeededReplications(b.stored, -1, 0); // 添加到待复制集合
    }
  }

  /**
   * Invalidates the given block on the given datanode.
   * @return true if the block was successfully invalidated and no longer
   * present in the BlocksMap
   */
  private boolean invalidateBlock(BlockToMarkCorrupt b, DatanodeInfo dn
      ) throws IOException { // 添加到待删除集合，并从NN内存中移除此副本信息
    blockLog.info("BLOCK* invalidateBlock: {} on {}", b, dn);
    DatanodeDescriptor node = getDatanodeManager().getDatanode(dn);
    if (node == null) {
      throw new IOException("Cannot invalidate " + b
          + " because datanode " + dn + " does not exist.");
    }

    // Check how many copies we have of the block
    NumberReplicas nr = countNodes(b.stored);
    if (nr.replicasOnStaleNodes() > 0) {
      blockLog.info("BLOCK* invalidateBlocks: postponing " +
          "invalidation of {} on {} because {} replica(s) are located on " +
          "nodes with potentially out-of-date block reports", b, dn,
          nr.replicasOnStaleNodes());
      postponeBlock(b.corrupted); // 如果有在过期节点上的副本，那么延迟处理损坏块
      return false;
    } else if (nr.liveReplicas() >= 1) {
      // If we have at least one copy on a live node, then we can delete it.
      addToInvalidates(b.corrupted, dn); // 添加到待删除集合
      removeStoredBlock(b.stored, node); // 从NN内存中移除此副本信息
      blockLog.debug("BLOCK* invalidateBlocks: {} on {} listed for deletion.",
          b, dn);
      return true;
    } else { // 只剩一个副本，就算是坏的，也不能删除
      blockLog.info("BLOCK* invalidateBlocks: {} on {} is the only copy and" +
          " was not deleted", b, dn);
      return false;
    }
  }


  public void setPostponeBlocksFromFuture(boolean postpone) { // StandbyNN为true，ActiveNN为false
    this.shouldPostponeBlocksFromFuture  = postpone;
  }


  private void postponeBlock(Block blk) { // 添加到推迟操作副本集合
    if (postponedMisreplicatedBlocks.add(blk)) {
      postponedMisreplicatedBlocksCount.incrementAndGet();
    }
  }
  
  
  void updateState() { // 更新metric值
    pendingReplicationBlocksCount = pendingReplications.size();
    underReplicatedBlocksCount = neededReplications.size();
    corruptReplicaBlocksCount = corruptReplicas.size();
  }

  /** Return number of under-replicated but not missing blocks */
  public int getUnderReplicatedNotMissingBlocks() {
    return neededReplications.getUnderReplicatedBlockCount();
  }
  
  /**
   * Schedule blocks for deletion at datanodes
   * @param nodesToProcess number of datanodes to schedule deletion work
   * @return total number of block for deletion
   */
  int computeInvalidateWork(int nodesToProcess) {
    final List<DatanodeInfo> nodes = invalidateBlocks.getDatanodes();
    Collections.shuffle(nodes);

    nodesToProcess = Math.min(nodes.size(), nodesToProcess);

    int blockCnt = 0;
    for (DatanodeInfo dnInfo : nodes) {
      int blocks = invalidateWorkForOneNode(dnInfo);
      if (blocks > 0) {
        blockCnt += blocks;
        if (--nodesToProcess == 0) {
          break;
        }
      }
    }
    return blockCnt;
  }

  /**
   * Scan blocks in {@link #neededReplications} and assign replication
   * work to data-nodes they belong to.
   *
   * The number of process blocks equals either twice the number of live
   * data-nodes or the number of under-replicated blocks whichever is less.
   *
   * @return number of blocks scheduled for replication during this iteration.
   */
  int computeReplicationWork(int blocksToProcess) {
    List<List<Block>> blocksToReplicate = null;
    namesystem.writeLock();
    try {
      // Choose the blocks to be replicated
      blocksToReplicate = neededReplications
          .chooseUnderReplicatedBlocks(blocksToProcess);
    } finally {
      namesystem.writeUnlock();
    }
    return computeReplicationWorkForBlocks(blocksToReplicate);
  }

  /** Replicate a set of blocks
   *
   * @param blocksToReplicate blocks to be replicated, for each priority
   * @return the number of blocks scheduled for replication
   */
  @VisibleForTesting
  int computeReplicationWorkForBlocks(List<List<Block>> blocksToReplicate) {
    int requiredReplication, numEffectiveReplicas; // 有效副本数
    List<DatanodeDescriptor> containingNodes;
    DatanodeDescriptor srcNode;
    BlockCollection bc = null;
    int additionalReplRequired;

    int scheduledWork = 0;
    List<ReplicationWork> work = new LinkedList<ReplicationWork>();

    namesystem.writeLock();
    try {
      // step 1: 选择 source
      synchronized (neededReplications) {
        for (int priority = 0; priority < blocksToReplicate.size(); priority++) {
          for (Block block : blocksToReplicate.get(priority)) {
            // block should belong to a file
            bc = blocksMap.getBlockCollection(block);
            // abandoned block or block reopened for append
            // 正在构建，不必备份
            if(bc == null || (bc.isUnderConstruction() && block.equals(bc.getLastBlock()))) {
              neededReplications.remove(block, priority); // remove from neededReplications
              neededReplications.decrementReplicationIndex(priority);
              continue;
            }

            requiredReplication = bc.getBlockReplication();

            // get a source data-node
            containingNodes = new ArrayList<DatanodeDescriptor>();
            List<DatanodeStorageInfo> liveReplicaNodes = new ArrayList<DatanodeStorageInfo>();
            NumberReplicas numReplicas = new NumberReplicas();
            srcNode = chooseSourceDatanode(
                block, containingNodes, liveReplicaNodes, numReplicas,
                priority);
            if(srcNode == null) { // block can not be replicated from any node
              LOG.debug("Block " + block + " cannot be repl from any node");
              continue;
            }

            // liveReplicaNodes can include READ_ONLY_SHARED replicas which are 
            // not included in the numReplicas.liveReplicas() count
            assert liveReplicaNodes.size() >= numReplicas.liveReplicas();

            // do not schedule more if enough replicas is already pending
            numEffectiveReplicas = numReplicas.liveReplicas() +
                                    pendingReplications.getNumReplicas(block);

            // 副本数已足够，不必备份
            if (numEffectiveReplicas >= requiredReplication) {
              if ( (pendingReplications.getNumReplicas(block) > 0) ||
                   (blockHasEnoughRacks(block)) ) {
                neededReplications.remove(block, priority); // remove from neededReplications
                neededReplications.decrementReplicationIndex(priority);
                blockLog.info("BLOCK* Removing {} from neededReplications as" +
                        " it has enough replicas", block);
                continue;
              }
            }

            if (numReplicas.liveReplicas() < requiredReplication) {
              // 副本数不足，计算需要添加几个副本
              additionalReplRequired = requiredReplication
                  - numEffectiveReplicas;
            } else {
              // 虽然副本数足够，但在同一个机架上
              additionalReplRequired = 1; // Needed on a new rack
            }
            // 添加复制任务
            work.add(new ReplicationWork(block, bc, srcNode,
                containingNodes, liveReplicaNodes, additionalReplRequired,
                priority));
          }
        }
      }
    } finally {
      namesystem.writeUnlock();
    }

    // step 2: 选择 target
    final Set<Node> excludedNodes = new HashSet<Node>();
    for(ReplicationWork rw : work){
      // Exclude all of the containing nodes from being targets.
      // This list includes decommissioning or corrupt nodes.
      excludedNodes.clear();
      // 排除已有副本的DN
      for (DatanodeDescriptor dn : rw.containingNodes) {
        excludedNodes.add(dn);
      }

      // choose replication targets: NOT HOLDING THE GLOBAL LOCK
      // It is costly to extract the filename for which chooseTargets is called,
      // so for now we pass in the block collection itself.
      rw.chooseTargets(blockplacement, storagePolicySuite, excludedNodes);
    }

    // step 3: 将副本加入DN复制队列，下次心跳，发出复制命令
    namesystem.writeLock();
    try {
      for(ReplicationWork rw : work){
        final DatanodeStorageInfo[] targets = rw.targets;
        if(targets == null || targets.length == 0){
          rw.targets = null;
          continue;
        }

        synchronized (neededReplications) {
          Block block = rw.block;
          int priority = rw.priority;
          // Recheck since global lock was released
          // block should belong to a file
          bc = blocksMap.getBlockCollection(block);
          // abandoned block or block reopened for append
          if(bc == null || (bc.isUnderConstruction() && block.equals(bc.getLastBlock()))) {
            neededReplications.remove(block, priority); // remove from neededReplications
            rw.targets = null;
            neededReplications.decrementReplicationIndex(priority);
            continue;
          }
          requiredReplication = bc.getBlockReplication();

          // do not schedule more if enough replicas is already pending
          NumberReplicas numReplicas = countNodes(block);
          numEffectiveReplicas = numReplicas.liveReplicas() +
            pendingReplications.getNumReplicas(block);

          if (numEffectiveReplicas >= requiredReplication) {
            if ( (pendingReplications.getNumReplicas(block) > 0) ||
                 (blockHasEnoughRacks(block)) ) {
              neededReplications.remove(block, priority); // remove from neededReplications
              neededReplications.decrementReplicationIndex(priority);
              rw.targets = null;
              blockLog.info("BLOCK* Removing {} from neededReplications as" +
                      " it has enough replicas", block);
              continue;
            }
          }

          if ( (numReplicas.liveReplicas() >= requiredReplication) &&
               (!blockHasEnoughRacks(block)) ) {
            if (rw.srcNode.getNetworkLocation().equals(
                targets[0].getDatanodeDescriptor().getNetworkLocation())) {
              //No use continuing, unless a new rack in this case
              continue;
            }
          }

          // Add block to the to be replicated list
          // 将副本加入DN的复制队列中
          rw.srcNode.addBlockToBeReplicated(block, targets);
          scheduledWork++;
          DatanodeStorageInfo.incrementBlocksScheduled(targets);

          // Move the block-replication into a "pending" state.
          // The reason we use 'pending' is so we can retry
          // replications that fail after an appropriate amount of time.
          pendingReplications.increment(block,
              DatanodeStorageInfo.toDatanodeDescriptors(targets));
          blockLog.debug("BLOCK* block {} is moved from neededReplications to "
                  + "pendingReplications", block);

          // remove from neededReplications
          if(numEffectiveReplicas + targets.length >= requiredReplication) {
            neededReplications.remove(block, priority); // remove from neededReplications
            neededReplications.decrementReplicationIndex(priority);
          }
        }
      }
    } finally {
      namesystem.writeUnlock();
    }

    if (blockLog.isInfoEnabled()) {
      // log which blocks have been scheduled for replication
      for(ReplicationWork rw : work){
        DatanodeStorageInfo[] targets = rw.targets;
        if (targets != null && targets.length != 0) {
          StringBuilder targetList = new StringBuilder("datanode(s)");
          for (int k = 0; k < targets.length; k++) {
            targetList.append(' ');
            targetList.append(targets[k].getDatanodeDescriptor());
          }
          blockLog.info("BLOCK* ask {} to replicate {} to {}", rw.srcNode,
              rw.block, targetList);
        }
      }
    }
    if (blockLog.isDebugEnabled()) {
      blockLog.debug("BLOCK* neededReplications = {} pendingReplications = {}",
          neededReplications.size(), pendingReplications.size());
    }

    return scheduledWork;
  }

  /** Choose target for WebHDFS redirection. */
  public DatanodeStorageInfo[] chooseTarget4WebHDFS(String src,
      DatanodeDescriptor clientnode, Set<Node> excludes, long blocksize) {
    return blockplacement.chooseTarget(src, 1, clientnode,
        Collections.<DatanodeStorageInfo>emptyList(), false, excludes,
        blocksize, storagePolicySuite.getDefaultPolicy());
  }

  /** Choose target for getting additional datanodes for an existing pipeline. */
  public DatanodeStorageInfo[] chooseTarget4AdditionalDatanode(String src,
      int numAdditionalNodes,
      Node clientnode,
      List<DatanodeStorageInfo> chosen,
      Set<Node> excludes,
      long blocksize,
      byte storagePolicyID) {
    
    final BlockStoragePolicy storagePolicy = storagePolicySuite.getPolicy(storagePolicyID);
    return blockplacement.chooseTarget(src, numAdditionalNodes, clientnode,
        chosen, true, excludes, blocksize, storagePolicy);
  }

  /**
   * Choose target datanodes for creating a new block.
   * 
   * @throws IOException
   *           if the number of targets < minimum replication.
   * @see BlockPlacementPolicy#chooseTarget(String, int, Node,
   *      Set, long, List, BlockStoragePolicy)
   */
  public DatanodeStorageInfo[] chooseTarget4NewBlock(final String src,
      final int numOfReplicas, final Node client,
      final Set<Node> excludedNodes,
      final long blocksize,
      final List<String> favoredNodes,
      final byte storagePolicyID) throws IOException {
    List<DatanodeDescriptor> favoredDatanodeDescriptors = 
        getDatanodeDescriptors(favoredNodes); // hostname转为DN描述符
    final BlockStoragePolicy storagePolicy = storagePolicySuite.getPolicy(storagePolicyID);
    final DatanodeStorageInfo[] targets = blockplacement.chooseTarget(src, // 选择副本存储位置
        numOfReplicas, client, excludedNodes, blocksize, 
        favoredDatanodeDescriptors, storagePolicy);
    if (targets.length < minReplication) { // 只有达不到最小副本数时，才会抛异常
      throw new IOException("File " + src + " could only be replicated to "
          + targets.length + " nodes instead of minReplication (="
          + minReplication + ").  There are "
          + getDatanodeManager().getNetworkTopology().getNumOfLeaves()
          + " datanode(s) running and "
          + (excludedNodes == null? "no": excludedNodes.size())
          + " node(s) are excluded in this operation.");
    }
    return targets;
  }

  /**
   * Get list of datanode descriptors for given list of nodes. Nodes are
   * hostaddress:port or just hostaddress.
   */
  List<DatanodeDescriptor> getDatanodeDescriptors(List<String> nodes) { // hostname转为DN描述符
    List<DatanodeDescriptor> datanodeDescriptors = null;
    if (nodes != null) {
      datanodeDescriptors = new ArrayList<DatanodeDescriptor>(nodes.size());
      for (int i = 0; i < nodes.size(); i++) {
        DatanodeDescriptor node = datanodeManager.getDatanodeDescriptor(nodes.get(i));
        if (node != null) {
          datanodeDescriptors.add(node);
        }
      }
    }
    return datanodeDescriptors;
  }

  /**
   * Parse the data-nodes the block belongs to and choose one,
   * which will be the replication source.
   *
   * We prefer nodes that are in DECOMMISSION_INPROGRESS state to other nodes
   * since the former do not have write traffic and hence are less busy.
   * We do not use already decommissioned nodes as a source.
   * Otherwise we choose a random node among those that did not reach their
   * replication limits.  However, if the replication is of the highest priority
   * and all nodes have reached their replication limits, we will choose a
   * random node despite the replication limit.
   *
   * In addition form a list of all nodes containing the block
   * and calculate its replication numbers.
   *
   * @param block Block for which a replication source is needed
   * @param containingNodes List to be populated with nodes found to contain the 
   *                        given block
   * @param nodesContainingLiveReplicas List to be populated with nodes found to
   *                                    contain live replicas of the given block
   * @param numReplicas NumberReplicas instance to be initialized with the 
   *                                   counts of live, corrupt, excess, and
   *                                   decommissioned replicas of the given
   *                                   block.
   * @param priority integer representing replication priority of the given
   *                 block
   * @return the DatanodeDescriptor of the chosen node from which to replicate
   *         the given block
   */
   @VisibleForTesting
   DatanodeDescriptor chooseSourceDatanode(Block block,
       List<DatanodeDescriptor> containingNodes,
       List<DatanodeStorageInfo>  nodesContainingLiveReplicas,
       NumberReplicas numReplicas,
       int priority) {
    containingNodes.clear();
    nodesContainingLiveReplicas.clear();
    DatanodeDescriptor srcNode = null;
    int live = 0;
    int decommissioned = 0;
    int corrupt = 0;
    int excess = 0;
    
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(block);
    for(DatanodeStorageInfo storage : blocksMap.getStorages(block)) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      LightWeightLinkedSet<Block> excessBlocks =
        excessReplicateMap.get(node.getDatanodeUuid());
      int countableReplica = storage.getState() == State.NORMAL ? 1 : 0; 
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node)))
        corrupt += countableReplica;
      else if (node.isDecommissionInProgress() || node.isDecommissioned())
        decommissioned += countableReplica;
      else if (excessBlocks != null && excessBlocks.contains(block)) {
        excess += countableReplica;
      } else {
        nodesContainingLiveReplicas.add(storage);
        live += countableReplica;
      }
      containingNodes.add(node);
      // Check if this replica is corrupt
      // If so, do not select the node as src node
      if ((nodesCorrupt != null) && nodesCorrupt.contains(node))
        continue;
      // block副本数不足时，需要执行补副本操作，以达到期望的副本数
      // 如果此block只剩一个副本了，那就需要以最高优先级执行补副本操作
      // 就算DN达到了maxReplicationStreams，但没达到replicationStreamsHardLimit，也可以选择此DN
      if(priority != UnderReplicatedBlocks.QUEUE_HIGHEST_PRIORITY
          && !node.isDecommissionInProgress() 
          && node.getNumberOfBlocksToBeReplicated() >= maxReplicationStreams)
      {
        continue; // already reached replication limit
      }
      if (node.getNumberOfBlocksToBeReplicated() >= replicationStreamsHardLimit)
      {
        continue;
      }
      // the block must not be scheduled for removal on srcNode
      if(excessBlocks != null && excessBlocks.contains(block))
        continue;
      // never use already decommissioned nodes
      if(node.isDecommissioned())
        continue;

      // We got this far, current node is a reasonable choice
      if (srcNode == null) {
        srcNode = node;
        continue;
      }
      // switch to a different node randomly
      // this to prevent from deterministically selecting the same node even
      // if the node failed to replicate the block on previous iterations
      if(DFSUtil.getRandom().nextBoolean())
        srcNode = node;
    }
    if(numReplicas != null)
      numReplicas.initialize(live, decommissioned, corrupt, excess, 0);
    return srcNode;
  }

  /**
   * If there were any replication requests that timed out, reap them
   * and put them back into the neededReplication queue
   */
  private void processPendingReplications() {
    Block[] timedOutItems = pendingReplications.getTimedOutBlocks();
    if (timedOutItems != null) {
      namesystem.writeLock();
      try {
        for (int i = 0; i < timedOutItems.length; i++) {
          /*
           * Use the blockinfo from the blocksmap to be certain we're working
           * with the most up-to-date block information (e.g. genstamp).
           */
          BlockInfoContiguous bi = blocksMap.getStoredBlock(timedOutItems[i]);
          if (bi == null) {
            continue;
          }
          NumberReplicas num = countNodes(timedOutItems[i]);
          if (isNeededReplication(bi, getReplication(bi), num.liveReplicas())) {
            neededReplications.add(bi, num.liveReplicas(),
                num.decommissionedReplicas(), getReplication(bi));
          }
        }
      } finally {
        namesystem.writeUnlock();
      }
      /* If we know the target datanodes where the replication timedout,
       * we could invoke decBlocksScheduled() on it. Its ok for now.
       */
    }
  }
  
  /**
   * StatefulBlockInfo is used to build the "toUC" list, which is a list of
   * updates to the information about under-construction blocks.
   * Besides the block in question, it provides the ReplicaState
   * reported by the datanode in the block report. 
   */
  static class StatefulBlockInfo {
    final BlockInfoContiguousUnderConstruction storedBlock;
    final Block reportedBlock;
    final ReplicaState reportedState;
    
    StatefulBlockInfo(BlockInfoContiguousUnderConstruction storedBlock,
        Block reportedBlock, ReplicaState reportedState) {
      this.storedBlock = storedBlock;
      this.reportedBlock = reportedBlock;
      this.reportedState = reportedState;
    }
  }
  
  /**
   * BlockToMarkCorrupt is used to build the "toCorrupt" list, which is a
   * list of blocks that should be considered corrupt due to a block report.
   */
  private static class BlockToMarkCorrupt {
    /** The corrupted block in a datanode. */
    final BlockInfoContiguous corrupted;
    /** The corresponding block stored in the BlockManager. */
    final BlockInfoContiguous stored;
    /** The reason to mark corrupt. */
    final String reason;
    /** The reason code to be stored */
    final Reason reasonCode;

    BlockToMarkCorrupt(BlockInfoContiguous corrupted,
        BlockInfoContiguous stored, String reason,
        Reason reasonCode) {
      Preconditions.checkNotNull(corrupted, "corrupted is null");
      Preconditions.checkNotNull(stored, "stored is null");

      this.corrupted = corrupted;
      this.stored = stored;
      this.reason = reason;
      this.reasonCode = reasonCode;
    }

    BlockToMarkCorrupt(BlockInfoContiguous stored, String reason,
        Reason reasonCode) {
      this(stored, stored, reason, reasonCode);
    }

    BlockToMarkCorrupt(BlockInfoContiguous stored, long gs, String reason,
        Reason reasonCode) {
      this(new BlockInfoContiguous(stored), stored, reason, reasonCode);
      //the corrupted block in datanode has a different generation stamp
      corrupted.setGenerationStamp(gs);
    }

    @Override
    public String toString() {
      return corrupted + "("
          + (corrupted == stored? "same as stored": "stored=" + stored) + ")";
    }
  }

  /**
   * The given storage is reporting all its blocks.
   * Update the (storage-->block list) and (block-->storage list) maps.
   *
   * @return true if all known storages of the given DN have finished reporting.
   * @throws IOException
   */
  public boolean processReport(final DatanodeID nodeID,
      final DatanodeStorage storage,
      final BlockListAsLongs newReport, BlockReportContext context,
      boolean lastStorageInRpc) throws IOException { // 全量块汇报
    namesystem.writeLock();
    final long startTime = Time.monotonicNow(); //after acquiring write lock
    final long endTime;
    DatanodeDescriptor node;
    Collection<Block> invalidatedBlocks = null;

    try {
      node = datanodeManager.getDatanode(nodeID); // NN端描述一个DN
      if (node == null || !node.isAlive) {
        throw new IOException("ProcessReport from dead or unregistered node: " + nodeID);
      }

      // To minimize startup time, we discard any second (or later) block reports
      // that we receive while still in startup phase.
      DatanodeStorageInfo storageInfo = node.getStorageInfo(storage.getStorageID()); // NN端描述DN一个存储目录，包括此目录中存储的块

      if (storageInfo == null) {
        // We handle this for backwards compatibility.
        storageInfo = node.updateStorage(storage);
      }
      if (namesystem.isInStartupSafeMode()
          && storageInfo.getBlockReportCount() > 0) { // 启动的时候，一个storage只处理一次全量块汇报
        blockLog.info("BLOCK* processReport: "
            + "discarded non-initial block report from {}"
            + " because namenode still in startup phase", nodeID);
        return !node.hasStaleStorages();
      }

      if (storageInfo.getBlockReportCount() == 0) { // storage的第一次全量块汇报
        // The first block report can be processed a lot more efficiently than
        // ordinary block reports.  This shortens restart times.
        processFirstBlockReport(storageInfo, newReport); // 第一次块汇报，直接添加block，不维护BM中的副本集合
      } else {
        invalidatedBlocks = processReport(storageInfo, newReport);
      }
      
      storageInfo.receivedBlockReport();
      if (context != null) {
        storageInfo.setLastBlockReportId(context.getReportId());
        if (lastStorageInRpc) { // 本次汇报的最后一个storage
          int rpcsSeen = node.updateBlockReportContext(context);
          if (rpcsSeen >= context.getTotalRpcs()) { // 判断此DN所有的storage，是否都已经处理过
            List<DatanodeStorageInfo> zombies = node.removeZombieStorages(); // 移除有问题的磁盘
            if (zombies.isEmpty()) {
              LOG.debug("processReport 0x{}: no zombie storages found.",
                  Long.toHexString(context.getReportId()));
            } else {
              for (DatanodeStorageInfo zombie : zombies) {
                removeZombieReplicas(context, zombie); // 从NN内存中移除问题磁盘的副本信息
              }
            }
            node.clearBlockReportContext();
          } else {
            LOG.debug("processReport 0x{}: {} more RPCs remaining in this " +
                    "report.", Long.toHexString(context.getReportId()),
                (context.getTotalRpcs() - rpcsSeen)
            );
          }
        }
      }
    } finally {
      endTime = Time.monotonicNow();
      namesystem.writeUnlock();
    }

    if (invalidatedBlocks != null) {
      for (Block b : invalidatedBlocks) {
        blockLog.info("BLOCK* processReport: {} on node {} size {} does not " +
            "belong to any file", b, node, b.getNumBytes());
      }
    }

    // Log the block report processing stats from Namenode perspective
    final NameNodeMetrics metrics = NameNode.getNameNodeMetrics();
    if (metrics != null) {
      metrics.addBlockReport((int) (endTime - startTime));
    }
    blockLog.info("BLOCK* processReport: from storage {} node {}, " +
        "blocks: {}, hasStaleStorage: {}, processing time: {} msecs", storage
        .getStorageID(), nodeID, newReport.getNumberOfBlocks(),
        node.hasStaleStorages(), (endTime - startTime));
    return !node.hasStaleStorages();
  }

  private void removeZombieReplicas(BlockReportContext context,
      DatanodeStorageInfo zombie) { // 从NN内存中移除问题磁盘的副本信息
    LOG.warn("processReport 0x{}: removing zombie storage {}, which no " +
             "longer exists on the DataNode.",
              Long.toHexString(context.getReportId()), zombie.getStorageID());
    assert(namesystem.hasWriteLock());
    Iterator<BlockInfoContiguous> iter = zombie.getBlockIterator();
    int prevBlocks = zombie.numBlocks();
    while (iter.hasNext()) {
      BlockInfoContiguous block = iter.next();
      // We assume that a block can be on only one storage in a DataNode.
      // That's why we pass in the DatanodeDescriptor rather than the
      // DatanodeStorageInfo.
      // TODO: remove this assumption in case we want to put a block on
      // more than one storage on a datanode (and because it's a difficult
      // assumption to really enforce)
      removeStoredBlock(block, zombie.getDatanodeDescriptor()); // 从NN内存中移除此副本信息
      invalidateBlocks.remove(zombie.getDatanodeDescriptor(), block);
    }
    assert(zombie.numBlocks() == 0);
    LOG.warn("processReport 0x{}: removed {} replicas from storage {}, " +
            "which no longer exists on the DataNode.",
            Long.toHexString(context.getReportId()), prevBlocks,
            zombie.getStorageID());
  }

  /**
   * Rescan the list of blocks which were previously postponed.
   */
  void rescanPostponedMisreplicatedBlocks() { // 处理postponedMisreplicatedBlocks中的block
    if (getPostponedMisreplicatedBlocksCount() == 0) { // 没有需要延后处理的block
      return;
    }
    long startTimeRescanPostponedMisReplicatedBlocks = Time.monotonicNow();
    long startPostponedMisReplicatedBlocksCount =
        getPostponedMisreplicatedBlocksCount();
    namesystem.writeLock();
    try {
      // blocksPerRescan is the configured number of blocks per rescan.
      // Randomly select blocksPerRescan consecutive blocks from the HashSet
      // when the number of blocks remaining is larger than blocksPerRescan.
      // The reason we don't always pick the first blocksPerRescan blocks is to
      // handle the case if for some reason some datanodes remain in
      // content stale state for a long time and only impact the first
      // blocksPerRescan blocks.
      int i = 0;
      long startIndex = 0;
      long blocksPerRescan =
          datanodeManager.getBlocksPerPostponedMisreplicatedBlocksRescan(); // 获取一次写锁，处理的延迟block个数
      long base = getPostponedMisreplicatedBlocksCount() - blocksPerRescan;
      if (base > 0) { // 如果延迟block总数大于blocksPerRescan，则随机一个位置开始处理，而不是每次都从头开始处理blocksPerRescan个
        startIndex = DFSUtil.getRandom().nextLong() % (base+1);
        if (startIndex < 0) {
          startIndex += (base+1);
        }
      }
      Iterator<Block> it = postponedMisreplicatedBlocks.iterator();
      for (int tmp = 0; tmp < startIndex; tmp++) { // startIndex之前的直接跳过
        it.next();
      }

      for (;it.hasNext(); i++) {
        Block b = it.next();
        if (i >= blocksPerRescan) {
          break;
        }

        BlockInfoContiguous bi = blocksMap.getStoredBlock(b);
        if (bi == null) { // blocksMap中没有此block，可以直接移除
          if (LOG.isDebugEnabled()) {
            LOG.debug("BLOCK* rescanPostponedMisreplicatedBlocks: " +
                "Postponed mis-replicated block " + b + " no longer found " +
                "in block map.");
          }
          it.remove();
          postponedMisreplicatedBlocksCount.decrementAndGet();
          continue;
        }
        MisReplicationResult res = processMisReplicatedBlock(bi); // 处理延迟block
        if (LOG.isDebugEnabled()) {
          LOG.debug("BLOCK* rescanPostponedMisreplicatedBlocks: " +
              "Re-scanned block " + b + ", result is " + res);
        }
        if (res != MisReplicationResult.POSTPONE) { // 表明处理成功，移除
          it.remove();
          postponedMisreplicatedBlocksCount.decrementAndGet();
        }
      }
    } finally {
      namesystem.writeUnlock();
      long endPostponedMisReplicatedBlocksCount =
          getPostponedMisreplicatedBlocksCount();
      LOG.info("Rescan of postponedMisreplicatedBlocks completed in " +
          (Time.monotonicNow() - startTimeRescanPostponedMisReplicatedBlocks) +
          " msecs. " + endPostponedMisReplicatedBlocksCount +
          " blocks are left. " + (startPostponedMisReplicatedBlocksCount -
          endPostponedMisReplicatedBlocksCount) + " blocks are removed.");
    }
  }

  // Step 1: 判断副本状态
  // Step 2: 根据副本状态，执行不同的操作
  private Collection<Block> processReport(
      final DatanodeStorageInfo storageInfo,
      final BlockListAsLongs report) throws IOException { // 非第一次全量块汇报
    // Normal case:
    // Modify the (block-->datanode) map, according to the difference
    // between the old and new block report.
    //
    Collection<BlockInfoContiguous> toAdd = new LinkedList<BlockInfoContiguous>(); // DN和NN一致，添加到NN
    Collection<Block> toRemove = new TreeSet<Block>(); // 这个replica，NN有，DN没有，从NN中删除
    Collection<Block> toInvalidate = new LinkedList<Block>(); // 这个block，NN没有，DN有，从DN中删除
    Collection<BlockToMarkCorrupt> toCorrupt = new LinkedList<BlockToMarkCorrupt>(); // DN和NN不一致，先补副本，再从DN中删除
    Collection<StatefulBlockInfo> toUC = new LinkedList<StatefulBlockInfo>(); // 正在构建中的副本，更新NN（UC block replicas字段）

    // Step 1: 判断副本状态
    reportDiff(storageInfo, report, toAdd, toRemove, toInvalidate, toCorrupt, toUC);
   
    DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();
    // Process the blocks on each queue
    // Step 2: 根据副本状态，执行不同的操作
    for (StatefulBlockInfo b : toUC) { 
      addStoredBlockUnderConstruction(b, storageInfo);
    }
    for (Block b : toRemove) {
      removeStoredBlock(b, node); // 从NN内存中移除此副本信息，并且维护相应集合
    }
    int numBlocksLogged = 0;
    for (BlockInfoContiguous b : toAdd) {
      addStoredBlock(b, storageInfo, null, numBlocksLogged < maxNumBlocksToLog);
      numBlocksLogged++;
    }
    if (numBlocksLogged > maxNumBlocksToLog) {
      blockLog.info("BLOCK* processReport: logged info for {} of {} " +
          "reported.", maxNumBlocksToLog, numBlocksLogged);
    }
    for (Block b : toInvalidate) {
      addToInvalidates(b, node);
    }
    for (BlockToMarkCorrupt b : toCorrupt) {
      markBlockAsCorrupt(b, storageInfo, node);
    }

    return toInvalidate;
  }

  /**
   * Mark block replicas as corrupt except those on the storages in 
   * newStorages list.
   */
  public void markBlockReplicasAsCorrupt(BlockInfoContiguous block, 
      long oldGenerationStamp, long oldNumBytes, 
      DatanodeStorageInfo[] newStorages) throws IOException {
    assert namesystem.hasWriteLock();
    BlockToMarkCorrupt b = null;
    if (block.getGenerationStamp() != oldGenerationStamp) {
      b = new BlockToMarkCorrupt(block, oldGenerationStamp,
          "genstamp does not match " + oldGenerationStamp
          + " : " + block.getGenerationStamp(), Reason.GENSTAMP_MISMATCH);
    } else if (block.getNumBytes() != oldNumBytes) {
      b = new BlockToMarkCorrupt(block,
          "length does not match " + oldNumBytes
          + " : " + block.getNumBytes(), Reason.SIZE_MISMATCH);
    } else {
      return;
    }

    for (DatanodeStorageInfo storage : getStorages(block)) {
      boolean isCorrupt = true;
      if (newStorages != null) {
        for (DatanodeStorageInfo newStorage : newStorages) {
          if (newStorage!= null && storage.equals(newStorage)) {
            isCorrupt = false;
            break;
          }
        }
      }
      if (isCorrupt) {
        blockLog.info("BLOCK* markBlockReplicasAsCorrupt: mark block replica" +
            " {} on {} as corrupt because the dn is not in the new committed " +
            "storage list.", b, storage.getDatanodeDescriptor());
        markBlockAsCorrupt(b, storage, storage.getDatanodeDescriptor());
      }
    }
  }

  /**
   * processFirstBlockReport is intended only for processing "initial" block
   * reports, the first block report received from a DN after it registers.
   * It just adds all the valid replicas to the datanode, without calculating 
   * a toRemove list (since there won't be any).  It also silently discards 
   * any invalid blocks, thereby deferring their processing until 
   * the next block report.
   * @param storageInfo - DatanodeStorageInfo that sent the report
   * @param report - the initial block report, to be processed
   * @throws IOException 
   */
  private void processFirstBlockReport( // Storage的第一次块汇报，直接添加block，不维护BM中的副本集合
      final DatanodeStorageInfo storageInfo,
      final BlockListAsLongs report) throws IOException {
    if (report == null) return;
    assert (namesystem.hasWriteLock());
    assert (storageInfo.getBlockReportCount() == 0);

    for (BlockReportReplica iblk : report) {
      ReplicaState reportedState = iblk.getState();

      // 如果当前为standby节点，可能出现standby接收块汇报时，并没有完全更新edits的情况
      if (shouldPostponeBlocksFromFuture && namesystem.isGenStampInFuture(iblk)) {
        queueReportedBlock(storageInfo, iblk, reportedState, QUEUE_REASON_FUTURE_GENSTAMP);
        continue;
      }
      
      BlockInfoContiguous storedBlock = blocksMap.getStoredBlock(iblk);
      // If block does not belong to any file, we are done.
      if (storedBlock == null) continue;
      
      // If block is corrupt, mark it and continue to next block.
      BlockUCState ucState = storedBlock.getBlockUCState();
      BlockToMarkCorrupt c = checkReplicaCorrupt( // 根据GS和长度，判断此副本是否损坏
          iblk, reportedState, storedBlock, ucState,
          storageInfo.getDatanodeDescriptor());
      if (c != null) {
        if (shouldPostponeBlocksFromFuture) {
          // In the Standby, we may receive a block report for a file that we
          // just have an out-of-date gen-stamp or state for, for example.
          queueReportedBlock(storageInfo, iblk, reportedState,
              QUEUE_REASON_CORRUPT_STATE);
        } else { // 发现一个损坏的副本
          markBlockAsCorrupt(c, storageInfo, storageInfo.getDatanodeDescriptor());
        }
        continue;
      }
      
      // If block is under construction, add this replica to its list
      if (isBlockUnderConstruction(storedBlock, ucState, reportedState)) { // 此副本是否正在构建中
        ((BlockInfoContiguousUnderConstruction)storedBlock)
            .addReplicaIfNotPresent(storageInfo, iblk, reportedState); // 添加或更新block replicas列表
        // OpenFileBlocks only inside snapshots also will be added to safemode
        // threshold. So we need to update such blocks to safemode
        // refer HDFS-5283
        BlockInfoContiguousUnderConstruction blockUC =
            (BlockInfoContiguousUnderConstruction) storedBlock;
        if (namesystem.isInSnapshot(blockUC)) {
          int numOfReplicas = blockUC.getNumExpectedLocations();
          namesystem.incrementSafeBlockCount(numOfReplicas);
        }
        //and fall through to next clause
      }      
      //add replica if appropriate
      if (reportedState == ReplicaState.FINALIZED) {
        addStoredBlockImmediate(storedBlock, storageInfo); // 添加block
      }
    }
  }

  // 判断副本状态
  private void reportDiff(DatanodeStorageInfo storageInfo, 
      BlockListAsLongs newReport, 
      Collection<BlockInfoContiguous> toAdd,              // add to DatanodeDescriptor
      Collection<Block> toRemove,           // remove from DatanodeDescriptor
      Collection<Block> toInvalidate,       // should be removed from DN
      Collection<BlockToMarkCorrupt> toCorrupt, // add to corrupt replicas list
      Collection<StatefulBlockInfo> toUC) { // add to under-construction list

    // place a delimiter in the list which separates blocks 
    // that have been reported from those that have not
    // 放置一个分隔符block到blockList的列表头，之后处理block的时候，把每个处理过的block都移动到blockList的列表头
    // 这样处理之后，分隔符block之后的block，就需要从NN内存中删除（NN中有，DN中没有）
    BlockInfoContiguous delimiter = new BlockInfoContiguous(new Block(), (short) 1);
    AddBlockResult result = storageInfo.addBlock(delimiter);
    assert result == AddBlockResult.ADDED 
        : "Delimiting block cannot be present in the node";
    int headIndex = 0; //currently the delimiter is in the head of the list
    int curIndex;

    if (newReport == null) {
      newReport = BlockListAsLongs.EMPTY;
    }
    // scan the report and process newly reported blocks
    for (BlockReportReplica iblk : newReport) {
      ReplicaState iState = iblk.getState();
      BlockInfoContiguous storedBlock = processReportedBlock(storageInfo, // 判断副本状态
          iblk, iState, toAdd, toInvalidate, toCorrupt, toUC);

      // move block to the head of the list
      if (storedBlock != null &&
          (curIndex = storedBlock.findStorageInfo(storageInfo)) >= 0) {
        // 把处理过的block，移动到blockList的列表头
        headIndex = storageInfo.moveBlockToHead(storedBlock, curIndex, headIndex);
      }
    }

    // collect blocks that have not been reported
    // all of them are next to the delimiter
    Iterator<BlockInfoContiguous> it =
        storageInfo.new BlockIterator(delimiter.getNext(0));
    while(it.hasNext()) // 分隔符之后的block，都添加到toRemove列表中
      toRemove.add(it.next());
    storageInfo.removeBlock(delimiter); // 删除分隔符
  }

  /**
   * Process a block replica reported by the data-node.
   * No side effects except adding to the passed-in Collections.
   * 
   * <ol>
   * <li>If the block is not known to the system (not in blocksMap) then the
   * data-node should be notified to invalidate this block.</li>
   * <li>If the reported replica is valid that is has the same generation stamp
   * and length as recorded on the name-node, then the replica location should
   * be added to the name-node.</li>
   * <li>If the reported replica is not valid, then it is marked as corrupt,
   * which triggers replication of the existing valid replicas.
   * Corrupt replicas are removed from the system when the block
   * is fully replicated.</li>
   * <li>If the reported replica is for a block currently marked "under
   * construction" in the NN, then it should be added to the 
   * BlockInfoUnderConstruction's list of replicas.</li>
   * </ol>
   * 
   * @param storageInfo DatanodeStorageInfo that sent the report.
   * @param block reported block replica
   * @param reportedState reported replica state
   * @param toAdd add to DatanodeDescriptor DN汇报的此副本信息和blocksMap中一致，且是Finalize状态且之前未汇报过，添加到NN
   * @param toInvalidate missing blocks (not in the blocks map) DN汇报的此副本，blocksMap没有，从DN删除
   *        should be removed from the data-node
   * @param toCorrupt replicas with unexpected length or generation stamp; DN汇报的信息和blocksMap中不一致，从DN删除
   *        add to corrupt replicas
   * @param toUC replicas of blocks currently under construction 正在构建中的副本
   * @return the up-to-date stored block, if it should be kept.
   *         Otherwise, null.
   */
  // 判断副本状态，并添加到不同的集合中
  private BlockInfoContiguous processReportedBlock(
      final DatanodeStorageInfo storageInfo,
      final Block block, final ReplicaState reportedState, 
      final Collection<BlockInfoContiguous> toAdd,
      final Collection<Block> toInvalidate, 
      final Collection<BlockToMarkCorrupt> toCorrupt,
      final Collection<StatefulBlockInfo> toUC) {
    
    DatanodeDescriptor dn = storageInfo.getDatanodeDescriptor();

    if(LOG.isDebugEnabled()) {
      LOG.debug("Reported block " + block
          + " on " + dn + " size " + block.getNumBytes()
          + " replicaState = " + reportedState);
    }
  
    if (shouldPostponeBlocksFromFuture &&
        namesystem.isGenStampInFuture(block)) { // Standby节点在没有收到edit信息之前，先收到了DN的块汇报
      queueReportedBlock(storageInfo, block, reportedState,
          QUEUE_REASON_FUTURE_GENSTAMP);
      return null;
    }
    
    // find block by blockId
    BlockInfoContiguous storedBlock = blocksMap.getStoredBlock(block);
    if(storedBlock == null) { // blocksMap没有，加入到toInvalidate中
      // If blocksMap does not contain reported block id,
      // the replica should be removed from the data-node.
      toInvalidate.add(new Block(block));
      return null;
    }
    BlockUCState ucState = storedBlock.getBlockUCState(); // 获取块状态
    
    // Block is on the NN
    if(LOG.isDebugEnabled()) {
      LOG.debug("In memory blockUCState = " + ucState);
    }

    // Ignore replicas already scheduled to be removed from the DN
    if(invalidateBlocks.contains(dn, block)) { // 已经在待删除队列中
      /*
       * TODO: following assertion is incorrect, see HDFS-2668 assert
       * storedBlock.findDatanode(dn) < 0 : "Block " + block +
       * " in recentInvalidatesSet should not appear in DN " + dn;
       */
      return storedBlock;
    }

    BlockToMarkCorrupt c = checkReplicaCorrupt(block, reportedState, storedBlock, ucState, dn); // 根据GS和长度，判断此副本是否损坏
    if (c != null) {
      if (shouldPostponeBlocksFromFuture) {
        // If the block is an out-of-date generation stamp or state,
        // but we're the standby, we shouldn't treat it as corrupt,
        // but instead just queue it for later processing.
        // TODO: Pretty confident this should be s/storedBlock/block below,
        // since we should be postponing the info of the reported block, not
        // the stored block. See HDFS-6289 for more context.
        queueReportedBlock(storageInfo, storedBlock, reportedState,
            QUEUE_REASON_CORRUPT_STATE);
      } else {
        toCorrupt.add(c); // 损坏的副本，加入到toInvalidate中
      }
      return storedBlock;
    }

    // 判断此Block，是否处于UC状态
    if (isBlockUnderConstruction(storedBlock, ucState, reportedState)) {
      toUC.add(new StatefulBlockInfo((BlockInfoContiguousUnderConstruction) storedBlock,
          new Block(block), reportedState)); // 添加到toUC中
      return storedBlock;
    }

    // Add replica if appropriate. If the replica was previously corrupt
    // but now okay, it might need to be updated.
    if (reportedState == ReplicaState.FINALIZED
        && (storedBlock.findStorageInfo(storageInfo) == -1 ||      // 之前未收到这个节点的这个副本汇报
            corruptReplicas.isReplicaCorrupt(storedBlock, dn))) {  // 之前这个节点上的这个副本损坏
      toAdd.add(storedBlock); // FINALIZED状态的副本，加入到toAdd中
    }
    return storedBlock;
  }

  /**
   * Queue the given reported block for later processing in the
   * standby node. @see PendingDataNodeMessages.
   * @param reason a textual reason to report in the debug logs
   */
  // Standby节点在没有收到edit信息之前，先收到了DN的块汇报，放到此集合，延迟处理
  private void queueReportedBlock(DatanodeStorageInfo storageInfo, Block block,
      ReplicaState reportedState, String reason) {
    assert shouldPostponeBlocksFromFuture;
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Queueing reported block " + block +
          " in state " + reportedState + 
          " from datanode " + storageInfo.getDatanodeDescriptor() +
          " for later processing because " + reason + ".");
    }
    pendingDNMessages.enqueueReportedBlock(storageInfo, block, reportedState);
  }

  /**
   * Try to process any messages that were previously queued for the given
   * block. This is called from FSEditLogLoader whenever a block's state
   * in the namespace has changed or a new block has been created.
   */
  // 回放edit的时候，会判断是否有延迟处理的副本信息，并处理
  public void processQueuedMessagesForBlock(Block b) throws IOException {
    Queue<ReportedBlockInfo> queue = pendingDNMessages.takeBlockQueue(b); // 获取指定block的延迟副本
    if (queue == null) {
      // Nothing to re-process
      return;
    }
    processQueuedMessages(queue);
  }
  
  private void processQueuedMessages(Iterable<ReportedBlockInfo> rbis)
      throws IOException {
    for (ReportedBlockInfo rbi : rbis) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Processing previouly queued message " + rbi);
      }
      if (rbi.getReportedState() == null) { // 被删除的block
        // This is a DELETE_BLOCK request
        DatanodeStorageInfo storageInfo = rbi.getStorageInfo();
        removeStoredBlock(rbi.getBlock(),
            storageInfo.getDatanodeDescriptor()); // 移除此副本信息
      } else {
        processAndHandleReportedBlock(rbi.getStorageInfo(), // 添加到相应集合
            rbi.getBlock(), rbi.getReportedState(), null);
      }
    }
  }
  
  /**
   * Process any remaining queued datanode messages after entering
   * active state. At this point they will not be re-queued since
   * we are the definitive master node and thus should be up-to-date
   * with the namespace information.
   */
  // Standby节点在没有收到edit信息之前，先收到了DN的块汇报，把这些块存到pendingDNMessages中
  // 切主的时候，会先回放到最新的edit，然后处理一下pendingDNMessages中的块汇报信息
  public void processAllPendingDNMessages() throws IOException {
    assert !shouldPostponeBlocksFromFuture :
      "processAllPendingDNMessages() should be called after disabling " +
      "block postponement.";
    int count = pendingDNMessages.count(); // 延后处理的块汇报消息
    if (count > 0) {
      LOG.info("Processing " + count + " messages from DataNodes " +
          "that were previously queued during standby state");
    }
    processQueuedMessages(pendingDNMessages.takeAll());
    assert pendingDNMessages.count() == 0;
  }

  /**
   * The next two methods test the various cases under which we must conclude
   * the replica is corrupt, or under construction.  These are laid out
   * as switch statements, on the theory that it is easier to understand
   * the combinatorics of reportedState and ucState that way.  It should be
   * at least as efficient as boolean expressions.
   * 
   * @return a BlockToMarkCorrupt object, or null if the replica is not corrupt
   */
  // 根据GS和长度，判断此副本是否损坏
  private BlockToMarkCorrupt checkReplicaCorrupt(
      Block reported, ReplicaState reportedState, 
      BlockInfoContiguous storedBlock, BlockUCState ucState,
      DatanodeDescriptor dn) {
    switch(reportedState) { // 汇报的副本状态
    case FINALIZED:
      switch(ucState) { // NN保存的块状态
      case COMPLETE:
      case COMMITTED:
        if (storedBlock.getGenerationStamp() != reported.getGenerationStamp()) {
          // DN汇报的GS和NN记录的不一致
          final long reportedGS = reported.getGenerationStamp();
          return new BlockToMarkCorrupt(storedBlock, reportedGS,
              "block is " + ucState + " and reported genstamp " + reportedGS
              + " does not match genstamp in block map "
              + storedBlock.getGenerationStamp(), Reason.GENSTAMP_MISMATCH);
        } else if (storedBlock.getNumBytes() != reported.getNumBytes()) {
          // DN汇报的长度和NN记录的不一致
          return new BlockToMarkCorrupt(storedBlock,
              "block is " + ucState + " and reported length " +
              reported.getNumBytes() + " does not match " +
              "length in block map " + storedBlock.getNumBytes(),
              Reason.SIZE_MISMATCH);
        } else {
          return null; // not corrupt
        }
      case UNDER_CONSTRUCTION:
        if (storedBlock.getGenerationStamp() > reported.getGenerationStamp()) {
          // 块在UC状态时，副本 reported.GS > storedBlock.GS 是正常的？
          // 可能的一种情况是：pipeline recovery 成功之后，才会向NN更新GS
          final long reportedGS = reported.getGenerationStamp();
          return new BlockToMarkCorrupt(storedBlock, reportedGS, "block is "
              + ucState + " and reported state " + reportedState
              + ", But reported genstamp " + reportedGS
              + " does not match genstamp in block map "
              + storedBlock.getGenerationStamp(), Reason.GENSTAMP_MISMATCH);
        }
        return null;
      default:
        return null;
      }
    case RBW:
    case RWR:
      if (!storedBlock.isComplete()) { // DN汇报的块状态和NN记录的，都是UC状态，则不做其他判断，认为此block没有损坏
        return null; // not corrupt
      } else if (storedBlock.getGenerationStamp() != reported.getGenerationStamp()) {
        final long reportedGS = reported.getGenerationStamp();
        return new BlockToMarkCorrupt(storedBlock, reportedGS,
            "reported " + reportedState + " replica with genstamp " + reportedGS
            + " does not match COMPLETE block's genstamp in block map "
            + storedBlock.getGenerationStamp(), Reason.GENSTAMP_MISMATCH);
      } else { // COMPLETE block, same genstamp
        if (reportedState == ReplicaState.RBW) {
          // If it's a RBW report for a COMPLETE block, it may just be that
          // the block report got a little bit delayed after the pipeline
          // closed. So, ignore this report, assuming we will get a
          // FINALIZED replica later. See HDFS-2791
          LOG.info("Received an RBW replica for " + storedBlock +
              " on " + dn + ": ignoring it, since it is " +
              "complete with the same genstamp");
          return null;
        } else {
          return new BlockToMarkCorrupt(storedBlock,
              "reported replica has invalid state " + reportedState,
              Reason.INVALID_STATE);
        }
      }
    case RUR:       // should not be reported
    case TEMPORARY: // should not be reported
    default:
      String msg = "Unexpected replica state " + reportedState
      + " for block: " + storedBlock + 
      " on " + dn + " size " + storedBlock.getNumBytes();
      // log here at WARN level since this is really a broken HDFS invariant
      LOG.warn(msg);
      return new BlockToMarkCorrupt(storedBlock, msg, Reason.INVALID_STATE);
    }
  }

  private boolean isBlockUnderConstruction(BlockInfoContiguous storedBlock,
      BlockUCState ucState, ReplicaState reportedState) { // 判断此Block，是否处于UC状态
    switch(reportedState) {
    case FINALIZED:
      switch(ucState) {
      case UNDER_CONSTRUCTION:
      case UNDER_RECOVERY:
        return true;
      default:
        return false;
      }
    case RBW:
    case RWR:
      return (!storedBlock.isComplete());
    case RUR:       // should not be reported                                                                                             
    case TEMPORARY: // should not be reported                                                                                             
    default:
      return false;
    }
  }

  void addStoredBlockUnderConstruction(StatefulBlockInfo ucBlock,
      DatanodeStorageInfo storageInfo) throws IOException {
    BlockInfoContiguousUnderConstruction block = ucBlock.storedBlock;
    block.addReplicaIfNotPresent(
        storageInfo, ucBlock.reportedBlock, ucBlock.reportedState); // 更新此副本信息，只更新replicas，并不会添加到triplets中

    if (ucBlock.reportedState == ReplicaState.FINALIZED &&
        !block.findDatanode(storageInfo.getDatanodeDescriptor())) { // 此副本已经完成，并且没有汇报过
      // 添加此副本信息到block和DN中
      addStoredBlock(block, storageInfo, null, true);
    }
  } 

  /**
   * Faster version of {@link #addStoredBlock},
   * intended for use with initial block report at startup. If not in startup
   * safe mode, will call standard addStoredBlock(). Assumes this method is
   * called "immediately" so there is no need to refresh the storedBlock from
   * blocksMap. Doesn't handle underReplication/overReplication, or worry about
   * pendingReplications or corruptReplicas, because it's in startup safe mode.
   * Doesn't log every block, because there are typically millions of them.
   * 
   * @throws IOException
   */
  // 启动时，Storage的第一次全量块汇报，直接添加block，不维护BM中的副本集合
  private void addStoredBlockImmediate(BlockInfoContiguous storedBlock,
      DatanodeStorageInfo storageInfo)
  throws IOException {
    assert (storedBlock != null && namesystem.hasWriteLock());
    if (!namesystem.isInStartupSafeMode() // 退出安全模式后，则使用正常的addStoredBlock()方法
        || namesystem.isPopulatingReplQueues()) {
      addStoredBlock(storedBlock, storageInfo, null, false);
      return;
    }

    // just add it
    AddBlockResult result = storageInfo.addBlock(storedBlock);

    // Now check for completion of blocks and safe block count
    int numCurrentReplica = countLiveNodes(storedBlock);
    if (storedBlock.getBlockUCState() == BlockUCState.COMMITTED
        && numCurrentReplica >= minReplication) { // 达到最小副本数committed状态的block，则complete
      completeBlock(storedBlock.getBlockCollection(), storedBlock, false);
    } else if (storedBlock.isComplete() && result == AddBlockResult.ADDED) {
      // check whether safe replication is reached for the block
      // only complete blocks are counted towards that.
      // In the case that the block just became complete above, completeBlock()
      // handles the safe block count maintenance.
      namesystem.incrementSafeBlockCount(numCurrentReplica);
    }
  }

  /**
   * Modify (block-->datanode) map. Remove block from set of
   * needed replications if this takes care of the problem.
   * @return the block that is stored in blockMap.
   */
  // 添加此副本信息到block和DN中，并且维护BM相应集合
  private Block addStoredBlock(final BlockInfoContiguous block,
                               DatanodeStorageInfo storageInfo,
                               DatanodeDescriptor delNodeHint,
                               boolean logEveryBlock)
  throws IOException {
    assert block != null && namesystem.hasWriteLock();
    BlockInfoContiguous storedBlock;
    DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();
    if (block instanceof BlockInfoContiguousUnderConstruction) {
      //refresh our copy in case the block got completed in another thread
      storedBlock = blocksMap.getStoredBlock(block);
    } else {
      storedBlock = block;
    }
    if (storedBlock == null || storedBlock.getBlockCollection() == null) { // 说明此block已经被删除
      // If this block does not belong to anyfile, then we are done.
      blockLog.info("BLOCK* addStoredBlock: {} on {} size {} but it does not" +
          " belong to any file", block, node, block.getNumBytes());

      // we could add this block to invalidate set of this datanode.
      // it will happen in next block report otherwise.
      return block;
    }
    BlockCollection bc = storedBlock.getBlockCollection();
    assert bc != null : "Block must belong to a file";

    // add block to the datanode
    // finalize的副本，才会调用此方法，添加到block的triplets和dn的storage中
    AddBlockResult result = storageInfo.addBlock(storedBlock); // 添加此副本信息到block和DN中

    int curReplicaDelta;
    if (result == AddBlockResult.ADDED) {
      curReplicaDelta = 1;
      if (logEveryBlock) {
        logAddStoredBlock(storedBlock, node);
      }
    } else if (result == AddBlockResult.REPLACED) {
      curReplicaDelta = 0;
      blockLog.warn("BLOCK* addStoredBlock: block {} moved to storageType " +
          "{} on node {}", storedBlock, storageInfo.getStorageType(), node);
    } else {
      // if the same block is added again and the replica was corrupt
      // previously because of a wrong gen stamp, remove it from the
      // corrupt block list.
      corruptReplicas.removeFromCorruptReplicasMap(block, node,
          Reason.GENSTAMP_MISMATCH);
      curReplicaDelta = 0;
      blockLog.warn("BLOCK* addStoredBlock: Redundant addStoredBlock request"
              + " received for {} on node {} size {}", storedBlock, node,
          storedBlock.getNumBytes());
    }

    // Now check for completion of blocks and safe block count
    NumberReplicas num = countNodes(storedBlock);
    int numLiveReplicas = num.liveReplicas();
    int numCurrentReplica = numLiveReplicas
      + pendingReplications.getNumReplicas(storedBlock);

    if(storedBlock.getBlockUCState() == BlockUCState.COMMITTED &&
        numLiveReplicas >= minReplication) { // 达到最小副本数，complete block
      storedBlock = completeBlock(bc, storedBlock, false);
    } else if (storedBlock.isComplete() && result == AddBlockResult.ADDED) {
      // check whether safe replication is reached for the block
      // only complete blocks are counted towards that
      // Is no-op if not in safe mode.
      // In the case that the block just became complete above, completeBlock()
      // handles the safe block count maintenance.
      namesystem.incrementSafeBlockCount(numCurrentReplica); // 安全模式相关：维护blockSafe字段
    }
    
    // if file is under construction, then done for now
    if (bc.isUnderConstruction()) { // 正在构建中的file，不判断副本数是否达到期望等逻辑
      return storedBlock;
    }

    // do not try to handle over/under-replicated blocks during first safe mode
    if (!namesystem.isPopulatingReplQueues()) {
      return storedBlock;
    }

    // handle underReplication/overReplication
    short fileReplication = bc.getBlockReplication();
    // 副本数是否达到期望
    if (!isNeededReplication(storedBlock, fileReplication, numCurrentReplica)) {
      // 副本数充足，则不需要复制副本
      neededReplications.remove(storedBlock, numCurrentReplica,
          num.decommissionedReplicas(), fileReplication);
    } else {
      // 副本数不足
      updateNeededReplications(storedBlock, curReplicaDelta, 0);
    }
    if (numCurrentReplica > fileReplication) {
      // 副本数高于期望值
      processOverReplicatedBlock(storedBlock, fileReplication, node, delNodeHint);
    }
    // If the file replication has reached desired value
    // we can remove any corrupt replicas the block may have
    int corruptReplicasCount = corruptReplicas.numCorruptReplicas(storedBlock);
    int numCorruptNodes = num.corruptReplicas();
    if (numCorruptNodes != corruptReplicasCount) {
      LOG.warn("Inconsistent number of corrupt replicas for " +
          storedBlock + "blockMap has " + numCorruptNodes + 
          " but corrupt replicas map has " + corruptReplicasCount);
    }
    if ((corruptReplicasCount > 0) && (numLiveReplicas >= fileReplication)) {
      // 副本数多余期望副本数，把损坏的副本删除
      invalidateCorruptReplicas(storedBlock);
    }
    return storedBlock;
  }

  private void logAddStoredBlock(BlockInfoContiguous storedBlock,
      DatanodeDescriptor node) {
    if (!blockLog.isInfoEnabled()) {
      return;
    }
    
    StringBuilder sb = new StringBuilder(500);
    sb.append("BLOCK* addStoredBlock: blockMap updated: ")
      .append(node)
      .append(" is added to ");
    storedBlock.appendStringTo(sb);
    sb.append(" size " )
      .append(storedBlock.getNumBytes());
    blockLog.info(sb.toString());
  }
  /**
   * Invalidate corrupt replicas.
   * <p>
   * This will remove the replicas from the block's location list,
   * add them to {@link #invalidateBlocks} so that they could be further
   * deleted from the respective data-nodes,
   * and remove the block from corruptReplicasMap.
   * <p>
   * This method should be called when the block has sufficient
   * number of live replicas.
   *
   * @param blk Block whose corrupt replicas need to be invalidated
   */
  private void invalidateCorruptReplicas(BlockInfoContiguous blk) { // 删除此block损坏的副本
    Collection<DatanodeDescriptor> nodes = corruptReplicas.getNodes(blk);
    boolean removedFromBlocksMap = true;
    if (nodes == null)
      return;
    // make a copy of the array of nodes in order to avoid
    // ConcurrentModificationException, when the block is removed from the node
    DatanodeDescriptor[] nodesCopy = nodes.toArray(new DatanodeDescriptor[0]);
    for (DatanodeDescriptor node : nodesCopy) {
      try {
        if (!invalidateBlock(new BlockToMarkCorrupt(blk, null,
              Reason.ANY), node)) { // 添加到带删除集合，并从NN内存中移除此副本信息
          removedFromBlocksMap = false;
        }
      } catch (IOException e) {
        blockLog.info("invalidateCorruptReplicas error in deleting bad block"
            + " {} on {}", blk, node, e);
        removedFromBlocksMap = false;
      }
    }
    // Remove the block from corruptReplicasMap
    if (removedFromBlocksMap) { // 说明添加到invalidateBlocks集合成功
      corruptReplicas.removeFromCorruptReplicasMap(blk);
    }
  }

  /**
   * For each block in the name-node verify whether it belongs to any file,
   * over or under replicated. Place it into the respective queue.
   *
   * 三种情况，会调用此方法：
   * 1、离开安全模式
   * 2、切换为Active
   * 3、第一次有多机架的时候
   *
   * 异步扫描所有的block，扫描一遍线程就退出
   *
   * 无效block，加入invalidateBlocks队列
   * 低于预期副本数的block，加入neededReplications队列
   * 超过预期副本数的block，调用processOverReplicatedBlock()处理
   */
  public void processMisReplicatedBlocks() {
    assert namesystem.hasWriteLock();
    stopReplicationInitializer(); // 先停止
    neededReplications.clear();
    replicationQueuesInitializer = new Daemon() {

      @Override
      public void run() {
        try {
          processMisReplicatesAsync();
        } catch (InterruptedException ie) {
          LOG.info("Interrupted while processing replication queues.");
        } catch (Exception e) {
          LOG.error("Error while processing replication queues async", e);
        }
      }
    };
    replicationQueuesInitializer.setName("Replication Queue Initializer");
    replicationQueuesInitializer.start(); // 再启动
  }

  /*
   * Stop the ongoing initialisation of replication queues
   */
  private void stopReplicationInitializer() {
    if (replicationQueuesInitializer != null) {
      replicationQueuesInitializer.interrupt();
      try {
        replicationQueuesInitializer.join();
      } catch (final InterruptedException e) {
        LOG.warn("Interrupted while waiting for replicationQueueInitializer. Returning..");
        return;
      } finally {
        replicationQueuesInitializer = null;
      }
    }
  }

  /*
   * Since the BlocksMapGset does not throw the ConcurrentModificationException
   * and supports further iteration after modification to list, there is a
   * chance of missing the newly added block while iterating. Since every
   * addition to blocksMap will check for mis-replication, missing mis-replication
   * check for new blocks will not be a problem.
   */
  private void processMisReplicatesAsync() throws InterruptedException {
    long nrInvalid = 0, nrOverReplicated = 0;
    long nrUnderReplicated = 0, nrPostponed = 0, nrUnderConstruction = 0;
    long startTimeMisReplicatedScan = Time.monotonicNow();
    Iterator<BlockInfoContiguous> blocksItr = blocksMap.getBlocks().iterator();
    long totalBlocks = blocksMap.size();
    replicationQueuesInitProgress = 0; // 整体进度
    long totalProcessed = 0;
    long sleepDuration =
        Math.max(1, Math.min(numBlocksPerIteration/1000, 10000)); // 10ms

    // 扫描所有的block
    while (namesystem.isRunning() && !Thread.currentThread().isInterrupted()) {
      int processed = 0;
      namesystem.writeLockInterruptibly();
      try {
        while (processed < numBlocksPerIteration && blocksItr.hasNext()) { // 每次持锁，最多处理10000个block
          BlockInfoContiguous block = blocksItr.next();
          MisReplicationResult res = processMisReplicatedBlock(block);
          if (LOG.isTraceEnabled()) {
            LOG.trace("block " + block + ": " + res);
          }
          switch (res) {
          case UNDER_REPLICATED: // 低于预期副本数
            nrUnderReplicated++;
            break;
          case OVER_REPLICATED: // 高于预期副本数
            nrOverReplicated++;
            break;
          case INVALID: // 无效
            nrInvalid++;
            break;
          case POSTPONE: // 推迟
            nrPostponed++;
            postponeBlock(block);
            break;
          case UNDER_CONSTRUCTION: // 正在构建
            nrUnderConstruction++;
            break;
          case OK: // 正常
            break;
          default:
            throw new AssertionError("Invalid enum value: " + res);
          }
          processed++;
        }
        totalProcessed += processed;
        // there is a possibility that if any of the blocks deleted/added during
        // initialisation, then progress might be different.
        replicationQueuesInitProgress = Math.min((double) totalProcessed
            / totalBlocks, 1.0); // 整体进度

        if (!blocksItr.hasNext()) { // blocksMap中所有的block都处理完了
          LOG.info("Total number of blocks            = " + blocksMap.size());
          LOG.info("Number of invalid blocks          = " + nrInvalid);
          LOG.info("Number of under-replicated blocks = " + nrUnderReplicated);
          LOG.info("Number of  over-replicated blocks = " + nrOverReplicated
              + ((nrPostponed > 0) ? (" (" + nrPostponed + " postponed)") : ""));
          LOG.info("Number of blocks being written    = " + nrUnderConstruction);
          NameNode.stateChangeLog
              .info("STATE* Replication Queue initialization "
                  + "scan for invalid, over- and under-replicated blocks "
                  + "completed in "
                  + (Time.monotonicNow() - startTimeMisReplicatedScan)
                  + " msec");
          break;
        }
      } finally {
        namesystem.writeUnlock();
        // Make sure it is out of the write lock for sufficiently long time.
        Thread.sleep(sleepDuration); // sleep 10ms
      }
    }
    if (Thread.currentThread().isInterrupted()) {
      LOG.info("Interrupted while processing replication queues.");
    }
  }

  /**
   * Get the progress of the Replication queues initialisation
   * 
   * @return Returns values between 0 and 1 for the progress.
   */
  public double getReplicationQueuesInitProgress() {
    return replicationQueuesInitProgress;
  }

  /**
   * Process a single possibly misreplicated block. This adds it to the
   * appropriate queues if necessary, and returns a result code indicating
   * what happened with it.
   *
   * 无效block，加入invalidateBlocks队列
   * 低于预期副本数的block，加入neededReplications队列
   * 超过预期副本数的block，调用processOverReplicatedBlock()处理
   */
  private MisReplicationResult processMisReplicatedBlock(BlockInfoContiguous block) {
    BlockCollection bc = block.getBlockCollection();
    if (bc == null) {
      // block does not belong to any file
      addToInvalidates(block); // 不属于任何inode，无效block
      return MisReplicationResult.INVALID;
    }
    if (!block.isComplete()) {
      // Incomplete blocks are never considered mis-replicated --
      // they'll be reached when they are completed or recovered.
      return MisReplicationResult.UNDER_CONSTRUCTION; // 正在构建
    }
    // calculate current replication
    short expectedReplication = bc.getBlockReplication();
    NumberReplicas num = countNodes(block);
    int numCurrentReplica = num.liveReplicas();
    // add to under-replicated queue if need to be
    if (isNeededReplication(block, expectedReplication, numCurrentReplica)) { // 低于预期副本数
      if (neededReplications.add(block, numCurrentReplica, num
          .decommissionedReplicas(), expectedReplication)) {
        return MisReplicationResult.UNDER_REPLICATED;
      }
    }

    if (numCurrentReplica > expectedReplication) { // 多余预期副本数
      if (num.replicasOnStaleNodes() > 0) { // stale DN节点上的副本
        // If any of the replicas of this block are on nodes that are
        // considered "stale", then these replicas may in fact have
        // already been deleted. So, we cannot safely act on the
        // over-replication until a later point in time, when
        // the "stale" nodes have block reported.
        return MisReplicationResult.POSTPONE;
      }
      
      // over-replicated block
      processOverReplicatedBlock(block, expectedReplication, null, null);
      return MisReplicationResult.OVER_REPLICATED;
    }
    
    return MisReplicationResult.OK;
  }
  
  /** Set replication for the blocks. */
  public void setReplication(final short oldRepl, final short newRepl,
      final String src, final Block... blocks) {
    if (newRepl == oldRepl) {
      return;
    }

    // update needReplication priority queues
    for(Block b : blocks) {
      updateNeededReplications(b, 0, newRepl-oldRepl);
    }
      
    if (oldRepl > newRepl) {
      // old replication > the new one; need to remove copies
      LOG.info("Decreasing replication from " + oldRepl + " to " + newRepl
          + " for " + src);
      for(Block b : blocks) {
        processOverReplicatedBlock(b, newRepl, null, null);
      }
    } else { // replication factor is increased
      LOG.info("Increasing replication from " + oldRepl + " to " + newRepl
          + " for " + src);
    }
  }

  /**
   * Find how many of the containing nodes are "extra", if any.
   * If there are any extras, call chooseExcessReplicates() to
   * mark them in the excessReplicateMap.
   */
  // 副本数高于期望值
  private void processOverReplicatedBlock(final Block block,
      final short replication, final DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint) {
    assert namesystem.hasWriteLock();
    if (addedNode == delNodeHint) {
      delNodeHint = null;
    }
    Collection<DatanodeStorageInfo> nonExcess = new ArrayList<DatanodeStorageInfo>(); // 正常的副本（排除掉正在decommission和损坏的副本）
    Collection<DatanodeDescriptor> corruptNodes = corruptReplicas.getNodes(block);
    // 选出符合条件的DN节点目录
    for(DatanodeStorageInfo storage : blocksMap.getStorages(block, State.NORMAL)) { // 此block的所有副本位置信息
      final DatanodeDescriptor cur = storage.getDatanodeDescriptor();
      if (storage.areBlockContentsStale()) {
        LOG.trace("BLOCK* processOverReplicatedBlock: Postponing {}"
            + " since storage {} does not yet have up-to-date information.",
            block, storage);
        postponeBlock(block);
        return;
      }
      LightWeightLinkedSet<Block> excessBlocks = excessReplicateMap.get(cur.getDatanodeUuid());
      if (excessBlocks == null || !excessBlocks.contains(block)) { // 此副本没有添加到多副本集合
        if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) { // 此副本所在的DN没有decommission
          // exclude corrupt replicas
          if (corruptNodes == null || !corruptNodes.contains(cur)) { // 此副本没有损坏
            nonExcess.add(storage);
          }
        }
      }
    }
    chooseExcessReplicates(nonExcess, block, replication, addedNode, delNodeHint, blockplacement);
  }


  /**
   * We want "replication" replicates for the block, but we now have too many.  
   * In this method, copy enough nodes from 'srcNodes' into 'dstNodes' such that:
   *
   * srcNodes.size() - dstNodes.size() == replication
   *
   * We pick node that make sure that replicas are spread across racks and
   * also try hard to pick one with least free space.
   * The algorithm is first to pick a node with least free space from nodes
   * that are on a rack holding more than one replicas of the block.
   * So removing such a replica won't remove a rack. 
   * If no such a node is available,
   * then pick a node with least free space
   */
  private void chooseExcessReplicates(final Collection<DatanodeStorageInfo> nonExcess, 
                              Block b, short replication,
                              DatanodeDescriptor addedNode,
                              DatanodeDescriptor delNodeHint,
                              BlockPlacementPolicy replicator) {
    assert namesystem.hasWriteLock();
    // first form a rack to datanodes map and
    BlockCollection bc = getBlockCollection(b);
    final BlockStoragePolicy storagePolicy = storagePolicySuite.getPolicy(bc.getStoragePolicyID()); // 此file的存储策略
    final List<StorageType> excessTypes = storagePolicy.chooseExcess( // 获取不符合期望的存储类型
        replication, DatanodeStorageInfo.toStorageTypes(nonExcess));


    final Map<String, List<DatanodeStorageInfo>> rackMap
        = new HashMap<String, List<DatanodeStorageInfo>>();
    final List<DatanodeStorageInfo> moreThanOne = new ArrayList<DatanodeStorageInfo>(); // 机架上有多于一个副本的DN列表
    final List<DatanodeStorageInfo> exactlyOne = new ArrayList<DatanodeStorageInfo>(); // 机架上只有一个副本的DN列表
    
    // split nodes into two sets
    // moreThanOne contains nodes on rack with more than one replica
    // exactlyOne contains the remaining nodes
    replicator.splitNodesWithRack(nonExcess, rackMap, moreThanOne, exactlyOne);
    
    // pick one node to delete that favors the delete hint
    // otherwise pick one with least space from priSet if it is not empty
    // otherwise one node with least space from remains
    boolean firstOne = true;
    final DatanodeStorageInfo delNodeHintStorage = DatanodeStorageInfo.getDatanodeStorageInfo(nonExcess, delNodeHint); // DN标记的删除副本线索
    final DatanodeStorageInfo addedNodeStorage = DatanodeStorageInfo.getDatanodeStorageInfo(nonExcess, addedNode); // addedNode汇报的此副本
    while (nonExcess.size() - replication > 0) { // 当前正常的副本数多余期望的副本数
      final DatanodeStorageInfo cur;
      if (useDelHint(firstOne, delNodeHintStorage, addedNodeStorage, moreThanOne, excessTypes)) {
        cur = delNodeHintStorage;
      } else { // regular excessive replica removal
        // 选择一个副本删除，优先选择可能已经挂了的DN所在的副本，否则选择存储空间最小的DN所在的副本
        cur = replicator.chooseReplicaToDelete(bc, b, replication, moreThanOne, exactlyOne, excessTypes);
      }
      firstOne = false;

      // adjust rackmap, moreThanOne, and exactlyOne
      replicator.adjustSetsWithChosenReplica(rackMap, moreThanOne, exactlyOne, cur); // 从rackMap、moreThanOne、exactlyOne中移除cur

      nonExcess.remove(cur);
      //
      // The 'excessblocks' tracks blocks until we get confirmation
      // that the datanode has deleted them; the only way we remove them
      // is when we get a "removeBlock" message.
      //
      // The 'invalidate' list is used to inform the datanode the block
      // should be deleted.  Items are removed from the invalidate list
      // upon giving instructions to the namenode.
      //

      // 删除多余副本流程：
      // 1、添加到invalidateBlocks和excessReplicateMap
      // 2、ReplicationMonitor.computeInvalidateWork从invalidateBlocks取出添加到DatanodeDescriptor.invalidateBlocks
      // 3、通过heartbeat下发给datanode
      // 4、datanode删除成功，发送IBR DELETED_BLOCK
      // 5、从excessReplicateMap中删除
      addToExcessReplicate(cur.getDatanodeDescriptor(), b); // 添加到多副本集合
      addToInvalidates(b, cur.getDatanodeDescriptor()); // 添加到待删除集合
      blockLog.info("BLOCK* chooseExcessReplicates: "
                +"({}, {}) is added to invalidated blocks set", cur, b);
    }
  }

  /** Check if we can use delHint */
  static boolean useDelHint(boolean isFirst, DatanodeStorageInfo delHint,
      DatanodeStorageInfo added, List<DatanodeStorageInfo> moreThan1Racks,
      List<StorageType> excessTypes) { // 是否从delHint节点
    if (!isFirst) {
      return false; // only consider delHint for the first case
    } else if (delHint == null) {
      return false; // no delHint
    } else if (!excessTypes.contains(delHint.getStorageType())) {
      return false; // delHint storage type is not an excess type
    } else {
      // check if removing delHint reduces the number of racks
      if (moreThan1Racks.contains(delHint)) { //
        return true; // delHint and some other nodes are under the same rack 
      } else if (added != null && !moreThan1Racks.contains(added)) {
        return true; // the added node adds a new rack
      }
      return false; // removing delHint reduces the number of racks;
    }
  }

  private void addToExcessReplicate(DatanodeInfo dn, Block block) {
    assert namesystem.hasWriteLock();
    LightWeightLinkedSet<Block> excessBlocks = excessReplicateMap.get(dn.getDatanodeUuid());
    if (excessBlocks == null) {
      excessBlocks = new LightWeightLinkedSet<Block>();
      excessReplicateMap.put(dn.getDatanodeUuid(), excessBlocks);
    }
    if (excessBlocks.add(block)) {
      excessBlocksCount.incrementAndGet();
      blockLog.debug("BLOCK* addToExcessReplicate: ({}, {}) is added to"
          + " excessReplicateMap", dn, block);
    }
  }

  private void removeStoredBlock(DatanodeStorageInfo storageInfo, Block block,
      DatanodeDescriptor node) {
    if (shouldPostponeBlocksFromFuture &&
        namesystem.isGenStampInFuture(block)) { // 延迟处理
      queueReportedBlock(storageInfo, block, null,
          QUEUE_REASON_FUTURE_GENSTAMP);
      return;
    }
    removeStoredBlock(block, node);
  }

  /**
   * Modify (block-->datanode) map. Possibly generate replication tasks, if the
   * removed block is still valid.
   */
  // 从NN内存中移除此副本信息，并且维护相应集合
  public void removeStoredBlock(Block block, DatanodeDescriptor node) {
    blockLog.debug("BLOCK* removeStoredBlock: {} from {}", block, node);
    assert (namesystem.hasWriteLock());
    {
      if (!blocksMap.removeNode(block, node)) { // 移除此副本信息（DN的blockList和block的storageList）
        blockLog.debug("BLOCK* removeStoredBlock: {} has already been" +
            " removed from node {}", block, node); // 说明已经被移除过了
        return;
      }

      //
      // It's possible that the block was removed because of a datanode
      // failure. If the block is still valid, check if replication is
      // necessary. In that case, put block on a possibly-will-
      // be-replicated list.
      //
      BlockCollection bc = blocksMap.getBlockCollection(block);
      if (bc != null) {
        namesystem.decrementSafeBlockCount(block); // 安全模式：维护blockSafe字段
        updateNeededReplications(block, -1, 0); // 更新少副本集合
      }

      //
      // We've removed a block from a node, so it's definitely no longer
      // in "excess" there.
      //
      // 从多副本集合移除
      LightWeightLinkedSet<Block> excessBlocks = excessReplicateMap.get(node
          .getDatanodeUuid());
      if (excessBlocks != null) {
        if (excessBlocks.remove(block)) {
          excessBlocksCount.decrementAndGet();
          blockLog.debug("BLOCK* removeStoredBlock: {} is removed from " +
              "excessBlocks", block);
          if (excessBlocks.size() == 0) {
            excessReplicateMap.remove(node.getDatanodeUuid());
          }
        }
      }

      // Remove the replica from corruptReplicas
      // 从损坏副本集合移除
      corruptReplicas.removeFromCorruptReplicasMap(block, node);
    }
  }

  /**
   * Get all valid locations of the block & add the block to results
   * return the length of the added block; 0 if the block is not added
   */
  private long addBlock(Block block, List<BlockWithLocations> results) {
    final List<DatanodeStorageInfo> locations = getValidLocations(block);
    if(locations.size() == 0) {
      return 0;
    } else {
      final String[] datanodeUuids = new String[locations.size()];
      final String[] storageIDs = new String[datanodeUuids.length];
      final StorageType[] storageTypes = new StorageType[datanodeUuids.length];
      for(int i = 0; i < locations.size(); i++) {
        final DatanodeStorageInfo s = locations.get(i);
        datanodeUuids[i] = s.getDatanodeDescriptor().getDatanodeUuid();
        storageIDs[i] = s.getStorageID();
        storageTypes[i] = s.getStorageType();
      }
      results.add(new BlockWithLocations(block, datanodeUuids, storageIDs,
          storageTypes));
      return block.getNumBytes();
    }
  }

  /**
   * The given node is reporting that it received a certain block.
   */
  @VisibleForTesting
  void addBlock(DatanodeStorageInfo storageInfo, Block block, String delHint)
      throws IOException {
    DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();
    // Decrement number of blocks scheduled to this datanode.
    // for a retry request (of DatanodeProtocol#blockReceivedAndDeleted with 
    // RECEIVED_BLOCK), we currently also decrease the approximate number.
    // 减少将要写到此DN上的Block数量
    // TODOWXY: 申请新块时，也会考虑此变量？
    node.decrementBlocksScheduled(storageInfo.getStorageType());

    // get the deletion hint node
    DatanodeDescriptor delHintNode = null;
    if (delHint != null && delHint.length() != 0) {
      delHintNode = datanodeManager.getDatanode(delHint);
      if (delHintNode == null) {
        blockLog.warn("BLOCK* blockReceived: {} is expected to be removed " +
            "from an unrecorded node {}", block, delHint);
      }
    }

    //
    // Modify the blocks->datanode map and node's map.
    //
    pendingReplications.decrement(block, node); // 移除复制请求
    processAndHandleReportedBlock(storageInfo, block, ReplicaState.FINALIZED, delHintNode);
  }
  
  private void processAndHandleReportedBlock(
      DatanodeStorageInfo storageInfo, Block block,
      ReplicaState reportedState, DatanodeDescriptor delHintNode) // delHintNode 用来干什么？
      throws IOException {
    // 每次只处理一个Block为何要初始化这么多Collection？
    // 因为processReportedBlock()是和全量块汇报共用的，全量块汇报一次会处理多个block
    // blockReceived reports a finalized block
    Collection<BlockInfoContiguous> toAdd = new LinkedList<BlockInfoContiguous>();
    Collection<Block> toInvalidate = new LinkedList<Block>();
    Collection<BlockToMarkCorrupt> toCorrupt = new LinkedList<BlockToMarkCorrupt>();
    Collection<StatefulBlockInfo> toUC = new LinkedList<StatefulBlockInfo>();
    final DatanodeDescriptor node = storageInfo.getDatanodeDescriptor();

    // 判断副本状态
    processReportedBlock(storageInfo, block, reportedState,
                              toAdd, toInvalidate, toCorrupt, toUC);
    // the block is only in one of the to-do lists
    // if it is in none then data-node already has it
    assert toUC.size() + toAdd.size() + toInvalidate.size() + toCorrupt.size() <= 1 // 此副本最多只能添加到一个集合中
      : "The block should be only in one of the lists.";

    for (StatefulBlockInfo b : toUC) {
      // 更新此副本信息
      addStoredBlockUnderConstruction(b, storageInfo);
    }
    long numBlocksLogged = 0;
    for (BlockInfoContiguous b : toAdd) {
      // 添加此副本信息到block和DN中
      addStoredBlock(b, storageInfo, delHintNode, numBlocksLogged < maxNumBlocksToLog);
      numBlocksLogged++;
    }
    if (numBlocksLogged > maxNumBlocksToLog) {
      blockLog.info("BLOCK* addBlock: logged info for {} of {} reported.",
          maxNumBlocksToLog, numBlocksLogged);
    }
    for (Block b : toInvalidate) { // NN blocksMap中没有，但是DN有
      blockLog.info("BLOCK* addBlock: block {} on node {} size {} does not " +
          "belong to any file", b, node, b.getNumBytes());
      addToInvalidates(b, node); // 添加到待删除集合
    }
    for (BlockToMarkCorrupt b : toCorrupt) { // GS或长度 和NN记录的不一致
      markBlockAsCorrupt(b, storageInfo, node); // 处理损坏的副本
    }
  }

  /**
   * The given node is reporting incremental information about some blocks.
   * This includes blocks that are starting to be received, completed being
   * received, or deleted.
   * 
   * This method must be called with FSNamesystem lock held.
   */
  public void processIncrementalBlockReport(final DatanodeID nodeID,
      final StorageReceivedDeletedBlocks srdb) throws IOException {
    assert namesystem.hasWriteLock();
    int received = 0;
    int deleted = 0;
    int receiving = 0;
    final DatanodeDescriptor node = datanodeManager.getDatanode(nodeID);
    if (node == null || !node.isAlive) { // 判断正在增量汇报的DN是否注册过
      blockLog.warn("BLOCK* processIncrementalBlockReport"
              + " is received from dead or unregistered node {}", nodeID);
      throw new IOException(
          "Got incremental block report from unregistered or dead node");
    }

    DatanodeStorageInfo storageInfo =
        node.getStorageInfo(srdb.getStorage().getStorageID());
    if (storageInfo == null) {
      // The DataNode is reporting an unknown storage. Usually the NN learns
      // about new storages from heartbeats but during NN restart we may
      // receive a block report or incremental report before the heartbeat.
      // We must handle this for protocol compatibility. This issue was
      // uncovered by HDFS-6094.
      storageInfo = node.updateStorage(srdb.getStorage());
    }

    for (ReceivedDeletedBlockInfo rdbi : srdb.getBlocks()) {
      switch (rdbi.getStatus()) {
      case DELETED_BLOCK: // DN已经删除的replica（DN删除成功、发现坏盘、下一块磁盘）
        removeStoredBlock(storageInfo, rdbi.getBlock(), node);
        deleted++;
        break;
      case RECEIVED_BLOCK: // DN已经接收完成的replica（Finalize状态）
        addBlock(storageInfo, rdbi.getBlock(), rdbi.getDelHints());
        received++;
        break;
      case RECEIVING_BLOCK: // DN正在接收的replica（RBW状态）
        receiving++;
        processAndHandleReportedBlock(storageInfo, rdbi.getBlock(),
                                      ReplicaState.RBW, null);
        break;
      default:
        String msg = 
          "Unknown block status code reported by " + nodeID +
          ": " + rdbi;
        blockLog.warn(msg);
        assert false : msg; // if assertions are enabled, throw.
        break;
      }
      blockLog.debug("BLOCK* block {}: {} is received from {}",
          rdbi.getStatus(), rdbi.getBlock(), nodeID);
    }
    blockLog.debug("*BLOCK* NameNode.processIncrementalBlockReport: from "
            + "{} receiving: {}, received: {}, deleted: {}", nodeID, receiving,
        received, deleted);
  }

  /**
   * Return the number of nodes hosting a given block, grouped
   * by the state of those replicas.
   */
  // 统计各种情况的副本个数
  public NumberReplicas countNodes(Block b) {
    int decommissioned = 0;
    int live = 0;
    int corrupt = 0;
    int excess = 0;
    int stale = 0;
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(b);
    for(DatanodeStorageInfo storage : blocksMap.getStorages(b, State.NORMAL)) { // 从blocksMap中取最新的block信息
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      // 如果正常的副本和损坏的副本在一个DN上，有可能会误判？
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node))) { // 记录在损坏集合中，说明此副本损坏
        corrupt++; // 损坏的副本数
      } else if (node.isDecommissionInProgress() || node.isDecommissioned()) { // 节点正在decommission
        decommissioned++; // decommission的副本数
      } else {
        LightWeightLinkedSet<Block> blocksExcess = excessReplicateMap.get(node
            .getDatanodeUuid());
        if (blocksExcess != null && blocksExcess.contains(b)) {
          excess++; // 多余的副本数
        } else {
          live++; // 正常的副本数（并不区分副本状态）
        }
      }
      if (storage.areBlockContentsStale()) {
        stale++; // 副本所在的节点有多少是stale状态
      }
    }
    return new NumberReplicas(live, decommissioned, corrupt, excess, stale);
  }

  /** 
   * Simpler, faster form of {@link #countNodes(Block)} that only returns the number
   * of live nodes.  If in startup safemode (or its 30-sec extension period),
   * then it gains speed by ignoring issues of excess replicas or nodes
   * that are decommissioned or in process of becoming decommissioned.
   * If not in startup, then it calls {@link #countNodes(Block)} instead.
   * 
   * @param b - the block being tested
   * @return count of live nodes for this block
   */
  int countLiveNodes(BlockInfoContiguous b) {
    if (!namesystem.isInStartupSafeMode()) {
      return countNodes(b).liveReplicas();
    }
    // else proceed with fast case
    int live = 0;
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(b);
    for(DatanodeStorageInfo storage : blocksMap.getStorages(b, State.NORMAL)) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      if ((nodesCorrupt == null) || (!nodesCorrupt.contains(node)))
        live++;
    }
    return live;
  }
  
  /**
   * On stopping decommission, check if the node has excess replicas.
   * If there are any excess replicas, call processOverReplicatedBlock().
   * Process over replicated blocks only when active NN is out of safe mode.
   */
  void processOverReplicatedBlocksOnReCommission(
      final DatanodeDescriptor srcNode) { // 由于取消decommission，需要对超出期望副本数的block进行处理
    if (!namesystem.isPopulatingReplQueues()) {
      return;
    }
    final Iterator<? extends Block> it = srcNode.getBlockIterator();
    int numOverReplicated = 0;
    while(it.hasNext()) { // 一个block一个block判断并处理
      final Block block = it.next();
      BlockCollection bc = blocksMap.getBlockCollection(block);
      short expectedReplication = bc.getBlockReplication();
      NumberReplicas num = countNodes(block); // 统计副本数
      int numCurrentReplica = num.liveReplicas();
      if (numCurrentReplica > expectedReplication) { // 是否大于期望副本数
        // over-replicated block 
        processOverReplicatedBlock(block, expectedReplication, null, null); // 处理高于期望副本数的block
        numOverReplicated++;
      }
    }
    LOG.info("Invalidated " + numOverReplicated + " over-replicated blocks on " +
        srcNode + " during recommissioning");
  }

  /**
   * Returns whether a node can be safely decommissioned based on its 
   * liveness. Dead nodes cannot always be safely decommissioned.
   */
  boolean isNodeHealthyForDecommission(DatanodeDescriptor node) { // 此DN是否能被安全的标记为DECOMMISSIONED
    if (!node.checkBlockReportReceived()) { // 此节点没有发送过全量块汇报
      LOG.info("Node {} hasn't sent its first block report.", node);
      return false;
    }

    if (node.isAlive) {
      return true;
    }

    // 如果正在decommission的DN挂了
    updateState();
    if (pendingReplicationBlocksCount == 0 && // 复制列表是否为空
        underReplicatedBlocksCount == 0) { // 少副本列表是否为空
      LOG.info("Node {} is dead and there are no under-replicated" +
          " blocks or blocks pending replication. Safe to decommission.", 
          node);
      return true;
    }

    LOG.warn("Node {} is dead " +
        "while decommission is in progress. Cannot be safely " +
        "decommissioned since there is risk of reduced " +
        "data durability or data loss. Either restart the failed node or" +
        " force decommissioning by removing, calling refreshNodes, " +
        "then re-adding to the excludes files.", node);
    return false;
  }

  public int getActiveBlockCount() {
    return blocksMap.size();
  }

  public DatanodeStorageInfo[] getStorages(BlockInfoContiguous block) { // 获取此block的副本位置
    final DatanodeStorageInfo[] storages = new DatanodeStorageInfo[block.numNodes()];
    int i = 0;
    for(DatanodeStorageInfo s : blocksMap.getStorages(block)) {
      storages[i++] = s;
    }
    return storages;
  }

  public int getTotalBlocks() {
    return blocksMap.size();
  }

  public void removeBlock(Block block) { // 内存中删除block
    assert namesystem.hasWriteLock();
    // No need to ACK blocks that are being removed entirely
    // from the namespace, since the removal of the associated
    // file already removes them from the block map below.
    block.setNumBytes(BlockCommand.NO_ACK); // 告诉DN不需要增量块汇报，这个删除的block
    addToInvalidates(block); // 添加到invalidate队列
    removeBlockFromMap(block); // 内存中删除block
    // Remove the block from pendingReplications and neededReplications
    pendingReplications.remove(block); // 从复制队列中删除
    neededReplications.remove(block, UnderReplicatedBlocks.LEVEL);
    if (postponedMisreplicatedBlocks.remove(block)) { // 从延迟处理队列删除
      postponedMisreplicatedBlocksCount.decrementAndGet();
    }
  }

  public BlockInfoContiguous getStoredBlock(Block block) {// 通过blocksMap获取BlockInfoContiguous对象
    return blocksMap.getStoredBlock(block);
  }

  /** updates a block in under replication queue */
  // 更新少副本集合
  private void updateNeededReplications(final Block block,
      final int curReplicasDelta, int expectedReplicasDelta) {
    namesystem.writeLock();
    try {
      if (!namesystem.isPopulatingReplQueues()) {
        return;
      }
      NumberReplicas repl = countNodes(block);
      int curExpectedReplicas = getReplication(block);
      if (isNeededReplication(block, curExpectedReplicas, repl.liveReplicas())) { // 副本数不足
        neededReplications.update(block, repl.liveReplicas(), repl
            .decommissionedReplicas(), curExpectedReplicas, curReplicasDelta,
            expectedReplicasDelta);
      } else { // 副本数充足
        int oldReplicas = repl.liveReplicas()-curReplicasDelta;
        int oldExpectedReplicas = curExpectedReplicas-expectedReplicasDelta;
        neededReplications.remove(block, oldReplicas, repl.decommissionedReplicas(),
                                  oldExpectedReplicas);
      }
    } finally {
      namesystem.writeUnlock();
    }
  }

  /**
   * Check replication of the blocks in the collection.
   * If any block is needed replication, insert it into the replication queue.
   * Otherwise, if the block is more than the expected replication factor,
   * process it as an over replicated block.
   */
  public void checkReplication(BlockCollection bc) { // 检查此文件的所有block副本数是否符合期望
    final short expected = bc.getBlockReplication();
    for (Block block : bc.getBlocks()) {
      final NumberReplicas n = countNodes(block);
      if (isNeededReplication(block, expected, n.liveReplicas())) { // 副本数是否低于期望
        neededReplications.add(block, n.liveReplicas(),
            n.decommissionedReplicas(), expected);
      } else if (n.liveReplicas() > expected) { // 副本数是否高于期望
        processOverReplicatedBlock(block, expected, null, null);
      }
    }
  }

  /** 
   * @return 0 if the block is not found;
   *         otherwise, return the replication factor of the block.
   */
  private int getReplication(Block block) { // 期望的副本数
    final BlockCollection bc = blocksMap.getBlockCollection(block);
    return bc == null? 0: bc.getBlockReplication();
  }


  /**
   * Get blocks to invalidate for <i>nodeId</i>
   * in {@link #invalidateBlocks}.
   *
   * @return number of blocks scheduled for removal during this iteration.
   */
  private int invalidateWorkForOneNode(DatanodeInfo dn) {
    final List<Block> toInvalidate;
    
    namesystem.writeLock();
    try {
      // blocks should not be replicated or removed if safe mode is on
      if (namesystem.isInSafeMode()) {
        LOG.debug("In safemode, not computing replication work");
        return 0;
      }
      try {
        DatanodeDescriptor dnDescriptor = datanodeManager.getDatanode(dn);
        if (dnDescriptor == null) {
          LOG.warn("DataNode " + dn + " cannot be found with UUID " +
              dn.getDatanodeUuid() + ", removing block invalidation work.");
          invalidateBlocks.remove(dn);
          return 0;
        }
        toInvalidate = invalidateBlocks.invalidateWork(dnDescriptor);
        
        if (toInvalidate == null) {
          return 0;
        }
      } catch(UnregisteredNodeException une) {
        return 0;
      }
    } finally {
      namesystem.writeUnlock();
    }
    blockLog.info("BLOCK* {}: ask {} to delete {}", getClass().getSimpleName(),
        dn, toInvalidate);
    return toInvalidate.size();
  }

  boolean blockHasEnoughRacks(Block b) {
    boolean enoughRacks = false;;
    Collection<DatanodeDescriptor> corruptNodes = 
                                  corruptReplicas.getNodes(b);
    int numExpectedReplicas = getReplication(b); // 期望的副本数
    String rackName = null;
    for(DatanodeStorageInfo storage : blocksMap.getStorages(b)) {
      final DatanodeDescriptor cur = storage.getDatanodeDescriptor();
      if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) { //此DN没有decommission
        if ((corruptNodes == null ) || !corruptNodes.contains(cur)) {
          if (numExpectedReplicas == 1 ||
              (numExpectedReplicas > 1 &&
                  !datanodeManager.hasClusterEverBeenMultiRack())) {
            // 只有一个副本或只有一个机架
            enoughRacks = true;
            break;
          }
          String rackNameNew = cur.getNetworkLocation();
          if (rackName == null) {
            rackName = rackNameNew;
          } else if (!rackName.equals(rackNameNew)) {
            // 说明副本至少在两个机架上存在
            enoughRacks = true;
            break;
          }
        }
      }
    }
    return enoughRacks;
  }

  /**
   * A block needs replication if the number of replicas is less than expected
   * or if it does not have enough racks.
   */
  boolean isNeededReplication(Block b, int expected, int current) { // 副本数低于期望或者不符合机架放置策略
    return current < expected || !blockHasEnoughRacks(b);
  }
  
  public long getMissingBlocksCount() {
    // not locking
    return this.neededReplications.getCorruptBlockSize();
  }

  public long getMissingReplOneBlocksCount() {
    // not locking
    return this.neededReplications.getCorruptReplOneBlockSize();
  }

  public BlockInfoContiguous addBlockCollection(BlockInfoContiguous block,
      BlockCollection bc) {
    return blocksMap.addBlockCollection(block, bc);
  }

  public BlockCollection getBlockCollection(Block b) {
    return blocksMap.getBlockCollection(b);
  }

  /** @return an iterator of the datanodes. */
  public Iterable<DatanodeStorageInfo> getStorages(final Block block) {
    return blocksMap.getStorages(block);
  }

  public int numCorruptReplicas(Block block) {
    return corruptReplicas.numCorruptReplicas(block);
  }

  public void removeBlockFromMap(Block block) {
    removeFromExcessReplicateMap(block);
    blocksMap.removeBlock(block); // 内存中删除block
    // If block is removed from blocksMap remove it from corruptReplicasMap
    corruptReplicas.removeFromCorruptReplicasMap(block); // 从corrupt队列中删除
  }

  /**
   * If a block is removed from blocksMap, remove it from excessReplicateMap.
   */
  private void removeFromExcessReplicateMap(Block block) {
    for (DatanodeStorageInfo info : blocksMap.getStorages(block)) {
      String uuid = info.getDatanodeDescriptor().getDatanodeUuid();
      LightWeightLinkedSet<Block> excessReplicas = excessReplicateMap.get(uuid);
      if (excessReplicas != null) {
        if (excessReplicas.remove(block)) {
          excessBlocksCount.decrementAndGet();
          if (excessReplicas.isEmpty()) {
            excessReplicateMap.remove(uuid);
          }
        }
      }
    }
  }

  public int getCapacity() {
    return blocksMap.getCapacity();
  }
  
  /**
   * Return a range of corrupt replica block ids. Up to numExpectedBlocks 
   * blocks starting at the next block after startingBlockId are returned
   * (fewer if numExpectedBlocks blocks are unavailable). If startingBlockId 
   * is null, up to numExpectedBlocks blocks are returned from the beginning.
   * If startingBlockId cannot be found, null is returned.
   *
   * @param numExpectedBlocks Number of block ids to return.
   *  0 <= numExpectedBlocks <= 100
   * @param startingBlockId Block id from which to start. If null, start at
   *  beginning.
   * @return Up to numExpectedBlocks blocks from startingBlockId if it exists
   *
   */
  public long[] getCorruptReplicaBlockIds(int numExpectedBlocks,
                                   Long startingBlockId) {
    return corruptReplicas.getCorruptReplicaBlockIds(numExpectedBlocks,
                                                     startingBlockId);
  }

  /**
   * Return an iterator over the set of blocks for which there are no replicas.
   */
  public Iterator<Block> getCorruptReplicaBlockIterator() {
    return neededReplications.iterator(
        UnderReplicatedBlocks.QUEUE_WITH_CORRUPT_BLOCKS);
  }

  /**
   * Get the replicas which are corrupt for a given block.
   */
  public Collection<DatanodeDescriptor> getCorruptReplicas(Block block) {
    return corruptReplicas.getNodes(block);
  }

 /**
  * Get reason for certain corrupted replicas for a given block and a given dn.
  */
 public String getCorruptReason(Block block, DatanodeDescriptor node) {
   return corruptReplicas.getCorruptReason(block, node);
 }

  /** @return the size of UnderReplicatedBlocks */
  public int numOfUnderReplicatedBlocks() {
    return neededReplications.size();
  }

  /**
   * Periodically calls computeReplicationWork().
   */
  private class ReplicationMonitor implements Runnable {

    @Override
    public void run() {
      while (namesystem.isRunning()) {
        try {
          // Process replication work only when active NN is out of safe mode.
          if (namesystem.isPopulatingReplQueues()) {
            computeDatanodeWork();
            processPendingReplications();
            rescanPostponedMisreplicatedBlocks(); // 处理postponedMisreplicatedBlocks中的block
          }
          Thread.sleep(replicationRecheckInterval); // 默认：3s
        } catch (Throwable t) {
          if (!namesystem.isRunning()) {
            LOG.info("Stopping ReplicationMonitor.");
            if (!(t instanceof InterruptedException)) {
              LOG.info("ReplicationMonitor received an exception"
                  + " while shutting down.", t);
            }
            break;
          } else if (!checkNSRunning && t instanceof InterruptedException) {
            LOG.info("Stopping ReplicationMonitor for testing.");
            break;
          }
          LOG.error("ReplicationMonitor thread received Runtime exception. ",
              t);
          terminate(1, t);
        }
      }
    }
  }

  /** How often to check and the limit for the storageinfo efficiency. */
  private final long storageInfoDefragmentInterval = 0;
  private final long storageInfoDefragmentTimeout = 0;
  private final double storageInfoDefragmentRatio = 0;

  /**
   * Runnable that monitors the fragmentation of the StorageInfo TreeSet and
   * compacts it when it falls under a certain threshold.
   *
   * https://issues.apache.org/jira/browse/HDFS-9260
   * 全量块汇报相关优化
   */
  private class StorageInfoDefragmenter implements Runnable {

    @Override
    public void run() {
      while (namesystem.isRunning()) {
        try {
          // Check storage efficiency only when active NN is out of safe mode.
          if (namesystem.isPopulatingReplQueues()) {
            scanAndCompactStorages();
          }
          Thread.sleep(storageInfoDefragmentInterval);
        } catch (Throwable t) {
          if (!namesystem.isRunning()) {
            LOG.info("Stopping thread.");
            if (!(t instanceof InterruptedException)) {
              LOG.info("Received an exception while shutting down.", t);
            }
            break;
          } else if (!checkNSRunning && t instanceof InterruptedException) {
            LOG.info("Stopping for testing.");
            break;
          }
          LOG.error("Thread received Runtime exception.", t);
          terminate(1, t);
        }
      }
    }

    private void scanAndCompactStorages() throws InterruptedException {
      ArrayList<String> datanodesAndStorages = new ArrayList<>();
      for (DatanodeDescriptor node
              : datanodeManager.getDatanodeListForReport(HdfsConstants.DatanodeReportType.ALL)) {
        for (DatanodeStorageInfo storage : node.getStorageInfos()) {
          try {
            namesystem.readLock();
            double ratio = storage.treeSetFillRatio();
            if (ratio < storageInfoDefragmentRatio) {
              datanodesAndStorages.add(node.getDatanodeUuid());
              datanodesAndStorages.add(storage.getStorageID());
            }
            LOG.info("StorageInfo TreeSet fill ratio {} : {}{}",
                    storage.getStorageID(), ratio,
                    (ratio < storageInfoDefragmentRatio)
                            ? " (queued for defragmentation)" : "");
          } finally {
            namesystem.readUnlock();
          }
        }
      }
      if (!datanodesAndStorages.isEmpty()) {
        for (int i = 0; i < datanodesAndStorages.size(); i += 2) {
          namesystem.writeLock();
          try {
            final DatanodeDescriptor dn = datanodeManager.
                    getDatanode(datanodesAndStorages.get(i));
            if (dn == null) {
              continue;
            }
            final DatanodeStorageInfo storage = dn.
                    getStorageInfo(datanodesAndStorages.get(i + 1));
            if (storage != null) {
              boolean aborted =
                      !storage.treeSetCompact(storageInfoDefragmentTimeout);
              if (aborted) {
                // Compaction timed out, reset iterator to continue with
                // the same storage next iteration.
                i -= 2;
              }
              LOG.info("StorageInfo TreeSet defragmented {} : {}{}",
                      storage.getStorageID(), storage.treeSetFillRatio(),
                      aborted ? " (aborted)" : "");
            }
          } finally {
            namesystem.writeUnlock();
          }
          // Wait between each iteration
          Thread.sleep(1000);
        }
      }
    }
  }

  /**
   * Compute block replication and block invalidation work that can be scheduled
   * on data-nodes. The datanode will be informed of this work at the next
   * heartbeat.
   *
   * @return number of blocks scheduled for replication or removal.
   */
  int computeDatanodeWork() {
    // Blocks should not be replicated or removed if in safe mode.
    // It's OK to check safe mode here w/o holding lock, in the worst
    // case extra replications will be scheduled, and these will get
    // fixed up later.
    if (namesystem.isInSafeMode()) {
      return 0;
    }

    final int numlive = heartbeatManager.getLiveDatanodeCount();
    // 每次复制的副本数量
    final int blocksToProcess = numlive
        * this.blocksReplWorkMultiplier; // 默认2
    // 每次执行删除的副本操作的DN数量
    final int nodesToProcess = (int) Math.ceil(numlive
        * this.blocksInvalidateWorkPct);

    int workFound = this.computeReplicationWork(blocksToProcess); // 生成block复制任务

    // Update counters
    namesystem.writeLock();
    try {
      this.updateState(); // 更新metric值
      this.scheduledReplicationBlocksCount = workFound;   // 更新metric值: replicationMonitor一次生成的块复制任务数
    } finally {
      namesystem.writeUnlock();
    }
    workFound += this.computeInvalidateWork(nodesToProcess); // 生成block删除任务
    return workFound;
  }

  /**
   * Clear all queues that hold decisions previously made by
   * this NameNode.
   */
  public void clearQueues() { // 清空BM副本相关数据结构
    neededReplications.clear();
    pendingReplications.clear();
    excessReplicateMap.clear();
    invalidateBlocks.clear();
    datanodeManager.clearPendingQueues();
    postponedMisreplicatedBlocks.clear();
    postponedMisreplicatedBlocksCount.set(0);
  };
  

  private static class ReplicationWork {

    private final Block block;
    private final BlockCollection bc;

    private final DatanodeDescriptor srcNode;
    private final List<DatanodeDescriptor> containingNodes;
    private final List<DatanodeStorageInfo> liveReplicaStorages;
    private final int additionalReplRequired;

    private DatanodeStorageInfo targets[];
    private final int priority;

    public ReplicationWork(Block block,
        BlockCollection bc,
        DatanodeDescriptor srcNode,
        List<DatanodeDescriptor> containingNodes,
        List<DatanodeStorageInfo> liveReplicaStorages,
        int additionalReplRequired,
        int priority) {
      this.block = block;
      this.bc = bc;
      this.srcNode = srcNode;
      this.srcNode.incrementPendingReplicationWithoutTargets();
      this.containingNodes = containingNodes;
      this.liveReplicaStorages = liveReplicaStorages;
      this.additionalReplRequired = additionalReplRequired;
      this.priority = priority;
      this.targets = null;
    }
    
    private void chooseTargets(BlockPlacementPolicy blockplacement,
        BlockStoragePolicySuite storagePolicySuite,
        Set<Node> excludedNodes) {
      try {
        targets = blockplacement.chooseTarget(bc.getName(),
            additionalReplRequired, srcNode, liveReplicaStorages, false,
            excludedNodes, block.getNumBytes(),
            storagePolicySuite.getPolicy(bc.getStoragePolicyID()));
      } finally {
        srcNode.decrementPendingReplicationWithoutTargets();
      }
    }
  }

  /**
   * A simple result enum for the result of
   * {@link BlockManager#processMisReplicatedBlock(BlockInfoContiguous)}.
   */
  enum MisReplicationResult {
    /** The block should be invalidated since it belongs to a deleted file. */
    INVALID,
    /** The block is currently under-replicated. */
    UNDER_REPLICATED,
    /** The block is currently over-replicated. */
    OVER_REPLICATED,
    /** A decision can't currently be made about this block. */
    POSTPONE,
    /** The block is under construction, so should be ignored */
    UNDER_CONSTRUCTION,
    /** The block is properly replicated */
    OK
  }

  public void shutdown() {
    stopReplicationInitializer();
    blocksMap.close();
  }
  
  public void clear() {
    clearQueues();
    blocksMap.clear();
  }
}
