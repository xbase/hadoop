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
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;

/** A class that receives a block and writes to its own disk, meanwhile
 * may copies it to another site. If a throttler is provided,
 * streaming throttling is also supported.
 **/
class BlockReceiver implements Closeable {
  public static final Log LOG = DataNode.LOG;
  static final Log ClientTraceLog = DataNode.ClientTraceLog;

  @VisibleForTesting
  static long CACHE_DROP_LAG_BYTES = 8 * 1024 * 1024;
  private final long datanodeSlowLogThresholdMs;
  private DataInputStream in = null; // from where data are read
  private DataChecksum clientChecksum; // checksum used by client
  private DataChecksum diskChecksum; // checksum we write to disk
  
  /**
   * In the case that the client is writing with a different
   * checksum polynomial than the block is stored with on disk,
   * the DataNode needs to recalculate checksums before writing.
   */
  private final boolean needsChecksumTranslation;
  private OutputStream out = null; // to block file at local disk
  private FileDescriptor outFd;
  private DataOutputStream checksumOut = null; // to crc file at local disk
  private final int bytesPerChecksum;
  private final int checksumSize;
  
  private final PacketReceiver packetReceiver = new PacketReceiver(false);
  
  protected final String inAddr;
  protected final String myAddr;
  private String mirrorAddr;
  private DataOutputStream mirrorOut;
  private Daemon responder = null;
  private DataTransferThrottler throttler;
  private ReplicaOutputStreams streams;
  private DatanodeInfo srcDataNode = null;
  private final DataNode datanode;
  volatile private boolean mirrorError;

  // Cache management state
  private boolean dropCacheBehindWrites;
  private long lastCacheManagementOffset = 0;
  private boolean syncBehindWrites;
  private boolean syncBehindWritesInBackground;

  /** The client name.  It is empty if a datanode is the client */
  private final String clientname;
  private final boolean isClient; 
  private final boolean isDatanode;

  /** the block to receive */
  private final ExtendedBlock block; 
  /** the replica to write */
  private final ReplicaInPipelineInterface replicaInfo;
  /** pipeline stage */
  private final BlockConstructionStage stage;
  private final boolean isTransfer;

  private boolean syncOnClose;
  private long restartBudget;
  /** the reference of the volume where the block receiver writes to */
  private ReplicaHandler replicaHandler;

  /**
   * for replaceBlock response
   */
  private final long responseInterval;
  private long lastResponseTime = 0;
  private boolean isReplaceBlock = false;
  private DataOutputStream replyOut = null;
  
  private boolean pinning;
  private long lastSentTime;
  private long maxSendIdleTime;

  BlockReceiver(final ExtendedBlock block, final StorageType storageType,
      final DataInputStream in,
      final String inAddr, final String myAddr,
      final BlockConstructionStage stage, 
      final long newGs, final long minBytesRcvd, final long maxBytesRcvd, 
      final String clientname, final DatanodeInfo srcDataNode,
      final DataNode datanode, DataChecksum requestedChecksum,
      CachingStrategy cachingStrategy,
      final boolean allowLazyPersist,
      final boolean pinning) throws IOException {
    try{
      this.block = block;
      this.in = in; // 输入流
      this.inAddr = inAddr; // 输入流的地址
      this.myAddr = myAddr; // 本机地址
      this.srcDataNode = srcDataNode; // 上游DN（如果上游是DN的话）
      this.datanode = datanode;

      this.clientname = clientname;
      this.isDatanode = clientname.length() == 0; // 上游是否是DN
      this.isClient = !this.isDatanode; // 上游是否是Client
      this.restartBudget = datanode.getDnConf().restartReplicaExpiry; // DN restart时使用
      this.datanodeSlowLogThresholdMs = datanode.getDnConf().datanodeSlowIoWarningThresholdMs;
      // For replaceBlock() calls response should be sent to avoid socketTimeout
      // at clients. So sending with the interval of 0.5 * socketTimeout
      final long readTimeout = datanode.getDnConf().socketTimeout;
      this.responseInterval = (long) (readTimeout * 0.5);
      //for datanode, we have
      //1: clientName.length() == 0, and
      //2: stage == null or PIPELINE_SETUP_CREATE
      this.stage = stage;
      this.isTransfer = stage == BlockConstructionStage.TRANSFER_RBW
          || stage == BlockConstructionStage.TRANSFER_FINALIZED;

      this.pinning = pinning;
      this.lastSentTime = Time.monotonicNow();
      // Downstream will timeout in readTimeout on receiving the next packet.
      // If there is no data traffic, a heartbeat packet is sent at
      // the interval of 0.5*readTimeout. Here, we set 0.9*readTimeout to be
      // the threshold for detecting congestion.
      this.maxSendIdleTime = (long) (readTimeout * 0.9);
      if (LOG.isDebugEnabled()) {
        LOG.debug(getClass().getSimpleName() + ": " + block
            + "\n  isClient  =" + isClient + ", clientname=" + clientname
            + "\n  isDatanode=" + isDatanode + ", srcDataNode=" + srcDataNode
            + "\n  inAddr=" + inAddr + ", myAddr=" + myAddr
            + "\n  cachingStrategy = " + cachingStrategy
            + "\n  pinning=" + pinning
            );
      }

      //
      // Open local disk out
      //
      if (isDatanode) { //replication or move 上游如果是DN，写到tmp目录?
        replicaHandler = datanode.data.createTemporary(storageType, block);
      } else {
        switch (stage) {
        case PIPELINE_SETUP_CREATE:
          replicaHandler = datanode.data.createRbw(storageType, block, allowLazyPersist); // 创建RBW文件
          datanode.notifyNamenodeReceivingBlock(
              block, replicaHandler.getReplica().getStorageUuid());
          break;
        case PIPELINE_SETUP_STREAMING_RECOVERY:
          replicaHandler = datanode.data.recoverRbw(
              block, newGs, minBytesRcvd, maxBytesRcvd);
          block.setGenerationStamp(newGs);
          break;
        case PIPELINE_SETUP_APPEND:
          replicaHandler = datanode.data.append(block, newGs, minBytesRcvd);
          block.setGenerationStamp(newGs);
          datanode.notifyNamenodeReceivingBlock(
              block, replicaHandler.getReplica().getStorageUuid());
          break;
        case PIPELINE_SETUP_APPEND_RECOVERY:
          replicaHandler = datanode.data.recoverAppend(block, newGs, minBytesRcvd);
          block.setGenerationStamp(newGs);
          datanode.notifyNamenodeReceivingBlock(
              block, replicaHandler.getReplica().getStorageUuid());
          break;
        case TRANSFER_RBW:
        case TRANSFER_FINALIZED:
          // this is a transfer destination
          replicaHandler =
              datanode.data.createTemporary(storageType, block);
          break;
        default: throw new IOException("Unsupported stage " + stage + 
              " while receiving block " + block + " from " + inAddr);
        }
      }
      replicaInfo = replicaHandler.getReplica();
      // OS缓存相关
      this.dropCacheBehindWrites = (cachingStrategy.getDropBehind() == null) ?
        datanode.getDnConf().dropCacheBehindWrites :
          cachingStrategy.getDropBehind();
      this.syncBehindWrites = datanode.getDnConf().syncBehindWrites;
      this.syncBehindWritesInBackground = datanode.getDnConf().
          syncBehindWritesInBackground;
      
      final boolean isCreate = isDatanode || isTransfer 
          || stage == BlockConstructionStage.PIPELINE_SETUP_CREATE;
      streams = replicaInfo.createStreams(isCreate, requestedChecksum);
      assert streams != null : "null streams!";

      // read checksum meta information
      this.clientChecksum = requestedChecksum; // client使用的checksum算法
      this.diskChecksum = streams.getChecksum(); // 如果是append，本地已经存在的meta文件使用的checksum算法
      this.needsChecksumTranslation = !clientChecksum.equals(diskChecksum); // checksum算法不同，需要转换
      this.bytesPerChecksum = diskChecksum.getBytesPerChecksum(); // DN的chunk大小
      this.checksumSize = diskChecksum.getChecksumSize(); // 一个checksum几个字节

      this.out = streams.getDataOut(); // DN写data使用的stream
      if (out instanceof FileOutputStream) {
        this.outFd = ((FileOutputStream)out).getFD();
      } else {
        LOG.warn("Could not get file descriptor for outputstream of class " + out.getClass());
      }
      this.checksumOut = new DataOutputStream(new BufferedOutputStream( // DN写checksum使用的stream
          streams.getChecksumOut(), HdfsConstants.SMALL_BUFFER_SIZE));
      // write data chunk header if creating a new replica
      if (isCreate) {
        BlockMetadataHeader.writeHeader(checksumOut, diskChecksum); // 写meta文件的header
      } 
    } catch (ReplicaAlreadyExistsException bae) {
      throw bae;
    } catch (ReplicaNotFoundException bne) {
      throw bne;
    } catch(IOException ioe) {
      IOUtils.closeStream(this);
      cleanupBlock();
      
      // check if there is a disk error
      IOException cause = DatanodeUtil.getCauseIfDiskError(ioe);
      DataNode.LOG.warn("IOException in BlockReceiver constructor. Cause is ",
          cause);
      
      if (cause != null) { // possible disk error
        ioe = cause;
        datanode.checkDiskErrorAsync();
      }
      
      throw ioe;
    }
  }

  /** Return the datanode object. */
  DataNode getDataNode() {return datanode;}

  String getStorageUuid() {
    return replicaInfo.getStorageUuid();
  }

  /**
   * close files and release volume reference.
   */
  @Override
  public void close() throws IOException {
    packetReceiver.close();

    IOException ioe = null;
    if (syncOnClose && (out != null || checksumOut != null)) {
      datanode.metrics.incrFsyncCount();      
    }
    long flushTotalNanos = 0;
    boolean measuredFlushTime = false;
    // close checksum file
    try {
      if (checksumOut != null) {
        long flushStartNanos = System.nanoTime();
        checksumOut.flush();
        long flushEndNanos = System.nanoTime();
        if (syncOnClose) {
          long fsyncStartNanos = flushEndNanos;
          streams.syncChecksumOut();
          datanode.metrics.addFsyncNanos(System.nanoTime() - fsyncStartNanos);
        }
        flushTotalNanos += flushEndNanos - flushStartNanos;
        measuredFlushTime = true;
        checksumOut.close();
        checksumOut = null;
      }
    } catch(IOException e) {
      ioe = e;
    }
    finally {
      IOUtils.closeStream(checksumOut);
    }
    // close block file
    try {
      if (out != null) {
        long flushStartNanos = System.nanoTime();
        out.flush();
        long flushEndNanos = System.nanoTime();
        if (syncOnClose) {
          long fsyncStartNanos = flushEndNanos;
          streams.syncDataOut();
          datanode.metrics.addFsyncNanos(System.nanoTime() - fsyncStartNanos);
        }
        flushTotalNanos += flushEndNanos - flushStartNanos;
        measuredFlushTime = true;
        out.close();
        out = null;
      }
    } catch (IOException e) {
      ioe = e;
    }
    finally{
      IOUtils.closeStream(out);
    }
    if (replicaHandler != null) {
      IOUtils.cleanup(null, replicaHandler);
      replicaHandler = null;
    }
    if (measuredFlushTime) {
      datanode.metrics.addFlushNanos(flushTotalNanos);
    }
    // disk check
    if(ioe != null) {
      datanode.checkDiskErrorAsync();
      throw ioe;
    }
  }

  synchronized void setLastSentTime(long sentTime) {
    lastSentTime = sentTime;
  }

  /**
   * It can return false if
   * - upstream did not send packet for a long time
   * - a packet was received but got stuck in local disk I/O.
   * - a packet was received but got stuck on send to mirror.
   */
  synchronized boolean packetSentInTime() {
    long diff = Time.monotonicNow() - lastSentTime;
    if (diff > maxSendIdleTime) {
      LOG.info("A packet was last sent " + diff + " milliseconds ago.");
      return false;
    }
    return true;
  }

  /**
   * Flush block data and metadata files to disk.
   * @throws IOException
   */
  void flushOrSync(boolean isSync) throws IOException {
    long flushTotalNanos = 0;
    long begin = Time.monotonicNow();
    if (checksumOut != null) {
      long flushStartNanos = System.nanoTime();
      checksumOut.flush();
      long flushEndNanos = System.nanoTime();
      if (isSync) {
        long fsyncStartNanos = flushEndNanos;
        streams.syncChecksumOut();
        datanode.metrics.addFsyncNanos(System.nanoTime() - fsyncStartNanos);
      }
      flushTotalNanos += flushEndNanos - flushStartNanos;
    }
    if (out != null) {
      long flushStartNanos = System.nanoTime();
      out.flush();
      long flushEndNanos = System.nanoTime();
      if (isSync) {
        long fsyncStartNanos = flushEndNanos;
        streams.syncDataOut();
        datanode.metrics.addFsyncNanos(System.nanoTime() - fsyncStartNanos);
      }
      flushTotalNanos += flushEndNanos - flushStartNanos;
    }
    if (checksumOut != null || out != null) {
      datanode.metrics.addFlushNanos(flushTotalNanos);
      if (isSync) {
    	  datanode.metrics.incrFsyncCount();      
      }
    }
    long duration = Time.monotonicNow() - begin;
    if (duration > datanodeSlowLogThresholdMs) {
      LOG.warn("Slow flushOrSync took " + duration + "ms (threshold="
          + datanodeSlowLogThresholdMs + "ms), isSync:" + isSync + ", flushTotalNanos="
          + flushTotalNanos + "ns");
    }
  }

  /**
   * While writing to mirrorOut, failure to write to mirror should not
   * affect this datanode unless it is caused by interruption.
   */
  private void handleMirrorOutError(IOException ioe) throws IOException {
    String bpid = block.getBlockPoolId();
    LOG.info(datanode.getDNRegistrationForBP(bpid)
        + ":Exception writing " + block + " to mirror " + mirrorAddr, ioe);
    if (Thread.interrupted()) { // shut down if the thread is interrupted
      throw ioe;
    } else { // encounter an error while writing to mirror
      // continue to run even if can not write to mirror
      // notify client of the error
      // and wait for the client to shut down the pipeline
      mirrorError = true;
    }
  }
  
  /**
   * Verify multiple CRC chunks. 
   */
  private void verifyChunks(ByteBuffer dataBuf, ByteBuffer checksumBuf)
      throws IOException {
    try {
      clientChecksum.verifyChunkedSums(dataBuf, checksumBuf, clientname, 0);
    } catch (ChecksumException ce) {
      LOG.warn("Checksum error in block " + block + " from " + inAddr, ce);
      // No need to report to namenode when client is writing.
      if (srcDataNode != null && isDatanode) {
        try {
          LOG.info("report corrupt " + block + " from datanode " +
                    srcDataNode + " to namenode");
          datanode.reportRemoteBadBlock(srcDataNode, block);
        } catch (IOException e) {
          LOG.warn("Failed to report bad " + block + 
                    " from datanode " + srcDataNode + " to namenode");
        }
      }
      throw new IOException("Unexpected checksum mismatch while writing "
          + block + " from " + inAddr);
    }
  }
  
    
  /**
   * Translate CRC chunks from the client's checksum implementation
   * to the disk checksum implementation.
   * 
   * This does not verify the original checksums, under the assumption
   * that they have already been validated.
   */
  private void translateChunks(ByteBuffer dataBuf, ByteBuffer checksumBuf) {
    diskChecksum.calculateChunkedSums(dataBuf, checksumBuf);
  }

  /** 
   * Check whether checksum needs to be verified.
   * Skip verifying checksum iff this is not the last one in the 
   * pipeline and clientName is non-null. i.e. Checksum is verified
   * on all the datanodes when the data is being written by a 
   * datanode rather than a client. Whe client is writing the data, 
   * protocol includes acks and only the last datanode needs to verify 
   * checksum.
   * @return true if checksum verification is needed, otherwise false.
   */
  private boolean shouldVerifyChecksum() { // 如果当前节点是pipeline的最后一个节点，需验证checksum
    return (mirrorOut == null || isDatanode || needsChecksumTranslation);
  }

  /** 
   * Receives and processes a packet. It can contain many chunks.
   * returns the number of data bytes that the packet has.
   *
   * 1、从流中读出一个packet
   * 2、发送给下游节点
   * 3、数据和checksum写本地磁盘
   * 4、添加到Ack队列
   * 5、向上游响应
   */
  private int receivePacket() throws IOException {
    // read the next packet
    // step 1: 从流中读出一个packet
    packetReceiver.receiveNextPacket(in);

    PacketHeader header = packetReceiver.getHeader();
    if (LOG.isDebugEnabled()){
      LOG.debug("Receiving one packet for block " + block +
                ": " + header);
    }

    // Sanity check the header
    if (header.getOffsetInBlock() > replicaInfo.getNumBytes()) {
      throw new IOException("Received an out-of-sequence packet for " + block + 
          "from " + inAddr + " at offset " + header.getOffsetInBlock() +
          ". Expecting packet starting at " + replicaInfo.getNumBytes());
    }
    if (header.getDataLen() < 0) {
      throw new IOException("Got wrong length during writeBlock(" + block + 
                            ") from " + inAddr + " at offset " + 
                            header.getOffsetInBlock() + ": " +
                            header.getDataLen()); 
    }

    long offsetInBlock = header.getOffsetInBlock(); // 在block中的偏移量
    long seqno = header.getSeqno(); // packet 序号
    boolean lastPacketInBlock = header.isLastPacketInBlock(); // 是否是最后一个packet
    final int len = header.getDataLen(); // 数据的长度
    boolean syncBlock = header.getSyncBlock(); // 是否需要sync

    // avoid double sync'ing on close
    if (syncBlock && lastPacketInBlock) {
      this.syncOnClose = false;
    }

    // update received bytes
    final long firstByteInBlock = offsetInBlock;
    offsetInBlock += len;
    if (replicaInfo.getNumBytes() < offsetInBlock) {
      replicaInfo.setNumBytes(offsetInBlock); // 当前读到的block长度
    }
    
    // put in queue for pending acks, unless sync was requested
    if (responder != null && !syncBlock && !shouldVerifyChecksum()) {
      // 优先处理Ack，如果不需要立即sync (syncBlock=false)
      ((PacketResponder) responder.getRunnable()).enqueue(seqno,
          lastPacketInBlock, offsetInBlock, Status.SUCCESS); // 放入等待ack队列
    }

    // Drop heartbeat for testing.
    if (seqno < 0 && len == 0 &&
        DataNodeFaultInjector.get().dropHeartbeatPacket()) {
      return 0;
    }

    //First write the packet to the mirror:
    // step 2: 发送给下游节点
    if (mirrorOut != null && !mirrorError) {
      try {
        long begin = Time.monotonicNow();
        packetReceiver.mirrorPacketTo(mirrorOut); // 发送给下游节点
        mirrorOut.flush();
        long now = Time.monotonicNow();
        setLastSentTime(now);
        long duration = now - begin;
        if (duration > datanodeSlowLogThresholdMs) {
          LOG.warn("Slow BlockReceiver write packet to mirror took " + duration
              + "ms (threshold=" + datanodeSlowLogThresholdMs + "ms)");
        }
      } catch (IOException e) {
        handleMirrorOutError(e);
      }
    }
    
    ByteBuffer dataBuf = packetReceiver.getDataSlice();
    ByteBuffer checksumBuf = packetReceiver.getChecksumSlice();
    
    if (lastPacketInBlock || len == 0) { // packet中data长度为0，可能客户端仅仅发了一个sync操作
      if(LOG.isDebugEnabled()) {
        LOG.debug("Receiving an empty packet or the end of the block " + block);
      }
      // sync block if requested
      if (syncBlock) { // 如果接收了完整的数据块，并且启动了sync标识，则立即sync
        flushOrSync(true);
      }
    } else {
      final int checksumLen = diskChecksum.getChecksumSize(len); // checksum的总长度
      final int checksumReceivedLen = checksumBuf.capacity(); // 上游发送的checksum总长度

      if (checksumReceivedLen > 0 && checksumReceivedLen != checksumLen) {
        throw new IOException("Invalid checksum length: received length is "
            + checksumReceivedLen + " but expected length is " + checksumLen);
      }

      // 如果当前节点是pipeline的最后一个节点，需验证checksum
      if (checksumReceivedLen > 0 && shouldVerifyChecksum()) {
        try {
          verifyChunks(dataBuf, checksumBuf); // 验证校验和
        } catch (IOException ioe) {
          // checksum error detected locally. there is no reason to continue.
          if (responder != null) {
            try {
              // pipeline最后一个节点，现在enqueue一个Ack
              ((PacketResponder) responder.getRunnable()).enqueue(seqno,
                  lastPacketInBlock, offsetInBlock,
                  Status.ERROR_CHECKSUM);
              // Wait until the responder sends back the response
              // and interrupt this thread.
              Thread.sleep(3000);
            } catch (InterruptedException e) { }
          }
          throw new IOException("Terminating due to a checksum error." + ioe);
        }
 
        if (needsChecksumTranslation) {
          // overwrite the checksums in the packet buffer with the
          // appropriate polynomial for the disk storage.
          translateChunks(dataBuf, checksumBuf); // 转换checksum算法
        }
      }

      // 客户端没有发送checksum
      if (checksumReceivedLen == 0 && !streams.isTransientStorage()) {
        // checksum is missing, need to calculate it
        checksumBuf = ByteBuffer.allocate(checksumLen);
        diskChecksum.calculateChunkedSums(dataBuf, checksumBuf); // 重新计算checksum
      }
      
      // by this point, the data in the buffer uses the disk checksum

      final boolean shouldNotWriteChecksum = checksumReceivedLen == 0 // 这种情况，不需要写checksum
          && streams.isTransientStorage();
      try {
        long onDiskLen = replicaInfo.getBytesOnDisk(); // 在磁盘上的block长度
        if (onDiskLen<offsetInBlock) { // 写data和checksum到文件
          // Normally the beginning of an incoming packet is aligned with the
          // existing data on disk. If the beginning packet data offset is not
          // checksum chunk aligned, the end of packet will not go beyond the
          // next chunk boundary.
          // When a failure-recovery is involved, the client state and the
          // the datanode state may not exactly agree. I.e. the client may
          // resend part of data that is already on disk. Correct number of
          // bytes should be skipped when writing the data and checksum
          // buffers out to disk.
          long partialChunkSizeOnDisk = onDiskLen % bytesPerChecksum; // 最后一个不完整chunk，缺少的长度
          long lastChunkBoundary = onDiskLen - partialChunkSizeOnDisk; // 最后一个不完整的chunk长度
          boolean alignedOnDisk = partialChunkSizeOnDisk == 0;
          boolean alignedInPacket = firstByteInBlock % bytesPerChecksum == 0; // alignedOnDisk和alignedInPacket，有什么不同?

          // If the end of the on-disk data is not chunk-aligned, the last
          // checksum needs to be overwritten.
          boolean overwriteLastCrc = !alignedOnDisk && !shouldNotWriteChecksum; // 是否需要复写最后一个checksum
          // If the starting offset of the packat data is at the last chunk
          // boundary of the data on disk, the partial checksum recalculation
          // can be skipped and the checksum supplied by the client can be used
          // instead. This reduces disk reads and cpu load.
          boolean doCrcRecalc = overwriteLastCrc &&
              (lastChunkBoundary != firstByteInBlock); // firstByteInBlock == lastChunkBoundary，则不用重新计算checksum

          // If this is a partial chunk, then verify that this is the only
          // chunk in the packet. If the starting offset is not chunk
          // aligned, the packet should terminate at or before the next
          // chunk boundary.
          if (!alignedInPacket && len > bytesPerChecksum) {
            throw new IOException("Unexpected packet data length for "
                +  block + " from " + inAddr + ": a partial chunk must be "
                + " sent in an individual packet (data length = " + len
                +  " > bytesPerChecksum = " + bytesPerChecksum + ")");
          }

          // If the last portion of the block file is not a full chunk,
          // then read in pre-existing partial data chunk and recalculate
          // the checksum so that the checksum calculation can continue
          // from the right state. If the client provided the checksum for
          // the whole chunk, this is not necessary.
          Checksum partialCrc = null;
          if (doCrcRecalc) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("receivePacket for " + block 
                  + ": previous write did not end at the chunk boundary."
                  + " onDiskLen=" + onDiskLen);
            }
            long offsetInChecksum = BlockMetadataHeader.getHeaderSize() +
                onDiskLen / bytesPerChecksum * checksumSize;
            partialCrc = computePartialChunkCrc(onDiskLen, offsetInChecksum); // 最后一个不完整的chunk，重新计算checksum
          }

          // The data buffer position where write will begin. If the packet
          // data and on-disk data have no overlap, this will not be at the
          // beginning of the buffer.
          int startByteToDisk = (int)(onDiskLen-firstByteInBlock) 
              + dataBuf.arrayOffset() + dataBuf.position();

          // Actual number of data bytes to write.
          int numBytesToDisk = (int)(offsetInBlock-onDiskLen);
          
          // Write data to disk.
          // step 3: 数据和checksum写本地磁盘
          long begin = Time.monotonicNow();
          // step 3.1：写数据
          out.write(dataBuf.array(), startByteToDisk, numBytesToDisk);
          long duration = Time.monotonicNow() - begin;
          if (duration > datanodeSlowLogThresholdMs) {
            LOG.warn("Slow BlockReceiver write data to disk cost:" + duration
                + "ms (threshold=" + datanodeSlowLogThresholdMs + "ms)");
          }

          final byte[] lastCrc;
          if (shouldNotWriteChecksum) {
            lastCrc = null;
          } else {
            int skip = 0;
            byte[] crcBytes = null;

            // First, prepare to overwrite the partial crc at the end.
            if (overwriteLastCrc) { // not chunk-aligned on disk
              // prepare to overwrite last checksum
              adjustCrcFilePosition(); // 先把meta的文件指针移动到指定位置，为复写checksum做准备
            }

            // The CRC was recalculated for the last partial chunk. Update the
            // CRC by reading the rest of the chunk, then write it out.
            if (doCrcRecalc) {
              // Calculate new crc for this chunk.
              int bytesToReadForRecalc =
                  (int)(bytesPerChecksum - partialChunkSizeOnDisk);
              if (numBytesToDisk < bytesToReadForRecalc) {
                bytesToReadForRecalc = numBytesToDisk;
              }

              partialCrc.update(dataBuf.array(), startByteToDisk,
                  bytesToReadForRecalc);
              byte[] buf = FSOutputSummer.convertToByteStream(partialCrc,
                  checksumSize);
              crcBytes = copyLastChunkChecksum(buf, checksumSize, buf.length);
              // 复写checksum
              checksumOut.write(buf);
              if(LOG.isDebugEnabled()) {
                LOG.debug("Writing out partial crc for data len " + len +
                    ", skip=" + skip);
              }
              skip++; //  For the partial chunk that was just read.
            }

            // Determine how many checksums need to be skipped up to the last
            // boundary. The checksum after the boundary was already counted
            // above. Only count the number of checksums skipped up to the
            // boundary here.
            long skippedDataBytes = lastChunkBoundary - firstByteInBlock;

            if (skippedDataBytes > 0) {
              skip += (int)(skippedDataBytes / bytesPerChecksum) +
                  ((skippedDataBytes % bytesPerChecksum == 0) ? 0 : 1);
            }
            skip *= checksumSize; // Convert to number of bytes

            // write the rest of checksum
            final int offset = checksumBuf.arrayOffset() +
                checksumBuf.position() + skip;
            final int end = offset + checksumLen - skip;
            // If offset >= end, there is no more checksum to write.
            // I.e. a partial chunk checksum rewrite happened and there is no
            // more to write after that.
            if (offset >= end && doCrcRecalc) {
              lastCrc = crcBytes;
            } else {
              final int remainingBytes = checksumLen - skip;
              lastCrc = copyLastChunkChecksum(checksumBuf.array(),
                  checksumSize, end);
              // step 3.2：写checksum
              checksumOut.write(checksumBuf.array(), offset, remainingBytes);
            }
          }

          /// flush entire packet, sync if requested
          flushOrSync(syncBlock);
          
          replicaInfo.setLastChecksumAndDataLen(offsetInBlock, lastCrc);

          datanode.metrics.incrBytesWritten(len);
          datanode.metrics.incrTotalWriteTime(duration);

          // 清除操作系统缓存
          manageWriterOsCache(offsetInBlock);
        }
      } catch (IOException iex) {
        datanode.checkDiskErrorAsync();
        throw iex;
      }
    }

    // if sync was requested, put in queue for pending acks here
    // (after the fsync finished)
    // 如果是pipeline的最后一个节点，或者syncBlock=true，则在数据落盘之后再处理Ack
    if (responder != null && (syncBlock || shouldVerifyChecksum())) {
      // step 4：处理Ack: pipeline最后一个节点，现在enqueue一个Ack
      ((PacketResponder) responder.getRunnable()).enqueue(seqno,
          lastPacketInBlock, offsetInBlock, Status.SUCCESS);
    }

    /*
     * Send in-progress responses for the replaceBlock() calls back to caller to
     * avoid timeouts due to balancer throttling. HDFS-6247
     */
    if (isReplaceBlock
        && (Time.monotonicNow() - lastResponseTime > responseInterval)) {
      // step 5：向上游响应
      BlockOpResponseProto.Builder response = BlockOpResponseProto.newBuilder()
          .setStatus(Status.IN_PROGRESS);
      response.build().writeDelimitedTo(replyOut);
      replyOut.flush();

      lastResponseTime = Time.monotonicNow();
    }

    if (throttler != null) { // throttle I/O
      throttler.throttle(len);
    }
    
    return lastPacketInBlock?-1:len;
  }

  private static byte[] copyLastChunkChecksum(byte[] array, int size, int end) {
    return Arrays.copyOfRange(array, end - size, end);
  }

  private void manageWriterOsCache(long offsetInBlock) {
    try {
      if (outFd != null &&
          offsetInBlock > lastCacheManagementOffset + CACHE_DROP_LAG_BYTES) {
        long begin = Time.monotonicNow();
        //
        // For SYNC_FILE_RANGE_WRITE, we want to sync from
        // lastCacheManagementOffset to a position "two windows ago"
        //
        //                         <========= sync ===========>
        // +-----------------------O--------------------------X
        // start                  last                      curPos
        // of file                 
        //
        // 刷写脏页到磁盘
        if (syncBehindWrites) {
          if (syncBehindWritesInBackground) {
            this.datanode.getFSDataset().submitBackgroundSyncFileRangeRequest(
                block, outFd, lastCacheManagementOffset,
                offsetInBlock - lastCacheManagementOffset,
                NativeIO.POSIX.SYNC_FILE_RANGE_WRITE);
          } else {
            NativeIO.POSIX.syncFileRangeIfPossible(outFd,
                lastCacheManagementOffset, offsetInBlock
                    - lastCacheManagementOffset,
                NativeIO.POSIX.SYNC_FILE_RANGE_WRITE);
          }
        }
        //
        // For POSIX_FADV_DONTNEED, we want to drop from the beginning 
        // of the file to a position prior to the current position.
        //
        // <=== drop =====> 
        //                 <---W--->
        // +--------------+--------O--------------------------X
        // start        dropPos   last                      curPos
        // of file             
        //
        // 清除指定缓存
        long dropPos = lastCacheManagementOffset - CACHE_DROP_LAG_BYTES;
        if (dropPos > 0 && dropCacheBehindWrites) {
          NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(
              block.getBlockName(), outFd, 0, dropPos,
              NativeIO.POSIX.POSIX_FADV_DONTNEED);
        }
        lastCacheManagementOffset = offsetInBlock;
        long duration = Time.monotonicNow() - begin;
        if (duration > datanodeSlowLogThresholdMs) {
          LOG.warn("Slow manageWriterOsCache took " + duration
              + "ms (threshold=" + datanodeSlowLogThresholdMs + "ms)");
        }
      }
    } catch (Throwable t) {
      LOG.warn("Error managing cache for writer of block " + block, t);
    }
  }
  
  public void sendOOB() throws IOException, InterruptedException {
    ((PacketResponder) responder.getRunnable()).sendOOBResponse(PipelineAck
        .getRestartOOBStatus());
  }

  /**
   * 1、启动PacketResponder线程，处理Ack包的接收和转发
   * 2、接收、转发packet
   * 3、等待所有的packet被ack，然后结束PacketResponder线程
   */
  void receiveBlock(
      DataOutputStream mirrOut, // output to next datanode
      DataInputStream mirrIn,   // input from next datanode
      DataOutputStream replyOut,  // output to previous datanode
      String mirrAddr, DataTransferThrottler throttlerArg,
      DatanodeInfo[] downstreams,
      boolean isReplaceBlock) throws IOException {

      syncOnClose = datanode.getDnConf().syncOnClose;
      boolean responderClosed = false;
      mirrorOut = mirrOut;
      mirrorAddr = mirrAddr; // 下游DN地址
      throttler = throttlerArg;

      this.replyOut = replyOut; // 响应上游
      this.isReplaceBlock = isReplaceBlock;

    try {
      if (isClient && !isTransfer) {
        // step 1：启动PacketResponder线程，处理Ack包的接收和转发
        responder = new Daemon(datanode.threadGroup, 
            new PacketResponder(replyOut, mirrIn, downstreams));
        responder.start(); // start thread to processes responses
      }

      // step 2：接收、转发packet
      while (receivePacket() >= 0) { /* Receive until the last packet */ }

      // wait for all outstanding packet responses. And then
      // indicate responder to gracefully shutdown.
      // Mark that responder has been closed for future processing
      if (responder != null) {
        // step 3：等待所有的packet被ack，然后结束PacketResponder线程
        ((PacketResponder)responder.getRunnable()).close();
        responderClosed = true;
      }

      // If this write is for a replication or transfer-RBW/Finalized,
      // then finalize block or convert temporary to RBW.
      // For client-writes, the block is finalized in the PacketResponder.
      if (isDatanode || isTransfer) { // 如果是DN或复制时
        // Hold a volume reference to finalize block.
        try (ReplicaHandler handler = claimReplicaHandler()) {
          // close the block/crc files
          close();
          block.setNumBytes(replicaInfo.getNumBytes());

          if (stage == BlockConstructionStage.TRANSFER_RBW) {
            // for TRANSFER_RBW, convert temporary to RBW
            // 从临时目录移动到RBW目录
            datanode.data.convertTemporaryToRbw(block);
          } else {
            // for isDatnode or TRANSFER_FINALIZED
            // Finalize the block.
            datanode.data.finalizeBlock(block); // 从RBW目录移动到finalize目录
          }
        }
        datanode.metrics.incrBlocksWritten();
      }

    } catch (IOException ioe) {
      replicaInfo.releaseAllBytesReserved();
      if (datanode.isRestarting()) {
        // Do not throw if shutting down for restart. Otherwise, it will cause
        // premature termination of responder.
        LOG.info("Shutting down for restart (" + block + ").");
      } else {
        LOG.info("Exception for " + block, ioe);
        throw ioe;
      }
    } finally {
      // Clear the previous interrupt state of this thread.
      Thread.interrupted();

      // If a shutdown for restart was initiated, upstream needs to be notified.
      // There is no need to do anything special if the responder was closed
      // normally.
      if (!responderClosed) { // Data transfer was not complete.
        if (responder != null) {
          // In case this datanode is shutting down for quick restart,
          // send a special ack upstream.
          if (datanode.isRestarting() && isClient && !isTransfer) { // DN将要重启
            File blockFile = ((ReplicaInPipeline)replicaInfo).getBlockFile();
            File restartMeta = new File(blockFile.getParent()  + 
                File.pathSeparator + "." + blockFile.getName() + ".restart");
            if (restartMeta.exists() && !restartMeta.delete()) {
              LOG.warn("Failed to delete restart meta file: " +
                  restartMeta.getPath());
            }
            try (Writer out = new OutputStreamWriter(
                new FileOutputStream(restartMeta), "UTF-8")) {
              // write out the current time.
              out.write(Long.toString(Time.now() + restartBudget));
              out.flush();
            } catch (IOException ioe) {
              // The worst case is not recovering this RBW replica. 
              // Client will fall back to regular pipeline recovery.
            } finally {
              IOUtils.cleanup(LOG, out);
            }
            try {              
              // Even if the connection is closed after the ack packet is
              // flushed, the client can react to the connection closure 
              // first. Insert a delay to lower the chance of client 
              // missing the OOB ack.
              Thread.sleep(1000);
            } catch (InterruptedException ie) {
              // It is already going down. Ignore this.
            }
          }
          responder.interrupt();
        }
        IOUtils.closeStream(this);
        cleanupBlock();
      }
      if (responder != null) {
        try {
          responder.interrupt();
          // join() on the responder should timeout a bit earlier than the
          // configured deadline. Otherwise, the join() on this thread will
          // likely timeout as well.
          long joinTimeout = datanode.getDnConf().getXceiverStopTimeout();
          joinTimeout = joinTimeout > 1  ? joinTimeout*8/10 : joinTimeout;
          responder.join(joinTimeout);
          if (responder.isAlive()) {
            String msg = "Join on responder thread " + responder
                + " timed out";
            LOG.warn(msg + "\n" + StringUtils.getStackTrace(responder));
            throw new IOException(msg);
          }
        } catch (InterruptedException e) {
          responder.interrupt();
          // do not throw if shutting down for restart.
          if (!datanode.isRestarting()) {
            throw new IOException("Interrupted receiveBlock");
          }
        }
        responder = null;
      }
    }
  }

  /** Cleanup a partial block 
   * if this write is for a replication request (and not from a client)
   */
  private void cleanupBlock() throws IOException {
    if (isDatanode) {
      datanode.data.unfinalizeBlock(block);
    }
  }

  /**
   * Adjust the file pointer in the local meta file so that the last checksum
   * will be overwritten.
   */
  private void adjustCrcFilePosition() throws IOException {
    if (out != null) {
     out.flush();
    }
    if (checksumOut != null) {
      checksumOut.flush();
    }

    // rollback the position of the meta file
    datanode.data.adjustCrcChannelPosition(block, streams, checksumSize);
  }

  /**
   * Convert a checksum byte array to a long
   */
  static private long checksum2long(byte[] checksum) {
    long crc = 0L;
    for(int i=0; i<checksum.length; i++) {
      crc |= (0xffL&checksum[i])<<((checksum.length-i-1)*8);
    }
    return crc;
  }

  /**
   * reads in the partial crc chunk and computes checksum
   * of pre-existing data in partial chunk.
   */
  private Checksum computePartialChunkCrc(long blkoff, long ckoff)
      throws IOException {

    // find offset of the beginning of partial chunk.
    //
    int sizePartialChunk = (int) (blkoff % bytesPerChecksum);
    blkoff = blkoff - sizePartialChunk;
    if (LOG.isDebugEnabled()) {
      LOG.debug("computePartialChunkCrc for " + block
          + ": sizePartialChunk=" + sizePartialChunk
          + ", block offset=" + blkoff
          + ", metafile offset=" + ckoff);
    }

    // create an input stream from the block file
    // and read in partial crc chunk into temporary buffer
    //
    byte[] buf = new byte[sizePartialChunk];
    byte[] crcbuf = new byte[checksumSize];
    try (ReplicaInputStreams instr =
        datanode.data.getTmpInputStreams(block, blkoff, ckoff)) {
      IOUtils.readFully(instr.getDataIn(), buf, 0, sizePartialChunk);

      // open meta file and read in crc value computer earlier
      IOUtils.readFully(instr.getChecksumIn(), crcbuf, 0, crcbuf.length);
    }

    // compute crc of partial chunk from data read in the block file.
    final Checksum partialCrc = DataChecksum.newDataChecksum(
        diskChecksum.getChecksumType(), diskChecksum.getBytesPerChecksum());
    partialCrc.update(buf, 0, sizePartialChunk);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Read in partial CRC chunk from disk for " + block);
    }

    // paranoia! verify that the pre-computed crc matches what we
    // recalculated just now
    if (partialCrc.getValue() != checksum2long(crcbuf)) {
      String msg = "Partial CRC " + partialCrc.getValue() +
                   " does not match value computed the " +
                   " last time file was closed " +
                   checksum2long(crcbuf);
      throw new IOException(msg);
    }
    return partialCrc;
  }

  /** The caller claims the ownership of the replica handler. */
  private ReplicaHandler claimReplicaHandler() {
    ReplicaHandler handler = replicaHandler;
    replicaHandler = null;
    return handler;
  }

  private static enum PacketResponderType {
    NON_PIPELINE, LAST_IN_PIPELINE, HAS_DOWNSTREAM_IN_PIPELINE
  }

  /**
   * Processes responses from downstream datanodes in the pipeline
   * and sends back replies to the originator.
   */
  class PacketResponder implements Runnable, Closeable {   
    /** queue for packets waiting for ack - synchronization using monitor lock */
    private final LinkedList<Packet> ackQueue = new LinkedList<Packet>(); 
    /** the thread that spawns this responder */
    private final Thread receiverThread = Thread.currentThread();
    /** is this responder running? - synchronization using monitor lock */
    private volatile boolean running = true;
    /** input from the next downstream datanode */
    private final DataInputStream downstreamIn;
    /** output to upstream datanode/client */
    private final DataOutputStream upstreamOut;
    /** The type of this responder */
    private final PacketResponderType type;
    /** for log and error messages */
    private final String myString; 
    private boolean sending = false;

    @Override
    public String toString() {
      return myString;
    }

    PacketResponder(final DataOutputStream upstreamOut,
        final DataInputStream downstreamIn, final DatanodeInfo[] downstreams) {
      this.downstreamIn = downstreamIn;
      this.upstreamOut = upstreamOut;

      this.type = downstreams == null? PacketResponderType.NON_PIPELINE
          : downstreams.length == 0? PacketResponderType.LAST_IN_PIPELINE
              : PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE;

      final StringBuilder b = new StringBuilder(getClass().getSimpleName())
          .append(": ").append(block).append(", type=").append(type);
      if (type != PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE) {
        b.append(", downstreams=").append(downstreams.length)
            .append(":").append(Arrays.asList(downstreams));
      }
      this.myString = b.toString();
    }

    private boolean isRunning() {
      // When preparing for a restart, it should continue to run until
      // interrupted by the receiver thread.
      return running && (datanode.shouldRun || datanode.isRestarting());
    }
    
    /**
     * enqueue the seqno that is still be to acked by the downstream datanode.
     * @param seqno sequence number of the packet
     * @param lastPacketInBlock if true, this is the last packet in block
     * @param offsetInBlock offset of this packet in block
     */
    // 添加到ackQueue，等待下游ack
    void enqueue(final long seqno, final boolean lastPacketInBlock,
        final long offsetInBlock, final Status ackStatus) {
      final Packet p = new Packet(seqno, lastPacketInBlock, offsetInBlock,
          System.nanoTime(), ackStatus);
      if(LOG.isDebugEnabled()) {
        LOG.debug(myString + ": enqueue " + p);
      }
      synchronized(ackQueue) {
        if (running) {
          ackQueue.addLast(p); // 添加到ackQueue
          ackQueue.notifyAll();
        }
      }
    }

    /**
     * Send an OOB response. If all acks have been sent already for the block
     * and the responder is about to close, the delivery is not guaranteed.
     * This is because the other end can close the connection independently.
     * An OOB coming from downstream will be automatically relayed upstream
     * by the responder. This method is used only by originating datanode.
     *
     * @param ackStatus the type of ack to be sent
     */
    void sendOOBResponse(final Status ackStatus) throws IOException,
        InterruptedException {
      if (!running) {
        LOG.info("Cannot send OOB response " + ackStatus + 
            ". Responder not running.");
        return;
      }

      synchronized(this) {
        if (sending) {
          wait(PipelineAck.getOOBTimeout(ackStatus));
          // Didn't get my turn in time. Give up.
          if (sending) {
            throw new IOException("Could not send OOB reponse in time: "
                + ackStatus);
          }
        }
        sending = true;
      }

      LOG.info("Sending an out of band ack of type " + ackStatus);
      try {
        sendAckUpstreamUnprotected(null, PipelineAck.UNKOWN_SEQNO, 0L, 0L,
            PipelineAck.combineHeader(datanode.getECN(), ackStatus));
      } finally {
        // Let others send ack. Unless there are miltiple OOB send
        // calls, there can be only one waiter, the responder thread.
        // In any case, only one needs to be notified.
        synchronized(this) {
          sending = false;
          notify();
        }
      }
    }
    
    /** Wait for a packet with given {@code seqno} to be enqueued to ackQueue */
    Packet waitForAckHead(long seqno) throws InterruptedException {
      synchronized(ackQueue) {
        while (isRunning() && ackQueue.size() == 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(myString + ": seqno=" + seqno +
                      " waiting for local datanode to finish write.");
          }
          ackQueue.wait();
        }
        return isRunning() ? ackQueue.getFirst() : null;
      }
    }

    /**
     * wait for all pending packets to be acked. Then shutdown thread.
     */
    @Override
    public void close() {
      synchronized(ackQueue) {
        while (isRunning() && ackQueue.size() != 0) { // 等待所有的packet被ack
          try {
            ackQueue.wait();
          } catch (InterruptedException e) {
            running = false;
            Thread.currentThread().interrupt();
          }
        }
        if(LOG.isDebugEnabled()) {
          LOG.debug(myString + ": closing");
        }
        running = false;
        ackQueue.notifyAll();
      }

      synchronized(this) {
        running = false;
        notifyAll();
      }
    }

    /**
     * Thread to process incoming acks.
     * @see java.lang.Runnable#run()
     *
     * 1、从下游流中读取一个Ack
     * 2、从ackQueue中获取一个等待Ack的packet
     * 3、对比Ack
     * 4、向上游发送Ack
     * 5、移除已经Ack过的packet
     */
    @Override
    public void run() {
      boolean lastPacketInBlock = false;
      final long startTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0;
      while (isRunning() && !lastPacketInBlock) {
        long totalAckTimeNanos = 0;
        boolean isInterrupted = false;
        try {
          Packet pkt = null;
          long expected = -2;
          PipelineAck ack = new PipelineAck();
          long seqno = PipelineAck.UNKOWN_SEQNO;
          long ackRecvNanoTime = 0;
          try {
            if (type != PacketResponderType.LAST_IN_PIPELINE && !mirrorError) {
              // read an ack from downstream datanode
              // step 1：从下游流中读取一个Ack
              ack.readFields(downstreamIn);
              ackRecvNanoTime = System.nanoTime();
              if (LOG.isDebugEnabled()) {
                LOG.debug(myString + " got " + ack);
              }
              // Process an OOB ACK.
              Status oobStatus = ack.getOOBStatus();
              if (oobStatus != null) { // 如果有OOB消息，则立即转发给上游(DN写数据时，如果发生重启，会向上游发送一个OOB消息)
                LOG.info("Relaying an out of band ack of type " + oobStatus);
                sendAckUpstream(ack, PipelineAck.UNKOWN_SEQNO, 0L, 0L,
                    PipelineAck.combineHeader(datanode.getECN(),
                      Status.SUCCESS));
                continue;
              }
              seqno = ack.getSeqno();
            }
            if (seqno != PipelineAck.UNKOWN_SEQNO
                || type == PacketResponderType.LAST_IN_PIPELINE) {
              // step 2：从ackQueue中获取一个等待Ack的packet
              pkt = waitForAckHead(seqno);
              if (!isRunning()) {
                break;
              }
              expected = pkt.seqno;
              // step 3：对比Ack
              if (type == PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE
                  && seqno != expected) { // seqno和期望的是否一致
                throw new IOException(myString + "seqno: expected=" + expected
                    + ", received=" + seqno);
              }
              if (type == PacketResponderType.HAS_DOWNSTREAM_IN_PIPELINE) {
                // The total ack time includes the ack times of downstream
                // nodes.
                // The value is 0 if this responder doesn't have a downstream
                // DN in the pipeline.
                totalAckTimeNanos = ackRecvNanoTime - pkt.ackEnqueueNanoTime;
                // Report the elapsed time from ack send to ack receive minus
                // the downstream ack time.
                long ackTimeNanos = totalAckTimeNanos
                    - ack.getDownstreamAckTimeNanos();
                if (ackTimeNanos < 0) {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Calculated invalid ack time: " + ackTimeNanos
                        + "ns.");
                  }
                } else {
                  datanode.metrics.addPacketAckRoundTripTimeNanos(ackTimeNanos);
                }
              }
              lastPacketInBlock = pkt.lastPacketInBlock;
            }
          } catch (InterruptedException ine) {
            isInterrupted = true;
          } catch (IOException ioe) {
            if (Thread.interrupted()) {
              isInterrupted = true;
            } else if (ioe instanceof EOFException && !packetSentInTime()) {
              // The downstream error was caused by upstream including this
              // node not sending packet in time. Let the upstream determine
              // who is at fault.  If the immediate upstream node thinks it
              // has sent a packet in time, this node will be reported as bad.
              // Otherwise, the upstream node will propagate the error up by
              // closing the connection.
              LOG.warn("The downstream error might be due to congestion in " +
                  "upstream including this node. Propagating the error: ",
                  ioe);
              throw ioe;
            } else {
              // continue to run even if can not read from mirror
              // notify client of the error
              // and wait for the client to shut down the pipeline
              mirrorError = true; // 如果从下游读取数据时发送异常，则设置为true
              LOG.info(myString, ioe);
            }
          }

          if (Thread.interrupted() || isInterrupted) {
            /*
             * The receiver thread cancelled this thread. We could also check
             * any other status updates from the receiver thread (e.g. if it is
             * ok to write to replyOut). It is prudent to not send any more
             * status back to the client because this datanode has a problem.
             * The upstream datanode will detect that this datanode is bad, and
             * rightly so.
             *
             * The receiver thread can also interrupt this thread for sending
             * an out-of-band response upstream.
             */
            LOG.info(myString + ": Thread is interrupted.");
            running = false;
            continue;
          }

          if (lastPacketInBlock) {
            // Finalize the block and close the block file
            finalizeBlock(startTime); // 完成一个block，向NN增量块汇报
          }

          Status myStatus = pkt != null ? pkt.ackStatus : Status.SUCCESS;
          // step 4：向上游发送Ack（复制上游状态，加入当前节点状态）
          sendAckUpstream(ack, expected, totalAckTimeNanos,
            (pkt != null ? pkt.offsetInBlock : 0),
            PipelineAck.combineHeader(datanode.getECN(), myStatus));
          if (pkt != null) {
            // remove the packet from the ack queue
            // step 5：移除已经Ack过的packet
            removeAckHead();
          }
        } catch (IOException e) {
          LOG.warn("IOException in BlockReceiver.run(): ", e);
          if (running) {
            datanode.checkDiskErrorAsync(); // 检查磁盘是否有问题
            LOG.info(myString, e);
            running = false;
            if (!Thread.interrupted()) { // failure not caused by interruption
              receiverThread.interrupt(); // 中断当前线程（PacketResponder）
            }
          }
        } catch (Throwable e) {
          if (running) {
            LOG.info(myString, e);
            running = false;
            receiverThread.interrupt(); // 中断当前线程（PacketResponder）
          }
        }
      }
      LOG.info(myString + " terminating");
    }
    
    /**
     * Finalize the block and close the block file
     * @param startTime time when BlockReceiver started receiving the block
     */
    private void finalizeBlock(long startTime) throws IOException {
      long endTime = 0;
      // Hold a volume reference to finalize block.
      try (ReplicaHandler handler = BlockReceiver.this.claimReplicaHandler()) {
        BlockReceiver.this.close();
        endTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0;
        block.setNumBytes(replicaInfo.getNumBytes());
        datanode.data.finalizeBlock(block);
      }

      if (pinning) {
        datanode.data.setPinning(block);
      }
      
      datanode.closeBlock(
          block, DataNode.EMPTY_DEL_HINT, replicaInfo.getStorageUuid());
      if (ClientTraceLog.isInfoEnabled() && isClient) {
        long offset = 0;
        DatanodeRegistration dnR = datanode.getDNRegistrationForBP(block
            .getBlockPoolId());
        ClientTraceLog.info(String.format(DN_CLIENTTRACE_FORMAT, inAddr,
            myAddr, block.getNumBytes(), "HDFS_WRITE", clientname, offset,
            dnR.getDatanodeUuid(), block, endTime - startTime));
      } else {
        LOG.info("Received " + block + " size " + block.getNumBytes()
            + " from " + inAddr);
      }
    }
    
    /**
     * The wrapper for the unprotected version. This is only called by
     * the responder's run() method.
     *
     * @param ack Ack received from downstream
     * @param seqno sequence number of ack to be sent upstream
     * @param totalAckTimeNanos total ack time including all the downstream
     *          nodes
     * @param offsetInBlock offset in block for the data in packet
     * @param myHeader the local ack header
     */
    private void sendAckUpstream(PipelineAck ack, long seqno,
        long totalAckTimeNanos, long offsetInBlock,
        int myHeader) throws IOException {
      try {
        // Wait for other sender to finish. Unless there is an OOB being sent,
        // the responder won't have to wait.
        synchronized(this) {
          while(sending) {
            wait(); // 同时只能发送一个Ack响应到上游
          }
          sending = true;
        }

        try {
          if (!running) return;
          sendAckUpstreamUnprotected(ack, seqno, totalAckTimeNanos,
              offsetInBlock, myHeader);
        } finally {
          synchronized(this) {
            sending = false;
            notify();
          }
        }
      } catch (InterruptedException ie) {
        // The responder was interrupted. Make it go down without
        // interrupting the receiver(writer) thread.  
        running = false;
      }
    }

    /**
     * @param ack Ack received from downstream
     * @param seqno sequence number of ack to be sent upstream
     * @param totalAckTimeNanos total ack time including all the downstream
     *          nodes
     * @param offsetInBlock offset in block for the data in packet
     * @param myHeader the local ack header
     */
    private void sendAckUpstreamUnprotected(PipelineAck ack, long seqno,
        long totalAckTimeNanos, long offsetInBlock, int myHeader)
        throws IOException {
      final int[] replies;
      if (ack == null) { // 如果下游节点发送的是OOB消息，则保留下游节点的响应内容
        // A new OOB response is being sent from this node. Regardless of
        // downstream nodes, reply should contain one reply.
        replies = new int[] { myHeader };
      } else if (mirrorError) { // ack read error 如果当前节点读取下游节点响应时发生异常，则将当前节点的错误信息记录在响应消息中
        int h = PipelineAck.combineHeader(datanode.getECN(), Status.SUCCESS);
        int h1 = PipelineAck.combineHeader(datanode.getECN(), Status.ERROR);
        replies = new int[] {h, h1};
      } else {
        short ackLen = type == PacketResponderType.LAST_IN_PIPELINE ? 0 : ack
            .getNumOfReplies();
        replies = new int[ackLen + 1];
        replies[0] = myHeader; // 当前节点的响应内容
        for (int i = 0; i < ackLen; ++i) {
          replies[i + 1] = ack.getHeaderFlag(i); // 下游节点的响应内容
        }
        // If the mirror has reported that it received a corrupt packet,
        // do self-destruct to mark myself bad, instead of making the
        // mirror node bad. The mirror is guaranteed to be good without
        // corrupt data on disk.
        if (ackLen > 0 && PipelineAck.getStatusFromHeader(replies[1]) ==
          Status.ERROR_CHECKSUM) { // 如果下游节点检测到checksum错误，则当前节点发送的数据有错误
          throw new IOException("Shutting down writer and responder "
              + "since the down streams reported the data sent by this "
              + "thread is corrupt");
        }
      }
      PipelineAck replyAck = new PipelineAck(seqno, replies,
          totalAckTimeNanos); // 构造新的响应消息
      if (replyAck.isSuccess()
          && offsetInBlock > replicaInfo.getBytesAcked()) {
        replicaInfo.setBytesAcked(offsetInBlock);
      }
      // send my ack back to upstream datanode
      long begin = Time.monotonicNow();
      replyAck.write(upstreamOut); // 发送给上游
      upstreamOut.flush();
      long duration = Time.monotonicNow() - begin;
      if (duration > datanodeSlowLogThresholdMs) {
        LOG.warn("Slow PacketResponder send ack to upstream took " + duration
            + "ms (threshold=" + datanodeSlowLogThresholdMs + "ms), " + myString
            + ", replyAck=" + replyAck);
      } else if (LOG.isDebugEnabled()) {
        LOG.debug(myString + ", replyAck=" + replyAck);
      }

      // If a corruption was detected in the received data, terminate after
      // sending ERROR_CHECKSUM back.
      Status myStatus = PipelineAck.getStatusFromHeader(myHeader);
      if (myStatus == Status.ERROR_CHECKSUM) { // 当前节点检测到checksum错误(pipeline最后一个节点会做checksum检查)
        throw new IOException("Shutting down writer and responder "
            + "due to a checksum error in received data. The error "
            + "response has been sent upstream.");
      }
    }
    
    /**
     * Remove a packet from the head of the ack queue
     * 
     * This should be called only when the ack queue is not empty
     */
    private void removeAckHead() { // 移除queue中的第一个packet（已经Ack过）
      synchronized(ackQueue) {
        ackQueue.removeFirst();
        ackQueue.notifyAll();
      }
    }
  }

  /**
   * This information is cached by the Datanode in the ackQueue.
   */
  private static class Packet {
    final long seqno;
    final boolean lastPacketInBlock;
    final long offsetInBlock;
    final long ackEnqueueNanoTime;
    final Status ackStatus;

    Packet(long seqno, boolean lastPacketInBlock, long offsetInBlock,
        long ackEnqueueNanoTime, Status ackStatus) {
      this.seqno = seqno;
      this.lastPacketInBlock = lastPacketInBlock;
      this.offsetInBlock = offsetInBlock;
      this.ackEnqueueNanoTime = ackEnqueueNanoTime;
      this.ackStatus = ackStatus;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(seqno=" + seqno
        + ", lastPacketInBlock=" + lastPacketInBlock
        + ", offsetInBlock=" + offsetInBlock
        + ", ackEnqueueNanoTime=" + ackEnqueueNanoTime
        + ", ackStatus=" + ackStatus
        + ")";
    }
  }
}
