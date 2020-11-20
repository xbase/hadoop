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

import java.util.LinkedList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.util.LightWeightGSet;

/**
 * BlockInfo class maintains for a given block
 * the {@link INodeFile} it is part of and datanodes where the replicas of 
 * the block are stored.
 * BlockInfo class maintains for a given block
 * the {@link BlockCollection} it is part of and datanodes where the replicas of 
 * the block are stored.
 */
// 代表一个COMPLETE状态的Block
// 此类大部分方法，都和管理Replica相关
@InterfaceAudience.Private
public class BlockInfoContiguous extends Block
    implements LightWeightGSet.LinkedElement {
  public static final BlockInfoContiguous[] EMPTY_ARRAY = {};

  // 这个block属于哪个INode（INode继承BlockCollection）
  private BlockCollection bc;

  /** For implementing {@link LightWeightGSet.LinkedElement} interface */
  private LightWeightGSet.LinkedElement nextLinkedElement; // GSet类似于HashMap，当下标冲突时，使用链表解决冲突

  /**
   * This array contains triplets of references. For each i-th storage, the
   * block belongs to triplets[3*i] is the reference to the
   * {@link DatanodeStorageInfo} and triplets[3*i+1] and triplets[3*i+2] are
   * references to the previous and the next blocks, respectively, in the list
   * of blocks belonging to this storage.
   * 
   * Using previous and next in Object triplets is done instead of a
   * {@link LinkedList} list to efficiently use memory. With LinkedList the cost
   * per replica is 42 bytes (LinkedList#Entry object per replica) versus 16
   * bytes using the triplets.
   */
  // 副本的存储位置 storage list
  // 在HDFS2.6之前，使用DatanodeDescriptor对象描述（表示存储在哪个DN节点）
  // 现在使用DatanodeStorageInfo描述（表示DN节点的哪个存储目录）
  private Object[] triplets;

  /**
   * Construct an entry for blocksmap
   * @param replication the block's replication factor
   */
  public BlockInfoContiguous(short replication) {
    this.triplets = new Object[3*replication];
    this.bc = null;
  }
  
  public BlockInfoContiguous(Block blk, short replication) {
    super(blk);
    this.triplets = new Object[3*replication];
    this.bc = null;
  }

  /**
   * Copy construction.
   * This is used to convert BlockInfoUnderConstruction
   * @param from BlockInfo to copy from.
   */
  protected BlockInfoContiguous(BlockInfoContiguous from) {
    this(from, from.bc.getBlockReplication());
    this.bc = from.bc;
  }

  public BlockCollection getBlockCollection() {
    return bc;
  }

  public void setBlockCollection(BlockCollection bc) {
    this.bc = bc;
  }

  // 获取此block指定下标副本的存储DN
  public DatanodeDescriptor getDatanode(int index) {
    DatanodeStorageInfo storage = getStorageInfo(index);
    return storage == null ? null : storage.getDatanodeDescriptor();
  }

  // 获取此block指定下标副本的存储目录
  DatanodeStorageInfo getStorageInfo(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3 < triplets.length : "Index is out of bound";
    return (DatanodeStorageInfo)triplets[index*3];
  }

  // 获取同存储目录链表的前一个block
  private BlockInfoContiguous getPrevious(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3+1 < triplets.length : "Index is out of bound";
    BlockInfoContiguous info = (BlockInfoContiguous)triplets[index*3+1];
    assert info == null || 
        info.getClass().getName().startsWith(BlockInfoContiguous.class.getName()) :
              "BlockInfo is expected at " + index*3;
    return info;
  }

  // 获取同存储目录链表的后一个block
  BlockInfoContiguous getNext(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3+2 < triplets.length : "Index is out of bound";
    BlockInfoContiguous info = (BlockInfoContiguous)triplets[index*3+2];
    assert info == null || info.getClass().getName().startsWith(
        BlockInfoContiguous.class.getName()) :
        "BlockInfo is expected at " + index*3;
    return info;
  }

  // 设置此block指定副本的存储位置
  private void setStorageInfo(int index, DatanodeStorageInfo storage) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3 < triplets.length : "Index is out of bound";
    triplets[index*3] = storage;
  }

  /**
   * Return the previous block on the block list for the datanode at
   * position index. Set the previous block on the list to "to".
   *
   * @param index - the datanode index
   * @param to - block to be set to previous on the list of blocks
   * @return current previous block on the list of blocks
   */
  // 设置同存储目录链表的前一个block为to，并且返回设置前的block
  // 设置下标为index副本的前驱为to
  private BlockInfoContiguous setPrevious(int index, BlockInfoContiguous to) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3+1 < triplets.length : "Index is out of bound";
    BlockInfoContiguous info = (BlockInfoContiguous)triplets[index*3+1];
    triplets[index*3+1] = to;
    return info;
  }

  /**
   * Return the next block on the block list for the datanode at
   * position index. Set the next block on the list to "to".
   *
   * @param index - the datanode index
   * @param to - block to be set to next on the list of blocks
   *    * @return current next block on the list of blocks
   */
  // 设置同存储目录链表的后一个block为to，并且返回设置前的block
  private BlockInfoContiguous setNext(int index, BlockInfoContiguous to) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index*3+2 < triplets.length : "Index is out of bound";
    BlockInfoContiguous info = (BlockInfoContiguous)triplets[index*3+2];
    triplets[index*3+2] = to;
    return info;
  }

  // 当前block的副本总数，并不等于期望副本数
  // 当修改文件的副本数时，只修改INodeFile对象中的副本数字段
  // 当块汇报时，会检查block的capacity是否充足，如果不足，会调用ensureCapacity()方法扩容
  public int getCapacity() {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert triplets.length % 3 == 0 : "Malformed BlockInfo";
    return triplets.length / 3;
  }

  /**
   * Ensure that there is enough  space to include num more triplets.
   * @return first free triplet index.
   */
  // 如果triplets数组空间不足，则扩充
  private int ensureCapacity(int num) { // num 需要增加的副本数
    assert this.triplets != null : "BlockInfo is not initialized";
    int last = numNodes(); // 最后一个副本下标
    if(triplets.length >= (last+num)*3)
      return last; // 空间充足，无需扩充
    /* Not enough space left. Create a new array. Should normally 
     * happen only when replication is manually increased by the user. */
    Object[] old = triplets;
    triplets = new Object[(last+num)*3];
    System.arraycopy(old, 0, triplets, 0, last*3);
    return last;
  }

  /**
   * Count the number of data-nodes the block belongs to.
   */
  public int numNodes() { // 实际副本数
    assert this.triplets != null : "BlockInfo is not initialized";
    assert triplets.length % 3 == 0 : "Malformed BlockInfo";
    for(int idx = getCapacity()-1; idx >= 0; idx--) {
      if(getDatanode(idx) != null)
        return idx+1;
    }
    return 0;
  }

  /**
   * Add a {@link DatanodeStorageInfo} location for a block
   */
  // 刚申请的block为UC状态，所有的副本信息保存在BlockInfoContiguousUnderConstruction.replicas中
  // 当接受增量块汇报时，再调用此方法，添加到triplets中
  boolean addStorage(DatanodeStorageInfo storage) { // 添加一个副本
    // find the last null node
    int lastNode = ensureCapacity(1);
    setStorageInfo(lastNode, storage);
    setNext(lastNode, null);
    setPrevious(lastNode, null);
    return true;
  }

  /**
   * Remove {@link DatanodeStorageInfo} location for a block
   */
  // 从当前block中删除一个副本（从triplets数组中，删除这个存储目录）
  boolean removeStorage(DatanodeStorageInfo storage) {
    // 当前存储目录的下标
    int dnIndex = findStorageInfo(storage);
    if(dnIndex < 0) // the node is not found
      return false;
    assert getPrevious(dnIndex) == null && getNext(dnIndex) == null : 
      "Block is still in the list and must be removed first.";
    // find the last not null node
    int lastNode = numNodes()-1; 
    // replace current node triplet by the lastNode one 用最后一个副本，替换当前副本
    setStorageInfo(dnIndex, getStorageInfo(lastNode));
    setNext(dnIndex, getNext(lastNode)); 
    setPrevious(dnIndex, getPrevious(lastNode)); 
    // set the last triplet to null 设置最后一个副本为null
    setStorageInfo(lastNode, null);
    setNext(lastNode, null); 
    setPrevious(lastNode, null); 
    return true;
  }

  /**
   * Find specified DatanodeDescriptor.
   * @return index or -1 if not found.
   */
  boolean findDatanode(DatanodeDescriptor dn) { // 是否有副本，存储在此DN
    int len = getCapacity();
    for(int idx = 0; idx < len; idx++) {
      DatanodeDescriptor cur = getDatanode(idx);
      if(cur == dn) {
        return true;
      }
      if(cur == null) {
        break;
      }
    }
    return false;
  }

  /**
   * Find specified DatanodeStorageInfo.
   * @return DatanodeStorageInfo or null if not found.
   */
  DatanodeStorageInfo findStorageInfo(DatanodeDescriptor dn) { // 副本在此DN的哪个目录存储
    int len = getCapacity();
    for(int idx = 0; idx < len; idx++) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if(cur == null)
        break;
      if(cur.getDatanodeDescriptor() == dn)
        return cur;
    }
    return null;
  }
  
  /**
   * Find specified DatanodeStorageInfo.
   * @return index or -1 if not found.
   */
  int findStorageInfo(DatanodeStorageInfo storageInfo) { // 是否有副本，存储在此目录，找到则返回数组下标
    int len = getCapacity();
    for(int idx = 0; idx < len; idx++) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if (cur == storageInfo) {
        return idx;
      }
      if (cur == null) {
        break;
      }
    }
    return -1;
  }

  /**
   * Insert this block into the head of the list of blocks 
   * related to the specified DatanodeStorageInfo.
   * If the head is null then form a new list.
   * @return current block as the new head of the list.
   */
  // 在DN存储目录中添加一个block（添加到链表的头）
  // head：此存储目录的第一个block
  BlockInfoContiguous listInsert(BlockInfoContiguous head,
      DatanodeStorageInfo storage) {
    int dnIndex = this.findStorageInfo(storage);
    assert dnIndex >= 0 : "Data node is not found: current";
    assert getPrevious(dnIndex) == null && getNext(dnIndex) == null : 
            "Block is already in the list and cannot be inserted.";
    this.setPrevious(dnIndex, null);
    this.setNext(dnIndex, head);
    if(head != null)
      head.setPrevious(head.findStorageInfo(storage), this);
    return this;
  }

  /**
   * Remove this block from the list of blocks 
   * related to the specified DatanodeStorageInfo.
   * If this block is the head of the list then return the next block as 
   * the new head.
   * @return the new head of the list or null if the list becomes
   * empy after deletion.
   */
  // 在DN存储目录中删除当前block
  // head：此存储目录的第一个block
  BlockInfoContiguous listRemove(BlockInfoContiguous head,
      DatanodeStorageInfo storage) {
    if(head == null)
      return null;
    int dnIndex = this.findStorageInfo(storage);
    if(dnIndex < 0) // this block is not on the data-node list
      return head;

    BlockInfoContiguous next = this.getNext(dnIndex);
    BlockInfoContiguous prev = this.getPrevious(dnIndex);
    this.setNext(dnIndex, null);
    this.setPrevious(dnIndex, null);
    // 在链表中删除当前block
    if(prev != null)
      prev.setNext(prev.findStorageInfo(storage), next);
    if(next != null)
      next.setPrevious(next.findStorageInfo(storage), prev);
    // 如果链表的头被删除了（head == 当前block）
    if(this == head)  // removing the head
      head = next;
    return head;
  }

  /**
   * Remove this block from the list of blocks related to the specified
   * DatanodeDescriptor. Insert it into the head of the list of blocks.
   *
   * @return the new head of the list.
   */
  // 把当前节点移动到列表头
  public BlockInfoContiguous moveBlockToHead(BlockInfoContiguous head,
      DatanodeStorageInfo storage, int curIndex, int headIndex) {
    if (head == this) {
      return this;
    }
    // 把当前节点移动到列表头
    BlockInfoContiguous next = this.setNext(curIndex, head);
    BlockInfoContiguous prev = this.setPrevious(curIndex, null);
    head.setPrevious(headIndex, this);
    // 把当前节点从之前的位置移除
    prev.setNext(prev.findStorageInfo(storage), next);
    if (next != null) {
      next.setPrevious(next.findStorageInfo(storage), prev);
    }
    return this;
  }

  /**
   * BlockInfo represents a block that is not being constructed.
   * In order to start modifying the block, the BlockInfo should be converted
   * to {@link BlockInfoContiguousUnderConstruction}.
   * @return {@link BlockUCState#COMPLETE}
   */
  public BlockUCState getBlockUCState() {
    return BlockUCState.COMPLETE;
  }

  /**
   * Is this block complete?
   * 
   * @return true if the state of the block is {@link BlockUCState#COMPLETE}
   */
  public boolean isComplete() {
    return getBlockUCState().equals(BlockUCState.COMPLETE);
  }

  /**
   * Convert a complete block to an under construction block.
   * @return BlockInfoUnderConstruction -  an under construction block.
   */
  // 如果Block当前为Complete状态，则转换为UC
  // 如果已经是UC状态，则更新一下副本信息
  public BlockInfoContiguousUnderConstruction convertToBlockUnderConstruction(
      BlockUCState s, DatanodeStorageInfo[] targets) {
    if(isComplete()) { // Block的状态从complete转换为UC
      BlockInfoContiguousUnderConstruction ucBlock =
          new BlockInfoContiguousUnderConstruction(this,
          getBlockCollection().getBlockReplication(), s, targets);
      ucBlock.setBlockCollection(getBlockCollection());
      return ucBlock;
    }
    // the block is already under construction
    // 当前Block就是UC状态，更新一下副本信息
    BlockInfoContiguousUnderConstruction ucBlock =
        (BlockInfoContiguousUnderConstruction)this;
    ucBlock.setBlockUCState(s);
    ucBlock.setExpectedLocations(targets);
    ucBlock.setBlockCollection(getBlockCollection());
    return ucBlock;
  }

  @Override
  public int hashCode() {
    // Super implementation is sufficient
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    // Sufficient to rely on super's implementation
    return (this == obj) || super.equals(obj);
  }

  @Override
  public LightWeightGSet.LinkedElement getNext() {
    return nextLinkedElement;
  }

  @Override
  public void setNext(LightWeightGSet.LinkedElement next) {
    this.nextLinkedElement = next;
  }
}
