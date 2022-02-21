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

package org.apache.hadoop.ipc;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Determines which queue to start reading from, occasionally drawing from
 * low-priority queues in order to prevent starvation. Given the pull pattern
 * [9, 4, 1] for 3 queues:
 *
 * The cycle is (a minimum of) 9+4+1=14 reads.
 * Queue 0 is read (at least) 9 times
 * Queue 1 is read (at least) 4 times
 * Queue 2 is read (at least) 1 time
 * Repeat
 *
 * There may be more reads than the minimum due to race conditions. This is
 * allowed by design for performance reasons.
 */
public class WeightedRoundRobinMultiplexer implements RpcMultiplexer {
  // Config keys
  public static final String IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY =
    "faircallqueue.multiplexer.weights";

  public static final Logger LOG =
      LoggerFactory.getLogger(WeightedRoundRobinMultiplexer.class);

  private final int numQueues; // The number of queues under our provisioning

  private final AtomicInteger currentQueueIndex; // Current queue we're serving 当前queue下标
  private final AtomicInteger requestsLeft; // Number of requests left for this queue 从当前queue取多少个元素

  private int[] queueWeights; // The weights for each queue 每个queue的权重，每次从每个queue中取多少个元素

  public WeightedRoundRobinMultiplexer(int aNumQueues, String ns,
    Configuration conf) {
    if (aNumQueues <= 0) {
      throw new IllegalArgumentException("Requested queues (" + aNumQueues +
        ") must be greater than zero.");
    }

    this.numQueues = aNumQueues;
    this.queueWeights = conf.getInts(ns + "." +
      IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY);

    if (this.queueWeights.length == 0) {
      this.queueWeights = getDefaultQueueWeights(this.numQueues); // 默认权重：[1,2,4,8,16]
    } else if (this.queueWeights.length != this.numQueues) {
      throw new IllegalArgumentException(ns + "." +
        IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY + " must specify exactly " +
        this.numQueues + " weights: one for each priority level.");
    }

    this.currentQueueIndex = new AtomicInteger(0);
    this.requestsLeft = new AtomicInteger(this.queueWeights[0]);

    LOG.info("WeightedRoundRobinMultiplexer is being used.");
  }

  /**
   * Creates default weights for each queue. The weights are 2^N.
   */
  private int[] getDefaultQueueWeights(int aNumQueues) { // 默认权重：[1,2,4,8,16]
    int[] weights = new int[aNumQueues];

    int weight = 1; // Start low
    for(int i = aNumQueues - 1; i >= 0; i--) { // Start at lowest queue
      weights[i] = weight;
      weight *= 2; // Double every iteration
    }
    return weights;
  }

  /**
   * Move to the next queue.
   */
  private void moveToNextQueue() { // 下一个queue
    int thisIdx = this.currentQueueIndex.get();

    // Wrap to fit in our bounds
    int nextIdx = (thisIdx + 1) % this.numQueues; // 下一个queue下标

    // Set to next index: once this is called, requests will start being
    // drawn from nextIdx, but requestsLeft will continue to decrement into
    // the negatives
    this.currentQueueIndex.set(nextIdx); // 设置下一个queue下标为当前

    // Finally, reset requestsLeft. This will enable moveToNextQueue to be
    // called again, for the new currentQueueIndex
    this.requestsLeft.set(this.queueWeights[nextIdx]);
    LOG.debug("Moving to next queue from queue index {} to index {}, " +
        "number of requests left for current queue: {}.",
        thisIdx, nextIdx, requestsLeft);
  }

  /**
   * Advances the index, which will change the current index
   * if called enough times.
   */
  private void advanceIndex() {
    // Since we did read, we should decrement
    int requestsLeftVal = this.requestsLeft.decrementAndGet();

    // Strict compare with zero (instead of inequality) so that if another
    // thread decrements requestsLeft, only one thread will be responsible
    // for advancing currentQueueIndex
    if (requestsLeftVal == 0) {
      // This is guaranteed to be called exactly once per currentQueueIndex
      this.moveToNextQueue();
    }
  }

  /**
   * Gets the current index. Should be accompanied by a call to
   * advanceIndex at some point.
   */
  private int getCurrentIndex() {
    return this.currentQueueIndex.get();
  }

  /**
   * Use the mux by getting and advancing index.
   */
  public int getAndAdvanceCurrentIndex() { // 当前queue下标
    int idx = this.getCurrentIndex();
    this.advanceIndex();
    return idx;
  }

}
