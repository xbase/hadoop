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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstracts queue operations for different blocking queues.
 */
public class CallQueueManager<E extends Schedulable>
    extends AbstractQueue<E> implements BlockingQueue<E> {
  public static final Logger LOG =
      LoggerFactory.getLogger(CallQueueManager.class);
  // Number of checkpoints for empty queue.
  private static final int CHECKPOINT_NUM = 20;
  // Interval to check empty queue.
  private static final long CHECKPOINT_INTERVAL_MS = 10;

  @SuppressWarnings("unchecked")
  static <E> Class<? extends BlockingQueue<E>> convertQueueClass(
      Class<?> queueClass, Class<E> elementClass) {
    return (Class<? extends BlockingQueue<E>>)queueClass;
  }

  @SuppressWarnings("unchecked")
  static Class<? extends RpcScheduler> convertSchedulerClass(
      Class<?> schedulerClass) {
    return (Class<? extends RpcScheduler>)schedulerClass;
  }

  private volatile boolean clientBackOffEnabled;
  private boolean serverFailOverEnabled; // 是否抛StandbyException，使client重试其他NN

  // Atomic refs point to active callQueue
  // We have two so we can better control swapping
  // 之所以同一个bq，用两个引用，是为了动态刷新时使用
  private final AtomicReference<BlockingQueue<E>> putRef;
  private final AtomicReference<BlockingQueue<E>> takeRef;

  private RpcScheduler scheduler; // 用于决定请求的优先级

  public CallQueueManager(Class<? extends BlockingQueue<E>> backingClass,
                          Class<? extends RpcScheduler> schedulerClass,
      boolean clientBackOffEnabled, int maxQueueSize, String namespace,
      Configuration conf) {
    int priorityLevels = parseNumLevels(namespace, conf); // 一共几个优先级
    this.scheduler = createScheduler(schedulerClass, priorityLevels,
        namespace, conf);
    BlockingQueue<E> bq = createCallQueueInstance(backingClass,
        priorityLevels, maxQueueSize, namespace, conf); // call queue
    this.clientBackOffEnabled = clientBackOffEnabled;
    this.serverFailOverEnabled = conf.getBoolean(
        namespace + "." +
        CommonConfigurationKeys.IPC_CALLQUEUE_SERVER_FAILOVER_ENABLE,
        CommonConfigurationKeys.IPC_CALLQUEUE_SERVER_FAILOVER_ENABLE_DEFAULT);
    this.putRef = new AtomicReference<BlockingQueue<E>>(bq);
    this.takeRef = new AtomicReference<BlockingQueue<E>>(bq);
    LOG.info("Using callQueue: {}, queueCapacity: {}, " +
        "scheduler: {}, ipcBackoff: {}.",
        backingClass, maxQueueSize, schedulerClass, clientBackOffEnabled);
  }

  @VisibleForTesting // only!
  CallQueueManager(BlockingQueue<E> queue, RpcScheduler scheduler,
      boolean clientBackOffEnabled, boolean serverFailOverEnabled) {
    this.putRef = new AtomicReference<BlockingQueue<E>>(queue);
    this.takeRef = new AtomicReference<BlockingQueue<E>>(queue);
    this.scheduler = scheduler;
    this.clientBackOffEnabled = clientBackOffEnabled;
    this.serverFailOverEnabled = serverFailOverEnabled;
  }

  private static <T extends RpcScheduler> T createScheduler( // 实例化RpcScheduler
      Class<T> theClass, int priorityLevels, String ns, Configuration conf) {
    // Used for custom, configurable scheduler
    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor(int.class,
          String.class, Configuration.class);
      return ctor.newInstance(priorityLevels, ns, conf);
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(theClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
    }

    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor(int.class);
      return ctor.newInstance(priorityLevels);
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(theClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
    }

    // Last attempt
    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor();
      return ctor.newInstance();
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(theClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
    }

    // Nothing worked
    throw new RuntimeException(theClass.getName() +
        " could not be constructed.");
  }

  private <T extends BlockingQueue<E>> T createCallQueueInstance( // 实例化CallQueue
      Class<T> theClass, int priorityLevels, int maxLen, String ns,
      Configuration conf) {

    // Used for custom, configurable callqueues
    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor(int.class,
          int.class, String.class, Configuration.class);
      return ctor.newInstance(priorityLevels, maxLen, ns, conf);
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(theClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
    }

    // Used for LinkedBlockingQueue, ArrayBlockingQueue, etc
    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor(int.class);
      return ctor.newInstance(maxLen);
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(theClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
    }

    // Last attempt
    try {
      Constructor<T> ctor = theClass.getDeclaredConstructor();
      return ctor.newInstance();
    } catch (RuntimeException e) {
      throw e;
    } catch (InvocationTargetException e) {
      throw new RuntimeException(theClass.getName()
          + " could not be constructed.", e.getCause());
    } catch (Exception e) {
    }

    // Nothing worked
    throw new RuntimeException(theClass.getName() +
      " could not be constructed.");
  }

  boolean isClientBackoffEnabled() {
    return clientBackOffEnabled;
  }

  // Based on policy to determine back off current call
  boolean shouldBackOff(Schedulable e) { // 是否应该backoff
    return scheduler.shouldBackOff(e);
  }

  void addResponseTime(String name, Schedulable e, ProcessingDetails details) { // 记录处理时间
    scheduler.addResponseTime(name, e, details);
  }

  // This should be only called once per call and cached in the call object
  int getPriorityLevel(Schedulable e) { // 获取优先级
    return scheduler.getPriorityLevel(e);
  }

  int getPriorityLevel(UserGroupInformation user) { // 获取优先级
    if (scheduler instanceof DecayRpcScheduler) {
      return ((DecayRpcScheduler)scheduler).getPriorityLevel(user);
    }
    return 0;
  }

  void setPriorityLevel(UserGroupInformation user, int priority) { // 指定优先级
    if (scheduler instanceof DecayRpcScheduler) {
      ((DecayRpcScheduler)scheduler).setPriorityLevel(user, priority);
    }
  }

  void setClientBackoffEnabled(boolean value) {
    clientBackOffEnabled = value;
  }

  /**
   * Insert e into the backing queue or block until we can.  If client
   * backoff is enabled this method behaves like add which throws if
   * the queue overflows.
   * If we block and the queue changes on us, we will insert while the
   * queue is drained.
   */
  @Override
  public void put(E e) throws InterruptedException { // 如果queue满，则等待，直到成功
    if (!isClientBackoffEnabled()) { // 是否开启backoff，如果没开就等待put成功
      putRef.get().put(e);
    } else if (shouldBackOff(e)) { // 是否要backoff
      throwBackoff();
    } else {
      // No need to re-check backoff criteria since they were just checked
      addInternal(e, false);
    }
  }

  @Override
  public boolean add(E e) { // 如果queue满，则抛异常
    return addInternal(e, true);
  }

  @VisibleForTesting
  boolean addInternal(E e, boolean checkBackoff) {
    if (checkBackoff && isClientBackoffEnabled() && shouldBackOff(e)) { // 是否要backoff
      throwBackoff();
    }
    try {
      return putRef.get().add(e);
    } catch (CallQueueOverflowException ex) {
      // queue provided a custom exception that may control if the client
      // should be disconnected.
      throw ex;
    } catch (IllegalStateException ise) {
      throwBackoff();
    }
    return true;
  }

  // ideally this behavior should be controllable too.
  private void throwBackoff() throws IllegalStateException {
    throw serverFailOverEnabled ?
        CallQueueOverflowException.FAILOVER :
        CallQueueOverflowException.DISCONNECT;
  }

  /**
   * Insert e into the backing queue.
   * Return true if e is queued.
   * Return false if the queue is full.
   */
  @Override
  public boolean offer(E e) { // 如果queue满，返回false
    return putRef.get().offer(e);
  }

  @Override
  public boolean offer(E e, long timeout, TimeUnit unit)
      throws InterruptedException { // 如果queue满，先等待一会儿，如果queue还是满，返回false
    return putRef.get().offer(e, timeout, unit);
  }

  @Override
  public E peek() {
    return takeRef.get().peek();
  }

  @Override
  public E poll() {
    return takeRef.get().poll();
  }

  @Override
  public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    return takeRef.get().poll(timeout, unit);
  }

  /**
   * Retrieve an E from the backing queue or block until we can.
   * Guaranteed to return an element from the current queue.
   */
  @Override
  public E take() throws InterruptedException {
    E e = null;

    while (e == null) { // 确保获取一个非null元素
      e = takeRef.get().poll(1000L, TimeUnit.MILLISECONDS);
    }

    return e;
  }

  @Override
  public int size() { // call queue整体大小，不区分优先级
    return takeRef.get().size();
  }

  @Override
  public int remainingCapacity() { // call queue剩余空间，不区分优先级
    return takeRef.get().remainingCapacity();
  }

  /**
   * Read the number of levels from the configuration.
   * This will affect the FairCallQueue's overall capacity.
   * @throws IllegalArgumentException on invalid queue count
   */
  @SuppressWarnings("deprecation")
  private static int parseNumLevels(String ns, Configuration conf) { // 一共分几个优先级
    // Fair call queue levels (IPC_CALLQUEUE_PRIORITY_LEVELS_KEY)
    // takes priority over the scheduler level key
    // (IPC_SCHEDULER_PRIORITY_LEVELS_KEY)
    int retval = conf.getInt(ns + "." +
        FairCallQueue.IPC_CALLQUEUE_PRIORITY_LEVELS_KEY, 0);
    if (retval == 0) { // No FCQ priority level configured
      retval = conf.getInt(ns + "." +
          CommonConfigurationKeys.IPC_SCHEDULER_PRIORITY_LEVELS_KEY,
          CommonConfigurationKeys.IPC_SCHEDULER_PRIORITY_LEVELS_DEFAULT_KEY);
    } else {
      LOG.warn(ns + "." + FairCallQueue.IPC_CALLQUEUE_PRIORITY_LEVELS_KEY +
          " is deprecated. Please use " + ns + "." +
          CommonConfigurationKeys.IPC_SCHEDULER_PRIORITY_LEVELS_KEY + ".");
    }
    if(retval < 1) {
      throw new IllegalArgumentException("numLevels must be at least 1");
    }
    return retval;
  }

  /**
   * Replaces active queue with the newly requested one and transfers
   * all calls to the newQ before returning.
   */
  public synchronized void swapQueue( // 动态刷新call queue
      Class<? extends RpcScheduler> schedulerClass,
      Class<? extends BlockingQueue<E>> queueClassToUse, int maxSize,
      String ns, Configuration conf) {
    int priorityLevels = parseNumLevels(ns, conf); // 一共几个优先级
    this.scheduler.stop();
    RpcScheduler newScheduler = createScheduler(schedulerClass, priorityLevels,
        ns, conf);
    BlockingQueue<E> newQ = createCallQueueInstance(queueClassToUse,
        priorityLevels, maxSize, ns, conf);

    // Our current queue becomes the old queue
    BlockingQueue<E> oldQ = putRef.get();

    // Swap putRef first: allow blocked puts() to be unblocked
    putRef.set(newQ);

    // Wait for handlers to drain the oldQ
    while (!queueIsReallyEmpty(oldQ)) {} // 等待old queue中没有元素

    // Swap takeRef to handle new calls
    takeRef.set(newQ);

    this.scheduler = newScheduler;

    LOG.info("Old Queue: " + stringRepr(oldQ) + ", " +
      "Replacement: " + stringRepr(newQ));
  }

  /**
   * Checks if queue is empty by checking at CHECKPOINT_NUM points with
   * CHECKPOINT_INTERVAL_MS interval.
   * This doesn't mean the queue might not fill up at some point later, but
   * it should decrease the probability that we lose a call this way.
   */
  private boolean queueIsReallyEmpty(BlockingQueue<?> q) {
    for (int i = 0; i < CHECKPOINT_NUM; i++) { // 检查20此，确保queue为空
      try {
        Thread.sleep(CHECKPOINT_INTERVAL_MS); // sleep 10ms
      } catch (InterruptedException ie) {
        return false;
      }
      if (!q.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private String stringRepr(Object o) { // 打印log用
    return o.getClass().getName() + '@' + Integer.toHexString(o.hashCode());
  }

  @Override
  public int drainTo(Collection<? super E> c) {
    return takeRef.get().drainTo(c);
  }

  @Override
  public int drainTo(Collection<? super E> c, int maxElements) {
    return takeRef.get().drainTo(c, maxElements);
  }

  @Override
  public Iterator<E> iterator() {
    return takeRef.get().iterator();
  }

  // exception that mimics the standard ISE thrown by blocking queues but
  // embeds a rpc server exception for the client to retry and indicate
  // if the client should be disconnected.
  @SuppressWarnings("serial")
  static class CallQueueOverflowException extends IllegalStateException {
    private static String TOO_BUSY = "Server too busy";
    static final CallQueueOverflowException KEEPALIVE =
        new CallQueueOverflowException(
            new RetriableException(TOO_BUSY),
            RpcStatusProto.ERROR);
    static final CallQueueOverflowException DISCONNECT =
        new CallQueueOverflowException(
            new RetriableException(TOO_BUSY + " - disconnecting"),
            RpcStatusProto.FATAL);
    static final CallQueueOverflowException FAILOVER =
        new CallQueueOverflowException(
            new StandbyException(TOO_BUSY + " - disconnect and failover"),
            RpcStatusProto.FATAL);
    CallQueueOverflowException(final IOException ioe,
        final RpcStatusProto status) {
      super("Queue full", new RpcServerException(ioe.getMessage(), ioe){
        @Override
        public RpcStatusProto getRpcStatusProto() {
          return status;
        }
      });
    }
    @Override
    public IOException getCause() {
      return (IOException)super.getCause();
    }
  }
}
