/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.policy;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.network.TieredIdentityFactory;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A policy that returns local host first, and if the local worker doesn't have enough availability,
 * it randomly picks a worker from the active workers list for each block write.
 */
// TODO(peis): Move the BlockLocationPolicy implementation to alluxio.client.block.policy.
@ThreadSafe
public final class LocalFirstPolicy implements FileWriteWithRoleLocationPolicy, BlockLocationPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(LocalFirstPolicy.class);
  private final TieredIdentity mTieredIdentity;

  private static List<String> writerHosts = null;
  {
      String hosts = System.getenv("QINIU_WRITER_HOSTS");
      if (hosts != null) writerHosts = Arrays.asList(hosts.split("\\s*,\\s*"));
      if (writerHosts == null) writerHosts = new ArrayList<String>();
  }

  /**
   * Constructs a {@link LocalFirstPolicy}.
   */
  public LocalFirstPolicy() {
    this(TieredIdentityFactory.localIdentity());
  }

  /**
   * @param localTieredIdentity the local tiered identity
   */
  private LocalFirstPolicy(TieredIdentity localTieredIdentity) {
    mTieredIdentity = localTieredIdentity;
  }

  @VisibleForTesting
  static LocalFirstPolicy create(TieredIdentity localTieredIdentity) {
    return new LocalFirstPolicy(localTieredIdentity);
  }

  static boolean workerFilter(BlockWorkerInfo w, WorkerNetAddress.WorkerRole role) {
    WorkerNetAddress.WorkerRole workerRole = w.getNetAddress().getRole();
    if (role != null) {
      if (role.equals(WorkerNetAddress.WorkerRole.ALL) || role.equals(workerRole)) {
        return true;
      }
    }

    return writerHosts.contains(w.getNetAddress().getHost() + ":" + w.getNetAddress().getDataPort());
  }

  @Override
  @Nullable
  public WorkerNetAddress getWorkerForNextBlock(GetWorkerOptions options) {
    Iterable<BlockWorkerInfo> workerInfoList = options.getBlockWorkerInfos();
    long blockSizeBytes = options.getBlockSize();
    List<BlockWorkerInfo> shuffledWorkers = Lists.newArrayList(workerInfoList);
    List<BlockWorkerInfo> candidateWorkers = shuffledWorkers.stream()
      .filter(w -> LocalFirstPolicy.workerFilter(w, options.getRole()))
      .collect(Collectors.toList());
    WorkerNetAddress worker = getWorkerForNextBlockImpl(candidateWorkers, blockSizeBytes);
    if (worker != null) return worker;
    return getWorkerForNextBlockImpl(workerInfoList, blockSizeBytes);
  }

  /**
   * If configured, write goes to specified workers unless there is none satisified.  - qiniu
   */
  @Override
  @Nullable
  public WorkerNetAddress getWorkerForNextBlock(Iterable<BlockWorkerInfo> workerInfoList, long blockSizeBytes, WorkerNetAddress.WorkerRole role) {
    List<BlockWorkerInfo> shuffledWorkers = Lists.newArrayList(workerInfoList);
    LOG.info("shuffledWorkers in getWorkerForNextBlock: {}", shuffledWorkers);
    List<BlockWorkerInfo> candidateWorkers = shuffledWorkers.stream()
      .filter(w -> LocalFirstPolicy.workerFilter(w, role))
      .collect(Collectors.toList());
    LOG.info("candidateWorkers in getWorkerForNextBlock: {}", candidateWorkers);
    WorkerNetAddress worker = getWorkerForNextBlockImpl(candidateWorkers, blockSizeBytes);
    LOG.info("worker in getWorkerForNextBlock: {}", worker);
    if (worker != null) return worker;
    return getWorkerForNextBlockImpl(workerInfoList, blockSizeBytes);
  }

  private WorkerNetAddress getWorkerForNextBlockImpl(Iterable<BlockWorkerInfo> workerInfoList,
      long blockSizeBytes) {
    List<BlockWorkerInfo> shuffledWorkers = Lists.newArrayList(workerInfoList);
    Collections.shuffle(shuffledWorkers);
    // Workers must have enough capacity to hold the block.
    List<BlockWorkerInfo> candidateWorkers = shuffledWorkers.stream()
        .filter(worker -> worker.getCapacityBytes() >= blockSizeBytes)
        .collect(Collectors.toList());

    // Try finding a worker based on nearest tiered identity.
    List<TieredIdentity> identities = candidateWorkers.stream()
        .map(worker -> worker.getNetAddress().getTieredIdentity())
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
    Optional<TieredIdentity> nearest = mTieredIdentity.nearest(identities);
    if (!nearest.isPresent()) {
      return null;
    }
    // Map back to the worker with the nearest tiered identity.
    return candidateWorkers.stream()
        .filter(worker -> worker.getNetAddress().getTieredIdentity().equals(nearest.get()))
        .map(worker -> worker.getNetAddress())
        .findFirst().orElse(null);
  }

  @Override
  public WorkerNetAddress getWorker(GetWorkerOptions options) {
    return getWorkerForNextBlock(options);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LocalFirstPolicy)) {
      return false;
    }
    LocalFirstPolicy that = (LocalFirstPolicy) o;
    return Objects.equals(mTieredIdentity, that.mTieredIdentity);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mTieredIdentity);
  }

  @Override
  public String toString() {
    return com.google.common.base.Objects.toStringHelper(this)
        .add("tieredIdentity", mTieredIdentity)
        .toString();
  }
}
