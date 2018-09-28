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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectory;
import alluxio.master.file.meta.InodeFile;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.options.DeleteOptions;
import alluxio.util.io.FileUtils;
import alluxio.Configuration;
import alluxio.PropertyKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.Random;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Lost files periodic check.
 */
@NotThreadSafe
final class AsyncInodeFileEvictor implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncInodeFileEvictor.class);
  private final FileSystemMaster mFileSystemMaster;
  private final InodeTree mInodeTree;

  private final long CHECKPOINT_INTERVAL = Configuration.getMs(PropertyKey.MASTER_INODE_CHECKPOINT_INTERVAL_MS);
  private final long SWEEP_BASE = Configuration.getLong(PropertyKey.MASTER_INODE_EVICT_BASE);
  private final int STEP_READ_CHECKPOINT = 0;
  private final int STEP_MARK = 1;
  private final int STEP_SWEEP = 2;
  private final int SLOT = 20;               /* each sweep pass check inode within a slot */

  private int mSweepIdx = 0;                  /* index into SLOT */
  private int mStep = STEP_READ_CHECKPOINT;   /* 0: readCheckpoint 1: mark 2: sweep */
  private long mMin = 0;                      /* min access timestamp for all nodes */
  private long mMax = 0;                      /* max access timestamp for all nodes */
  private final long mInodeCapacity;          /* start sweep when inode number exceeds this */
  private final long mInodeCapacityLow;       /* stop sweep when inode number decrease to this */
  private long mSweepCnt = SWEEP_BASE;        /* how many to sweep in one pass */

  /**
   * Constructs a new {@link LostFileDetector}.
   */
  public AsyncInodeFileEvictor(FileSystemMaster fileSystemMaster, InodeTree inodeTree,
      long capacity, long capacityLow) {
    mFileSystemMaster = fileSystemMaster;
    mInodeTree = inodeTree;
    mInodeCapacity = capacity;
    mInodeCapacityLow = capacityLow;
    LOG.info("=== evict started, wait for 5 min to work");
  }

  public long getMin() {
    return (mMin == 0) ? System.currentTimeMillis() : mMin;
  }

  public long getMiddle() {
    return (mMin + mMax) / 2;
  }

  private void mark() {
    if (mInodeTree.getSize() <= mInodeCapacity) return;

    Iterator<Inode<?>> it = mInodeTree.iterator();
    mMin = Long.MAX_VALUE;
    mMax = Long.MIN_VALUE;
    while (it.hasNext()) {
      Inode<?> node = it.next();
      if (node instanceof InodeFile) {
        mMax = Long.max(mMax, node.getLastAccessTs());
        mMin = Long.min(mMin, node.getLastAccessTs());
      }
    }
    if (mMax < mMin) mMax = mMin;
    mSweepIdx = 0;
    mStep = STEP_SWEEP;
    LOG.info("=== mark min {} max {} size {}", mMin, mMax, mInodeTree.getSize());
  }

  private void sweep() {
    long size = mInodeTree.getSize();
    if (size <= mInodeCapacityLow) {
      mStep = STEP_MARK;
      return;
    }

    long delTo = mMin + (mMax - mMin) / SLOT * (mSweepIdx + 1), cnt = 0;

    Iterator<Inode<?>> it = mInodeTree.iterator();
    while (it.hasNext() && cnt < mSweepCnt && mInodeTree.getSize() > mInodeCapacityLow) {
      Inode<?> node = it.next();
      try {
        if (node instanceof InodeFile && node.getLastAccessTs() < delTo) {
          AlluxioURI uri = mFileSystemMaster.getPath(node.getId());
          if (node.isPersisted()) {
            mFileSystemMaster.delete(uri, DeleteOptions.defaults().setAlluxioOnly(true));
            LOG.debug("=== evict async delete {}:{}", node.getId(), uri.getPath());
            cnt++;
          } else {
            mFileSystemMaster.scheduleAsyncPersistence(uri);
            LOG.debug("=== evict async schedule {}:{}", node.getId(), uri.getPath());
          }
        } else if (node instanceof InodeDirectory && node.getLastAccessTs() < delTo
            && ((InodeDirectory)node).getChildren().size() == 0
            && !((InodeDirectory)node).isMountPoint()) {
          AlluxioURI uri = mFileSystemMaster.getPath(node.getId());
          mFileSystemMaster.delete(uri, DeleteOptions.defaults().setAlluxioOnly(true));
          LOG.debug("=== evict async delete {}:{}", node.getId(), uri.getPath());
          cnt++;
        }
      } catch (Exception e) {
        LOG.error(" === Failed to async inode evict file {} , because {}", node.getId(), e);
      }
    } // while

    if (!it.hasNext()) mSweepIdx++;
    if (mSweepIdx >= SLOT) {
      mStep = STEP_MARK;  // again
    }

    LOG.info("=== evict sweep {} [{}]:{} -> {}", cnt, mSweepIdx, size, mInodeTree.getSize());

    if (cnt > 0 && size < mInodeTree.getSize()) {
      mSweepCnt = mSweepCnt / 100 * 120;
      if (mSweepCnt > 2 * SWEEP_BASE) mSweepCnt = 2 * SWEEP_BASE;
    } else {
      mSweepCnt = mSweepCnt * 100 / 120;
      if (mSweepCnt < SWEEP_BASE) mSweepCnt = SWEEP_BASE;
    }

  }

  private long mLastCheckPointTs = System.currentTimeMillis();         /* used to control next checkpoint time */
  private boolean mayCheckPoint() throws IOException {
    if (mMax <= mMin || System.currentTimeMillis() - mLastCheckPointTs < CHECKPOINT_INTERVAL) return false;
    mLastCheckPointTs = System.currentTimeMillis();

    Iterator<Inode<?>> it = mInodeTree.iterator();
    int idx, slot100 = 100;
    long slice = (mMax - mMin) / slot100 + 1, low;
    long slots[] = new long[slot100];
    for (idx = 0; idx < slot100; idx++) {
      slots[idx] = 0;
    }
    while (it.hasNext()) {
      Inode<?> node = it.next();
      if (node instanceof InodeFile) {
        idx = (int)((node.getLastAccessTs() - mMin) * 100 / (mMax - mMin));
        if (idx < 0) {
          idx = 0;
        } else if (idx >= slot100) {
          idx = slot100 - 1;
        }
        slots[(int)idx]++;
      }
    }

    low = mInodeTree.getSize() / 100 * 70;    // save up to 30% into checkpoint
    for (idx = slot100 - 1; idx >= 0; --idx) {
      low -= slots[idx];
      if (low <= 0) break;
    }
    low = mMin + (mMax - mMin) / slot100 * idx;

    String dir = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    if (!dir.endsWith(AlluxioURI.SEPARATOR)) dir += AlluxioURI.SEPARATOR;
    String src = dir + "heat." + System.currentTimeMillis();

    it = mInodeTree.iterator();
    try (ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(src))) {
      long written = 0; 
      while (it.hasNext()) {
        Inode<?> node = it.next();
        if (node instanceof InodeFile && node.getLastAccessTs() >= low) {
          os.writeLong(node.getId());
          os.writeLong(node.getLastAccessTs());
          written++;
        }
      }
      LOG.info("=== do checkpoint {} out of {}", written, mInodeTree.getSize());
    }

    FileUtils.move(src, dir + "heat");
    return true;
  }

  private void readCheckpoint() throws IOException {
    String dir = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    if (!dir.endsWith(AlluxioURI.SEPARATOR)) dir += AlluxioURI.SEPARATOR;

    long loaded = 0;
    try {
      try (ObjectInputStream is = new ObjectInputStream(new FileInputStream(dir + "heat"))) {
        while (true) {
          long id = is.readLong();
          long ts = is.readLong();
          Inode<?> node = mInodeTree.getInode(id);
          if (node != null) {
            node.setLastAccessTs(ts);
            loaded++;
          }
        }
      }
    } catch (java.io.EOFException e) {
      LOG.debug("=== done loading checkpoint");
    } catch (Exception e) {
      LOG.error("=== fail to load checkpoinnnt {}", e.getMessage());
    }
    LOG.info("=== read checkpoint {} out of {}", loaded, mInodeTree.getSize());
    mStep = STEP_MARK;
  }

  private final long mStartedWait = System.currentTimeMillis();
  private long mLastPrintTs = System.currentTimeMillis();
  @Override
  public void heartbeat() {
    long wait = Configuration.getBoolean(PropertyKey.TEST_MODE) ? 30 * 1000 : 300 * 1000;
    if (System.currentTimeMillis() - mStartedWait < wait) return;

    wait = Configuration.getBoolean(PropertyKey.TEST_MODE) ? 60 * 1000 : 600 * 1000;
    if (System.currentTimeMillis() - mLastPrintTs >= wait) {
      LOG.info("=== evict size {}, sweep idx {}", mInodeTree.getSize(), mSweepIdx);
      mLastPrintTs = System.currentTimeMillis();
    }

    try {
      if (mayCheckPoint()) return;
      switch (mStep) {
        case STEP_READ_CHECKPOINT:
          readCheckpoint();
          break;
        case STEP_MARK:
          if (mInodeTree.getSize() >= mInodeCapacity) {
            mark();
          }
          break;
        case STEP_SWEEP:
          sweep();
          break;
        default:
          break;
      }
    } catch (IOException e) {
      LOG.error("=== error in step {}:{}", mStep, e.getMessage());
    }
  }

  @Override
  public void close() {
    // Nothing to clean up
  }
}
