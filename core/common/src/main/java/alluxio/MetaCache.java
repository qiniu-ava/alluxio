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

package alluxio;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.file.URIStatus;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.BlockLocation;
import alluxio.collections.ConcurrentHashSet;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the file meta cache
 */
@ThreadSafe
public class MetaCache {
  private static final Logger LOG = LoggerFactory.getLogger(MetaCache.class);

  private static Path alluxioRootPath = null;
  private static int maxCachedPaths = Configuration.getInt(PropertyKey.FUSE_CACHED_PATHS_MAX);
  private static LoadingCache<String, MetaCacheData> fcache 
    = CacheBuilder.newBuilder().maximumSize(maxCachedPaths).build(new MetaCacheLoader());
  private static LoadingCache<Long, BlockInfoData> bcache 
    = CacheBuilder.newBuilder().maximumSize(maxCachedPaths).build(new BlockInfoLoader());
  private static List<WorkerInfo> workerList = new ArrayList<>();
  private static AtomicLong lastWorkerListAccess = new AtomicLong(System.currentTimeMillis());
  private static boolean attrCacheEnabled = true;
  private static boolean blockCacheEnabled = true;
  private static boolean workerCacheEnabled = true;

  public static final int LOCAL_WORKER_PORT_MIN = 50000;
  public static final String WORKER_LOCAL_PATH = Configuration.get(PropertyKey.WORKER_LOCAL_PATH);

  public static final int ACTION_ASYNC_CACHE = 0;
  public static final int ACTION_X_CACHE = -1;
  public static final int ACTION_X_CACHE_REMOTE_EXISTED = -2;

  //qiniu worker -> array of files
  final static public int EVICT_EVICT = 0;
  final static public int EVICT_FREE = 1;
  static private Map <Long, Set<Long> > mEvictEvict = new ConcurrentHashMap<>();
  static private Map <Long, Set<Long> >mEvictFree = new ConcurrentHashMap<>();

  // need to synchronized
  static public Set<Long> getEvictBlock(int type, long worker) {
      Map <Long, Set<Long> > m = (EVICT_EVICT == type) ? mEvictEvict : mEvictFree;
      Set<Long> s = m.get(worker);
      if (s == null) {
          s = new ConcurrentHashSet<Long>();
          m.put(worker, s);
      }
      return s;
  }

  // add a place-hold PersistFile first, add blocks later
  static public boolean addEvictBlock(int type, long worker, long block) {
      Set<Long> s = getEvictBlock(type, worker);
      s.add(block);
      return true;
  }

  public static void setAlluxioRootPath(Path path) {
    alluxioRootPath = path;
  }

  public static void debugMetaCache(String p) {
    if (p.startsWith("a0")) {
      MetaCache.setAttrCache(0);
      System.out.println("Alluxio attr cache disabled.");
    } else if (p.startsWith("a1")) {
      MetaCache.setAttrCache(1);
      System.out.println("Alluxio attr cache enabled.");
    } else if (p.startsWith("ap")) {
      MetaCache.setAttrCache(2);
      System.out.println("Alluxio attr cache purged.");
    } else if (p.startsWith("as")) {
      System.out.println("Alluxio attr cache state:" + attrCacheEnabled);
      System.out.println("Alluxio attr cache size:" + fcache.size());
    } else if (p.startsWith("ac")) {
      p = p.substring(2);
      System.out.println("Attr cache for " + p + ":" + MetaCache.getStatus(MetaCache.resolve(p)));
    } else if (p.startsWith("b0")) {
      MetaCache.setBlockCache(0);
      System.out.println("Alluxio block cache disabled.");
    } else if (p.startsWith("b1")) {
      MetaCache.setBlockCache(1);
      System.out.println("Alluxio block cache enabled.");
    } else if (p.startsWith("bp")) {
      MetaCache.setBlockCache(2);
      System.out.println("Alluxio block cache purged.");
    } else if (p.startsWith("bs")) {
      System.out.println("Alluxio block cache state:" + blockCacheEnabled);
      System.out.println("Alluxio block cache size:" + bcache.size());
    } else if (p.startsWith("bc")) {
      p = p.substring(2);
      Long l = Long.parseLong(p);
      System.out.println("Cached block for " + l + ":" + MetaCache.getBlockInfoCache(l));
    } else if (p.startsWith("w0")) {
      MetaCache.setWorkerCache(0);
      System.out.println("Alluxio worker cache disabled.");
    } else if (p.startsWith("w1")) {
      MetaCache.setWorkerCache(1);
      System.out.println("Alluxio worker cache enabled.");
    } else if (p.startsWith("wp")) {
      MetaCache.setWorkerCache(2);
      System.out.println("Alluxio worker cache purged.");
    } else if (p.startsWith("ws")) {
      System.out.println("Cached workers state:" + workerCacheEnabled);
      System.out.println("Cached workers:" + MetaCache.getWorkerInfoList());
    }
  }

  public static void setAttrCache(int v) {
    switch (v) {
      case 0: 
        attrCacheEnabled = false; 
        fcache.invalidateAll();
        return;
      case 1: 
        attrCacheEnabled = true; 
        return;
      default: 
        fcache.invalidateAll(); 
    }
  }

  public static void setBlockCache(int v) {
    switch (v) {
      case 0: 
        blockCacheEnabled = false;
        bcache.invalidateAll();
        return;
      case 1:
        blockCacheEnabled = true;
        return;
      default:
        bcache.invalidateAll();
    }
  }

  public static void setWorkerCache(int v) {
    workerCacheEnabled = (0 == v) ? false : true;
    if (v > 1) MetaCache.invalidateWorkerInfoList();
  }

  public static void setWorkerInfoList(List<WorkerInfo> list) {
    if (!workerCacheEnabled) return;

    if (list != null) {
      synchronized (workerList) {
        workerList.clear();
        workerList.addAll(list);
      }
    }
  }

  public static List<WorkerInfo> getWorkerInfoList() {
    long now = System.currentTimeMillis(); // expire every 1min
    if (now - lastWorkerListAccess.get() > 1000 * 60) {
      synchronized (workerList) {
        workerList.clear();
      }
    }
    lastWorkerListAccess.set(now);
    synchronized (workerList) {
      return new ArrayList<WorkerInfo>(workerList);
    }
  }

  public static void invalidateWorkerInfoList() {
    synchronized (workerList) {
      workerList.clear();
    }
  }

  public static String resolve(String path) {
    if (alluxioRootPath == null) return path;
    if (path.indexOf(alluxioRootPath.toString()) == 0) return path;
    Path tpath = alluxioRootPath.resolve(path.substring(1));
    return tpath.toString();
  }

  public static URIStatus getStatus(String path) {
    path = resolve(path);
    MetaCacheData c = fcache.getIfPresent(path);
    return (c == null || c.getExpectMore() > System.currentTimeMillis()) ? null : c.getStatus();
  }

  public static void setStatusExpectMore(String path) {
    path = resolve(path);
    MetaCacheData c = fcache.getIfPresent(path);
    if (c != null) {
      c.setExpectMore(System.currentTimeMillis() + 5 * 60 * 1000);
    }
  }

  private static boolean statusContain(URIStatus a, URIStatus b) {
    Set<BlockLocation> seta = new HashSet<>(), setb = new HashSet<>();
    for (FileBlockInfo f: a.getFileBlockInfos()) {
      seta.addAll(f.getBlockInfo().getLocations());
    }
    for (FileBlockInfo f: b.getFileBlockInfos()) {
      setb.addAll(f.getBlockInfo().getLocations());
    }
    return seta.containsAll(setb);
  }

  public static void setStatus(String path, URIStatus s) {
    /* if (!attrCacheEnabled || s.isFolder() || s.getBlockSizeBytes() == 0
        || s.getLength() == 0 || s.getInAlluxioPercentage() != 100) return; */

    path = resolve(path);
    MetaCacheData c = fcache.getIfPresent(path);
    long more = (c != null && c.getExpectMore() > System.currentTimeMillis() 
        && !statusContain(s, c.getStatus())) ? c.getExpectMore() : 0;
    c = (c == null) ? fcache.getUnchecked(path) : c;
    c.setStatus(s);
    if (more > 0) {
      c.setExpectMore(more);
    }
    if (s.getLength() > 0) {
      for (FileBlockInfo f: s.getFileBlockInfos()) {
        BlockInfo b = f.getBlockInfo();
        addBlockInfoCache(b.getBlockId(), b);
      }
    }
  }

  private static int testLocalPath = -1;
  public static boolean localBlockExisted(long id) throws Exception {
    if (testLocalPath < 0) {
      testLocalPath = Files.isDirectory(Paths.get(MetaCache.WORKER_LOCAL_PATH)) ? 1 : 0;
    } 
    return (testLocalPath == 0) ? false : Files.exists(Paths.get(MetaCache.WORKER_LOCAL_PATH + "/" + id));
  }

  public static String localBlockPath(long id) {
    return MetaCache.WORKER_LOCAL_PATH + "/" + id;
  }

  public static AlluxioURI getURI(String path) {
    path = resolve(path);
    MetaCacheData c = fcache.getUnchecked(path); //confirm to original code logic
    return (c != null) ? c.getURI() : null;
  }

  public static void invalidate(String path) {
    //also invalidate block belonging to the file
    path = resolve(path);
    MetaCacheData data = fcache.getIfPresent(path);
    if (data != null) {
      URIStatus stat = data.getStatus();
      if (stat != null) {
        for (long blockId: stat.getBlockIds()) {
          bcache.invalidate(blockId);
        }
      }
    }
    fcache.invalidate(path);
  }

  public static void invalidatePrefix(String prefix) {
    prefix = resolve(prefix);
    Set<String> keys = fcache.asMap().keySet();
    for (String k: keys) {
      if (k.startsWith(prefix)) invalidate(k);
    }
  }

  public static void invalidateAll() {
    fcache.invalidateAll();
  }

  public static void addBlockInfoCache(long blockId, BlockInfo info) {
    if (!blockCacheEnabled) return;

    BlockInfoData data = bcache.getUnchecked(blockId);
    if (data != null) data.setBlockInfo(info);
  }

  public static BlockInfo getBlockInfoCache(long blockId) {
    BlockInfoData b = bcache.getIfPresent(blockId);
    return (b != null) ? b.getBlockInfo() : null;
  }

  public static void invalidateBlockInfoCache(long blockId) {
    bcache.invalidate(blockId);
  }

  public static void invalidateAllBlockInfoCache() {
    bcache.invalidateAll();
  }

  static class MetaCacheData {
    private URIStatus uriStatus;
    private AlluxioURI uri;
    private long mExpectMore;

    public void setExpectMore(long more) {
      mExpectMore = more;
    }
    public long getExpectMore() {
      return mExpectMore;
    }

    public MetaCacheData(String path) {
      /*
         if (alluxioRootPath != null) {
         Path tpath = alluxioRootPath.resolve(path.substring(1));
         this.uri = new AlluxioURI(tpath.toString());
         } else {
         this.uri = new AlluxioURI(path);
         }*/
      this.uri = new AlluxioURI(path);
      this.uriStatus = null;
      this.mExpectMore = 0;
    }

    public URIStatus getStatus() {
      return this.uriStatus;
    }

    public void setStatus(URIStatus s) {
      this.uriStatus = s;
    }

    public AlluxioURI getURI() {
      return this.uri;
    }
  }

  static class BlockInfoData {
    Long id;
    private BlockInfo info;

    BlockInfoData(Long id) {
      this.id = id;
      this.info = null;
    }

    public void setBlockInfo(BlockInfo info) {
      this.info = info;
    }
    public BlockInfo getBlockInfo() {
      return this.info;
    }
  }

  private static class MetaCacheLoader extends CacheLoader<String, MetaCacheData> {

    /**
     * Constructs a new {@link MetaCacheLoader}.
     */
    public MetaCacheLoader() {}

    @Override
    public MetaCacheData load(String path) {
      return new MetaCacheData(path);
    }
  }

  private static class BlockInfoLoader extends CacheLoader<Long, BlockInfoData> {

    /**
     * Constructs a new {@link BlockInfoLoader}.
     */
    public BlockInfoLoader() {}

    @Override
    public BlockInfoData load(Long blockid) {
      return new BlockInfoData(blockid);
    }
  }

}

