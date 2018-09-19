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

package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.MetaCache;
import alluxio.PropertyKey;
import alluxio.exception.InvalidPathException;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.FileLocationOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Lazily cache the block locations only when needed.
 */
@ThreadSafe
public class LazyUfsBlockLocationCache implements UfsBlockLocationCache {
  private static final Logger LOG = LoggerFactory.getLogger(LazyUfsBlockLocationCache.class);

  /** Number of blocks to cache. */
  private static final int MAX_BLOCKS =
      Configuration.getInt(PropertyKey.MASTER_UFS_BLOCK_LOCATION_CACHE_CAPACITY);

  /** Cache of ufs block locations, key is block ID, value is block locations. */
  private Map<Long, List<String>> mCache;
  private MountTable mMountTable;

  /**
   * Creates a new instance of {@link UfsBlockLocationCache}.
   *
   * @param mountTable the mount table
   */
  public LazyUfsBlockLocationCache(MountTable mountTable) {
    mCache = new ConcurrentHashMap<Long, List<String>>();
    mMountTable = mountTable;
  }

  @Override
  public void invalidate(long blockId) {
    mCache.remove(blockId);
  }

  @Override
  public List<String> get(long blockId) {
    return mCache.get(blockId);
  }

  @Override
  public List<String> get(long blockId, AlluxioURI fileUri, long offset) {
    List<String> locations = mCache.get(blockId);
    if (locations != null) {
      return locations;
    }
    try {
      MountTable.Resolution resolution = mMountTable.resolve(fileUri);
      String ufsUri = resolution.getUri().toString();
      try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
        UnderFileSystem ufs = ufsResource.get();
        locations = ufs.getFileLocations(ufsUri, FileLocationOptions.defaults().setOffset(offset));
      }
      if (locations != null) {
        if (mCache.size() >= MAX_BLOCKS) MetaCache.reduceCacheSize(mCache, 10);
        mCache.put(blockId, locations);
        return locations;
      }
    } catch (InvalidPathException | IOException e) {
      LOG.warn("Failed to get locations for block {} in file {} with offset {}: {}",
          blockId, fileUri, offset, e);
    }
    return null;
  }
}
