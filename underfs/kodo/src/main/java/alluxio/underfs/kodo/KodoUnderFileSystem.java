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

package alluxio.underfs.kodo;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import com.qiniu.common.QiniuException;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.Auth;

import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

/**
 * Qiniu Kodo {@link UnderFileSystem} implementation.
 */
public class KodoUnderFileSystem extends ObjectUnderFileSystem {

  private static final Logger LOG = LoggerFactory.getLogger(KodoUnderFileSystem.class);

  /**
   * Suffix for an empty file to flag it as a directory.
   */
  private static final String FOLDER_SUFFIX = "/";

  private final KodoClient mKodoClinet;

  protected KodoUnderFileSystem(AlluxioURI uri, KodoClient kodoclient,
      UnderFileSystemConfiguration conf) {
    super(uri, conf);
    mKodoClinet = kodoclient;
  }

  protected static KodoUnderFileSystem creatInstance(AlluxioURI uri,
      UnderFileSystemConfiguration conf) {
    String bucketName = UnderFileSystemUtils.getBucketName(uri);
    Preconditions.checkArgument(conf.containsKey(PropertyKey.KODO_ACCESS_KEY),
        "Property %s is required to connect to Koko", PropertyKey.KODO_ACCESS_KEY);
    Preconditions.checkArgument(conf.containsKey(PropertyKey.KODO_SECRET_KEY),
        "Property %s is required to connect to kodo", PropertyKey.KODO_SECRET_KEY);
    Preconditions.checkArgument(conf.containsKey(PropertyKey.KODO_DOWNLOAD_HOST),
        "Property %s is required to connect to kodo", PropertyKey.KODO_DOWNLOAD_HOST);
    Preconditions.checkArgument(conf.containsKey(PropertyKey.KODO_ENDPOINT),
        "Property %s is required to connect to kodo", PropertyKey.KODO_ENDPOINT);
    String accessKey = conf.getValue(PropertyKey.KODO_ACCESS_KEY);
    String secretKey = conf.getValue(PropertyKey.KODO_SECRET_KEY);
    String endPoint = conf.getValue(PropertyKey.KODO_ENDPOINT);
    String souceHost = conf.getValue(PropertyKey.KODO_DOWNLOAD_HOST);
    Auth auth = Auth.create(accessKey, secretKey);
    Configuration configuration = new Configuration();
    OkHttpClient.Builder okHttpBuilder = initializeKodoClientConfig(conf);
    KodoClient kodoClient =
        new KodoClient(auth, bucketName, souceHost, endPoint, configuration, okHttpBuilder);
    return new KodoUnderFileSystem(uri, kodoClient, conf);
  }

  private static Builder initializeKodoClientConfig(UnderFileSystemConfiguration conf) {
    OkHttpClient.Builder builder = new OkHttpClient.Builder();
    Dispatcher dispatcher = new Dispatcher();
    dispatcher.setMaxRequests( Integer.valueOf(conf.getValue(PropertyKey.UNDERFS_KODO_REQUESTS_MAX)));
    builder.connectTimeout( Long.valueOf(conf.getValue(PropertyKey.UNDERFS_KODO_CONNECT_TIMEOUT)),TimeUnit.SECONDS);
    return builder;
  }

  @Override
  public String getUnderFSType() {
    return "kodo";
  }

  // No ACL integration currently, no-op
  @Override
  public void setOwner(String path, String user, String group) {}

  // No ACL integration currently, no-op
  @Override
  public void setMode(String path, short mode) throws IOException {}

  @Override
  protected boolean copyObject(String src, String dst) {
    return mKodoClinet.copyObject(src, dst);
  }

  @Override
  protected boolean createEmptyObject(String key) {
    return mKodoClinet.createEmptyObject(key);
  }

  @Override
  protected OutputStream createObject(String key) throws IOException {
    return new KodoOutputStream(key, mKodoClinet);
  }

  @Override
  protected boolean deleteObject(String key) {
    return mKodoClinet.deleteObject(key);
  }

  @Override
  protected String getFolderSuffix() {
    return FOLDER_SUFFIX;
  }

  @Nullable
  @Override
  protected ObjectListingChunk getObjectListingChunk(String key, boolean recursive)
      throws IOException {
    String delimiter = recursive ? "" : PATH_SEPARATOR;
    key = PathUtils.normalizePath(key, PATH_SEPARATOR);
    key = key.equals(PATH_SEPARATOR) ? "" : key;
    FileListing result = getObjectListingChunk(key, getListingChunkLength(), delimiter);
    if (result != null) {
      return new KodoObjectListingChunk(result, getListingChunkLength(), delimiter, key);
    }
    return null;
  }

  private FileListing getObjectListingChunk(String prefix, int limit, String delimiter) {
    return mKodoClinet.listFiles(prefix, null, limit, delimiter);
  }

  /**
   * Get metadata information about object. Implementations should process the key as is, which may
   * be a file or a directory key.
   *
   * @param key ufs key to get metadata for
   * @return {@link ObjectStatus} if key exists and successful, otherwise null
   */
  @Nullable
  @Override
  protected ObjectStatus getObjectStatus(String key) throws IOException {
    try {
      FileInfo fileInfo = mKodoClinet.getFileInfo(key);
      if (fileInfo == null) {
        return null;
      }
      return new ObjectStatus(key, fileInfo.hash, fileInfo.fsize, fileInfo.putTime / 10000);
    } catch (QiniuException e) {
      LOG.warn("Failed to get Object {}, Msg:{}", key, e);

      e.printStackTrace();
    }
    return null;
  }

  // No ACL integration currently, returns default empty value
  @Override
  protected ObjectPermissions getPermissions() {
    return new ObjectPermissions("", "", Constants.DEFAULT_FILE_SYSTEM_MODE);
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options) throws IOException {
    try {
      return new KodoInputStream(key, mKodoClinet, options.getOffset());
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Get full path of root in object store.
   *
   * @return full path including scheme and bucket
   */
  @Override
  protected String getRootKey() {
    return Constants.HEADER_KODO + mKodoClinet.getBucketName();
  }

  private final class KodoObjectListingChunk implements ObjectListingChunk {
    final int mLimit;
    private final String mDelimiter;
    private final String mPrefix;
    private FileListing mResult;

    KodoObjectListingChunk(FileListing result, int limit, String delimiter, String prefix)
        throws IOException {
      mLimit = limit;
      mDelimiter = delimiter;
      mResult = result;
      mPrefix = prefix;
    }

    /**
     * @return
     */
    @Override
    public ObjectStatus[] getObjectStatuses() {

      FileInfo[] fileInfos = mResult.items;
      ObjectStatus[] ret = new ObjectStatus[fileInfos.length];
      int i = 0;
      for (FileInfo fileInfo : fileInfos) {
        if (fileInfo.key != null) {
          ret[i++] = new ObjectStatus(fileInfo.key, fileInfo.hash, fileInfo.fsize,
              fileInfo.putTime / 10000);
        }
      }
      return ret;
    }

    /**
     * Use common prefixes to infer pseudo-directories in object store.
     *
     * @return a list of common prefixes
     */
    @Override
    public String[] getCommonPrefixes() {
      if (mResult.commonPrefixes == null) {
        return new String[] {};
      }
      return mResult.commonPrefixes;
    }

    /**
     * Gets next chunk of object listings.
     *
     * @return null if listing did not find anything or is done, otherwise return new
     *         {@link ObjectListingChunk} for the next chunk
     */
    @Nullable
    @Override
    public ObjectListingChunk getNextChunk() throws IOException {
      if (!mResult.isEOF()) {
        FileListing nextResult = mKodoClinet.listFiles(mPrefix, mResult.marker, mLimit, mDelimiter);
        return new KodoObjectListingChunk(nextResult, mLimit, mDelimiter, mPrefix);
      }
      return null;
    }
  }
}
