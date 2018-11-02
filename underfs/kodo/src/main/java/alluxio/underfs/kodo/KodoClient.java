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

import com.qiniu.common.QiniuException;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.UploadManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.storage.model.ResumeBlockInfo;
import com.qiniu.util.Auth;
import com.qiniu.util.StringMap;
import com.qiniu.util.StringUtils;
import com.qiniu.util.UrlSafeBase64;
import com.qiniu.http.Client;
import com.qiniu.util.Crc32;
import com.qiniu.common.Constants;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.MediaType;
import okio.BufferedSink;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Client or Kodo under file system.
 */
public class KodoClient {

  /** Qiniu authentication for Qiniu SDK. */
  private Auth mAuth;

  /** bucket manager for Qiniu SDK. */
  private BucketManager mBucketManager;

  /** upload manager for Qiniu SDK. */
  private UploadManager mUploadManager;

  /** Http client for get Object. */
  private OkHttpClient mOkHttpClient;

  /** Bucket name of the Alluxio Kodo bucket. */
  private String mBucketName;

  /** Qiniu Kodo download Host. */
  private String mDownloadHost;

  /** Endpoint for Qiniu kodo. */
  private String mEndPoint;

  private Configuration mConfiguration;

  private static final String DefaultMime = "application/octet-stream";

  /**
   * Creates a new instance of {@link KodoClient}.
   * @param auth Qiniu authentication
   * @param BucketName bucketname for kodo
   * @param DownloadHost download host for kodo
   * @param EndPoint endpoint for kodo
   * @param cfg configuration for Qiniu SDK
   * @param okHttpClient http client
   */
  public KodoClient(Auth auth, String BucketName, String DownloadHost, String EndPoint,
      Configuration cfg, OkHttpClient okHttpClient) {
    mAuth = auth;
    mBucketName = BucketName;
    mEndPoint = EndPoint;
    mDownloadHost = DownloadHost;
    mBucketManager = new BucketManager(mAuth, cfg);
    mUploadManager = new UploadManager(cfg);
    mOkHttpClient = okHttpClient;
    mConfiguration = cfg;
  }

  /**
   * get bucketname for kodoclient.
   * @return bucketname
   */
  public String getBucketName() {
    return mBucketName;
  }

  /**
   * get file from for Qiniu kodo.
   * @param key Object jey
   * @return Qiniu FileInfo
   * @throws QiniuException
   */
  public FileInfo getFileInfo(String key) throws QiniuException {
    return mBucketManager.stat(mBucketName, key);
  }

  /**
   * get object from Qiniu kodo. All requests are authenticated by default，default expires 3600s We
   * use okhttp as our HTTP client and support two main parameters in the external adjustment, MAX
   * request and timeout time.
   * @param key object key
   * @param startPos start index for object
   * @param endPos end index for object
   * @return inputstream
   * @param contentLength object file size
   * @throws IOException
   */
  public InputStream getObject(String key, long startPos, long endPos, long contentLength)
      throws IOException {
    String baseUrl = String.format("http://%s/%s", mDownloadHost, key);
    String privateUrl = mAuth.privateDownloadUrl(baseUrl);
    URL url = new URL(privateUrl);
    String objectUrl = String.format("http://%s/%s?%s", mEndPoint, key, url.getQuery());
    Request request = new Request.Builder().url(objectUrl)
        .addHeader("Range",
            "bytes=" + String.valueOf(startPos) + "-"
                + String.valueOf(endPos < contentLength ? endPos - 1 : contentLength - 1))
        .addHeader("Host", mDownloadHost).get().build();
    Response response = mOkHttpClient.newCall(request).execute();
    if (response.code() != 200 && response.code() != 206) {
      throw new IOException(String.format("Qiniu kodo:get object failed errcode:%d,errmsg:%s",
          response.code(), response.message()));
    }
    return response.body().byteStream();
  }

  /**
   * Put Object to Qiniu kodo.
   * @param Key Object key for kodo
   * @param File Alluxio File
   */
  public void uploadFile(String Key, File File) throws QiniuException {
    com.qiniu.http.Response response =
        mUploadManager.put(File, Key, mAuth.uploadToken(mBucketName, Key));
    response.close();
  }

  /**
   * copy object in Qiniu kodo.
   * @param src source Object key
   * @param dst destination Object Key
   */
  public void copyObject(String src, String dst) throws QiniuException {
    mBucketManager.copy(mBucketName, src, mBucketName, dst);
  }

  /**
   * create empty Object in Qiniu kodo.
   * @param key empty Object key
   */
  public void createEmptyObject(String key) throws QiniuException {
    com.qiniu.http.Response response =
        mUploadManager.put(new byte[0], key, mAuth.uploadToken(mBucketName, key));
    response.close();
  }

  /**
   * delete Object in Qiniu kodo.
   * @param key Object key
   */
  public void deleteObject(String key) throws QiniuException {
    com.qiniu.http.Response response = mBucketManager.delete(mBucketName, key);
    response.close();
  }

  /**
   * list object for Qiniu kodo.
   * @param prefix prefix for bucket
   * @param marker Marker returned the last time a file list was obtained
   * @param limit Length limit for each iteration, Max. 1000
   * @param delimiter Specifies a directory separator that lists all common prefixes (simulated
   *        listing directory effects). The default is an empty string
   * @return result for list
   */
  public FileListing listFiles(String prefix, String marker, int limit, String delimiter)
      throws QiniuException {
    return mBucketManager.listFiles(mBucketName, prefix, marker, limit, delimiter);
  }

  public HashMap<Long, ArrayList<String>> streamUploader(InputStream stream, String key) throws QiniuException {
    HashMap<Long, ArrayList<String>> uploadResult = new HashMap<>();
    String host = mConfiguration.upHost(mAuth.uploadToken(mBucketName, key));
    byte[] blockBuffer = new byte[Constants.BLOCK_SIZE];
    ArrayList<String> contexts = new ArrayList<>();
    int retryMax = mConfiguration.retryMax;
    long uploaded = 0;
    int ret = 0;
    long size = 0;
    boolean retry = false;
    boolean eof = false;
    while (size == 0 && !eof) {
      int bufferIndex = 0;
      int blockSize = 0;

      //try to read the full BLOCK or until the EOF
      while (ret != -1 && bufferIndex != blockBuffer.length) {
        try {
          blockSize = blockBuffer.length - bufferIndex;
          ret = stream.read(blockBuffer, bufferIndex, blockSize);
        } catch (IOException e) {
          close(stream);
          throw new QiniuException(e);
        }
        if (ret != -1) {
          //continue to read more
          //advance bufferIndex
          bufferIndex += ret;
          if (ret == 0) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        } else {
          eof = true;
          //file EOF here, trigger outer while-loop finish
          size = uploaded + bufferIndex;
        }
      }

      if (bufferIndex == 0) {
        break;
      }
      long crc = Crc32.bytes(blockBuffer, 0, bufferIndex);
      com.qiniu.http.Response response = null;
      QiniuException temp = null;
      try {
        response = makeBlock(blockBuffer, bufferIndex, key, host);
      } catch (QiniuException e) {
        if (e.code() < 0) {
          host = mConfiguration.upHostBackup(mAuth.uploadToken(mBucketName, key));
        }
        if (e.response == null || e.response.needRetry()) {
          retry = true;
          temp = e;
        } else {
          close(stream);
          throw e;
        }
      }

      if (!retry) {
        ResumeBlockInfo blockInfo0 = response.jsonToObject(ResumeBlockInfo.class);
        if (blockInfo0.crc32 != crc) {
          retry = true;
          temp = new QiniuException(new Exception("block's crc32 is not match"));
        }
      }
      if (retry) {
        if (retryMax > 0) {
          retryMax--;
          try {
              response = makeBlock(blockBuffer, bufferIndex, key, host);
              retry = false;
          } catch (QiniuException e) {
              close(stream);
              throw e;
          }
        } else {
            throw temp;
        }
      }
      ResumeBlockInfo blockInfo = response.jsonToObject(ResumeBlockInfo.class);
      contexts.add(blockInfo.ctx);
      uploaded += bufferIndex;

    }
    close(stream);
    uploadResult.put(size, contexts);
    return uploadResult;
  }

  private com.qiniu.http.Response makeBlock(byte[] block, int blockSize, String key, String host) throws QiniuException {
    String url = host + "/mkblk/" + blockSize;
    String token = mAuth.uploadToken(mBucketName, key);
    return post(url, block, 0, blockSize, token);
  }

  private void close(InputStream stream) {
    try {
        stream.close();
    } catch (Exception e) {
        e.printStackTrace();
    }
  }

  private String fileUrl(long size, String mime, String key, StringMap params, String host) {
    String url =  host + "/mkfile/" + size + "/mimeType/" + UrlSafeBase64.encodeToString(mime);
    final StringBuilder b = new StringBuilder(url);
    if (key != null) {
        b.append("/key/");
        b.append(UrlSafeBase64.encodeToString(key));
    }
    if (params != null) {
        params.forEach(new StringMap.Consumer() {
            @Override
            public void accept(String key, Object value) {
                b.append("/");
                b.append(key);
                b.append("/");
                b.append(UrlSafeBase64.encodeToString("" + value));
            }
        });
    }
    return b.toString();
  }

  private static RequestBody create(final MediaType contentType,
    final byte[] content, final int offset, final int size) {
    if (content == null) throw new NullPointerException("content == null");

    return new RequestBody() {
        @Override
        public MediaType contentType() {
            return contentType;
        }

        @Override
        public long contentLength() {
            return size;
        }

        @Override
        public void writeTo(BufferedSink sink) throws IOException {
            sink.write(content, offset, size);
        }
    };
  }

  private static String userAgent() {
    String javaVersion = "Java/" + System.getProperty("java.version");
    String os = System.getProperty("os.name") + " "
            + System.getProperty("os.arch") + " " + System.getProperty("os.version");
    String sdk = "QiniuJava/" + Constants.VERSION;
    return sdk + " (" + os + ") " + javaVersion;
  }

  public com.qiniu.http.Response makeFile(long size, String mime, String key, StringMap params, ArrayList<String> contexts) throws QiniuException {
    if (mime == null) {
      mime = DefaultMime;
    }
    String host = mConfiguration.upHost(mAuth.uploadToken(mBucketName, key));
    String url = fileUrl(size, mime, key, params, host);
    String s = StringUtils.join(contexts, ",");
    String token = mAuth.uploadToken(mBucketName, key);
    return post(url, StringUtils.utf8Bytes(s), token);
  }

  private com.qiniu.http.Response post(String url, byte[] data, String token) throws QiniuException {
      // return mClient.post(url, data, new StringMap().put("Authorization", "UpToken " + token));
      RequestBody rbody;
      if (data != null && data.length > 0) {
        MediaType t = MediaType.parse(DefaultMime);
        rbody = RequestBody.create(t, data);
      } else {
        rbody = RequestBody.create(null, new byte[0]);
      }
      Request.Builder requestBuilder = new Request.Builder().url(url).post(rbody);
      return send(requestBuilder, new StringMap().put("Authorization", "UpToken " + token));
  }

  private com.qiniu.http.Response post(String url, byte[] data, int offset, int size, String token) throws QiniuException {
    // return mClient.post(url, data, offset, size, new StringMap().put("Authorization", "UpToken " + token),
    //           Client.DefaultMime);
    RequestBody rbody;
    if (data != null && data.length > 0) {
      MediaType t = MediaType.parse(DefaultMime);
      rbody = create(t, data, offset, size);
    } else {
      rbody = RequestBody.create(null, new byte[0]);
    }
    Request.Builder requestBuilder = new Request.Builder().url(url).post(rbody);
    return send(requestBuilder, new StringMap().put("Authorization", "UpToken " + token));
  }

  private com.qiniu.http.Response send(final Request.Builder requestBuilder, StringMap headers) throws QiniuException {
    if (headers != null) {
      headers.forEach(new StringMap.Consumer() {
        @Override
        public void accept(String key, Object value) {
            requestBuilder.header(key, value.toString());
        }
      });
    }
    requestBuilder.header("User-Agent", userAgent());
    long start = System.currentTimeMillis();
    Response res = null;
    com.qiniu.http.Response r;
    double duration = (System.currentTimeMillis() - start) / 1000.0;
    IpTag tag = new IpTag();
    try {
      res = mOkHttpClient.newCall(requestBuilder.tag(tag).build()).execute();
    } catch (IOException e) {
      e.printStackTrace();
      throw new QiniuException(e);
    }
    r = com.qiniu.http.Response.create(res, tag.ip, duration);
    if (r.statusCode >= 300) {
        throw new QiniuException(r);
    }
    return r;
  }

  private static class IpTag {
    public String ip = null;
  }

}
