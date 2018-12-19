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

package alluxio.client.file;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.Constants;
import alluxio.AlluxioURI;
import alluxio.MetaCache;
import alluxio.annotation.PublicApi;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.ExistsOptions;
import alluxio.client.file.options.FreeOptions;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.LoadMetadataOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.file.options.RenameOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.client.file.options.UnmountOptions;
import alluxio.client.block.BlockMasterClient;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.AlreadyExistsException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.wire.BlockInfo;
import alluxio.wire.CommonOptions;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.LoadMetadataType;
import alluxio.wire.MountPointInfo;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.proto.ProtoMessage;
import alluxio.network.netty.NettyRPC;
import alluxio.network.netty.NettyRPCContext;
import alluxio.network.TieredIdentityFactory;
import alluxio.wire.BlockLocation;
import alluxio.resource.CloseableResource;

import java.util.concurrent.ExecutorService;
import java.util.Optional;
import io.netty.channel.Channel;
import com.google.common.base.Preconditions;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.Iterator;

import javax.annotation.concurrent.ThreadSafe;

/**
* Default implementation of the {@link FileSystem} interface. Developers can extend this class
* instead of implementing the interface. This implementation reads and writes data through
* {@link FileInStream} and {@link FileOutStream}. This class is thread safe.
*/
@PublicApi
@ThreadSafe
public class BaseFileSystem implements FileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(BaseFileSystem.class);

  protected final FileSystemContext mFileSystemContext;
  TieredIdentity mLocalTier = TieredIdentityFactory.localIdentity();
  private static final long READ_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);

  @Override
  public void startAsyncCache(String file) {
    try {
      URIStatus status = getStatus(new AlluxioURI(file));
      List<Long> ids = status.getBlockIds();
      List<WorkerNetAddress> all = mFileSystemContext.getWorkerAddresses(false, true);
      List<WorkerNetAddress> all_remote = all.stream().filter(w -> w.getWebPort() < 5000).collect(toList());
      List<WorkerNetAddress> all_local = all.stream().filter(w -> w.getWebPort() >= 5000).collect(toList());
      Iterator<Long> it = ids.iterator();
      while (it.hasNext()) {
        WorkerNetAddress tgt = null, src = null;
        long id = it.next();
        BlockInfo binfo = getBlockInfo(id);
        List<BlockLocation> locations = binfo.getLocations();
        List<WorkerNetAddress> workers = locations.stream().map(BlockLocation::getWorkerAddress).collect(toList());
        Collections.shuffle(workers);
        Collections.shuffle(all);
        Collections.shuffle(all_remote);
        Collections.shuffle(all_local);
        if (mFileSystemContext.hasLocalWorker() && 1 == ids.size()) {
          if (workers.isEmpty()) {                                      // 1: no any cache, fetch from UFS to local worker
            src = tgt = mFileSystemContext.getLocalWorker();
          } else if (!workers.contains(mFileSystemContext.getLocalWorker())) {
            tgt = mFileSystemContext.getLocalWorker();                  // 2: remote worker --> local worker
            src = workers.get(0);
          }
        } else {
          if (workers.isEmpty()) {                                      // 3: no any cache, remote worker or any local worker
            src = tgt = all_remote.isEmpty() ? all_local.get(0) : all_remote.get(0);
          } else {
            List<WorkerNetAddress> workers_remote = workers.stream().filter(w -> w.getWebPort() < 5000).collect(toList());
            if (workers_remote.isEmpty() && !all_remote.isEmpty()) {    // 4: all local worker --> one remote worker
              tgt = all_remote.get(0);
              src = workers.get(0);
            }
          }
        }
        if (tgt != null && src != null) {   // Construct the async cache request
          Protocol.AsyncCacheRequest request =
            Protocol.AsyncCacheRequest.newBuilder().setBlockId(id).setLength(binfo.getLength())
            .setOpenUfsBlockOptions(OpenFileOptions.defaults().toInStreamOptions(status).getOpenUfsBlockOptions(id))
            .setSourceHost(src.getHost()).setSourcePort(src.getDataPort()).build();
          LOG.info("!!! sc send asyn cache request for id {} to {} from {}", id, tgt, src);
          MetaCache.invalidate(file);       // try to update block locations
          Channel channel = mFileSystemContext.acquireNettyChannel(tgt);
          try {
            NettyRPCContext rpcContext =
              NettyRPCContext.defaults().setChannel(channel).setTimeout(READ_TIMEOUT_MS);
            NettyRPC.fireAndForget(rpcContext, new ProtoMessage(request));
          } finally {
            mFileSystemContext.releaseNettyChannel(tgt, channel);
          }
        } // if
      } // while
    } catch (Exception e) { 
      LOG.error("!!! asyn cache error {}", e.getMessage());
    }
  }

  @Override
  public FileSystem.ShortCircuitInfo acquireShortCircuitInfo(String file) {
    Channel channel = null;
    FileSystem.ShortCircuitInfo info = FileSystem.ShortCircuitInfo.dummy();

    try {
      URIStatus status = getStatus(new AlluxioURI(file));
      if (status.getLength() > status.getBlockSizeBytes()) {
        LOG.info("!!! multiple blocks {}", file);
        return info;
      }
      List<Long> ids = status.getBlockIds();
      if (ids.size() != 1) {
        LOG.info("!!! need single ids but {}", ids);
        return info;
      }
      info.id(ids.get(0));
      BlockInfo binfo = getBlockInfo(info.id());
      List<BlockLocation> locations = binfo.getLocations();
      List<TieredIdentity> tieredLocations = locations.stream().map(location -> 
          location.getWorkerAddress().getTieredIdentity()).collect(toList());
      Optional<TieredIdentity> nearest = mLocalTier.nearest(tieredLocations);
      if (nearest.isPresent() && mLocalTier.getTier(0).getTierName().equals(Constants.LOCALITY_NODE)
            && mLocalTier.topTiersMatch(nearest.get())) {
        info.worker(locations.stream().map(BlockLocation::getWorkerAddress)
          .filter(addr -> addr.getTieredIdentity().equals(nearest.get())).findFirst().get());
        channel = mFileSystemContext.acquireNettyChannel(info.worker());
        LOG.debug("!!! worker {} channel {}", info.worker(), channel);
        // wired, AsyncCacheRequest with LocalBlockOpenResponse -- qiniu
        Protocol.AsyncCacheRequest request =
          Protocol.AsyncCacheRequest.newBuilder().setBlockId(info.id()).setLength(-1).build();
        ProtoMessage message = NettyRPC
          .call(NettyRPCContext.defaults().setChannel(channel).setTimeout(READ_TIMEOUT_MS),
              new ProtoMessage(request));
        Preconditions.checkState(message.isLocalBlockOpenResponse());
        info.file(message.asLocalBlockOpenResponse().getPath());
        LOG.debug("!!! local path {} for {}", info.file(), file);
      }
    } catch (Exception e) { 
      LOG.error("!!! channel {}: {}", channel,  e.getMessage());
    } finally {
      if (info.worker() != null && channel != null) {
        mFileSystemContext.releaseNettyChannel(info.worker(), channel);
      }
    }

    return info;
  }

  public BlockInfo getBlockInfo(long blockId) throws IOException {
    BlockInfo info = MetaCache.getBlockInfoCache(blockId);
    if (info != null && info.getLocations() != null
        && info.getLocations().size() > 0) return info;
    try (CloseableResource<BlockMasterClient> masterClientResource =
        mFileSystemContext.acquireBlockMasterClientResource()) {
      info = masterClientResource.get().getBlockInfo(blockId);
      MetaCache.addBlockInfoCache(blockId, info);
      return info;
    }
  }

  /**
   * @param context file system context
   * @return a {@link BaseFileSystem}
   */
  public static BaseFileSystem get(FileSystemContext context) {
    return new BaseFileSystem(context);
  }

  /**
   * Constructs a new base file system.
   *
   * @param context file system context
   */
  protected BaseFileSystem(FileSystemContext context) {
    mFileSystemContext = context;
  }

  @Override
  public void createDirectory(AlluxioURI path)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    createDirectory(path, CreateDirectoryOptions.defaults());
  }

  @Override
  public void createDirectory(AlluxioURI path, CreateDirectoryOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.createDirectory(path, options);
      LOG.debug("Created directory {}, options: {}", path.getPath(), options);
    } catch (AlreadyExistsException e) {
      throw new FileAlreadyExistsException(e.getMessage());
    } catch (InvalidArgumentException e) {
      throw new InvalidPathException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public FileOutStream createFile(AlluxioURI path)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    return createFile(path, CreateFileOptions.defaults());
  }

  @Override
  public FileOutStream createFile(AlluxioURI path, CreateFileOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    URIStatus status;
    try {
      masterClient.createFile(path, options);
      // Do not sync before this getStatus, since the UFS file is expected to not exist.
      //status = masterClient.getStatus(path,
      status = getStatus(path,
          GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
              .setCommonOptions(CommonOptions.defaults().setSyncIntervalMs(-1)));
      LOG.debug("Created file {}, options: {}", path.getPath(), options);
    } catch (AlreadyExistsException e) {
      throw new FileAlreadyExistsException(e.getMessage());
    } catch (InvalidArgumentException e) {
      throw new InvalidPathException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
    OutStreamOptions outStreamOptions = options.toOutStreamOptions();
    outStreamOptions.setUfsPath(status.getUfsPath());
    outStreamOptions.setMountId(status.getMountId());
    try {
      return new FileOutStream(path, outStreamOptions, mFileSystemContext);
    } catch (Exception e) {
      delete(path);
      throw e;
    }
  }

  @Override
  public void delete(AlluxioURI path)
      throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException {
    delete(path, DeleteOptions.defaults());
  }

  @Override
  public void delete(AlluxioURI path, DeleteOptions options)
      throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.delete(path, options);
      LOG.debug("Deleted {}, options: {}", path.getPath(), options);
    } catch (FailedPreconditionException e) {
      // A little sketchy, but this should be the only case that throws FailedPrecondition.
      throw new DirectoryNotEmptyException(e.getMessage());
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public boolean exists(AlluxioURI path)
      throws InvalidPathException, IOException, AlluxioException {
    return exists(path, ExistsOptions.defaults());
  }

  @Override
  public boolean exists(AlluxioURI path, ExistsOptions options)
      throws InvalidPathException, IOException, AlluxioException {
    URIStatus s = MetaCache.getStatus(path.getPath());
    if (s != null && s.getLength() > 0) return true;  // qiniu

    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      // TODO(calvin): Make this more efficient
      s = masterClient.getStatus(path, options.toGetStatusOptions());
      MetaCache.setStatus(path.getPath(), s);
      return true;
    } catch (NotFoundException e) {
      return false;
    } catch (InvalidArgumentException e) {
      // The server will throw this when a prefix of the path is a file.
      // TODO(andrew): Change the server so that a prefix being a file means the path does not exist
      return false;
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void free(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    free(path, FreeOptions.defaults());
  }

  @Override
  public void free(AlluxioURI path, FreeOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.free(path, options);
      LOG.debug("Freed {}, options: {}", path.getPath(), options);
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public URIStatus getStatus(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    return getStatus(path, GetStatusOptions.defaults());
  }

  @Override
  public URIStatus getStatus(AlluxioURI path, GetStatusOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    if (options.isInvalidateCache()) { 
        MetaCache.invalidate(path.getPath()); // qiniu
    } else {
        URIStatus s = MetaCache.getStatus(path.getPath());
        if (s != null) return s;
    }

    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      URIStatus s = masterClient.getStatus(path, options);  // qiniu
      MetaCache.setStatus(path.getPath(), s);
      return s;
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    return listStatus(path, ListStatusOptions.defaults());
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path, ListStatusOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    // TODO(calvin): Fix the exception handling in the master
    try {
      return masterClient.listStatus(path, options);
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @deprecated since version 1.1 and will be removed in version 2.0
   */
  @Deprecated
  @Override
  public void loadMetadata(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    loadMetadata(path, LoadMetadataOptions.defaults());
  }

  /**
   * {@inheritDoc}
   *
   * @deprecated since version 1.1 and will be removed in version 2.0
   */
  @Deprecated
  @Override
  public void loadMetadata(AlluxioURI path, LoadMetadataOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.loadMetadata(path, options);
      LOG.debug("Loaded metadata {}, options: {}", path.getPath(), options);
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath)
      throws IOException, AlluxioException {
    mount(alluxioPath, ufsPath, MountOptions.defaults());
  }

  @Override
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options)
      throws IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      // TODO(calvin): Make this fail on the master side
      masterClient.mount(alluxioPath, ufsPath, options);
      LOG.info("Mount " + ufsPath.toString() + " to " + alluxioPath.getPath());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public Map<String, MountPointInfo> getMountTable() throws IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      return masterClient.getMountTable();
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public FileInStream openFile(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    return openFile(path, OpenFileOptions.defaults());
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFileOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    URIStatus status = getStatus(path);
    if (status.isFolder()) {
      throw new FileDoesNotExistException(
          ExceptionMessage.CANNOT_READ_DIRECTORY.getMessage(status.getName()));
    }
    InStreamOptions inStreamOptions = options.toInStreamOptions(status);
    return new FileInStream(status, inStreamOptions, mFileSystemContext);
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst)
      throws FileDoesNotExistException, IOException, AlluxioException {
    rename(src, dst, RenameOptions.defaults());
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst, RenameOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      // TODO(calvin): Update this code on the master side.
      masterClient.rename(src, dst, options);
      LOG.debug("Renamed {} to {}, options: {}", src.getPath(), dst.getPath(), options);
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void setAttribute(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    setAttribute(path, SetAttributeOptions.defaults());
  }

  @Override
  public void setAttribute(AlluxioURI path, SetAttributeOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.setAttribute(path, options);
      LOG.debug("Set attributes for {}, options: {}", path.getPath(), options);
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void unmount(AlluxioURI path) throws IOException, AlluxioException {
    unmount(path, UnmountOptions.defaults());
  }

  @Override
  public void unmount(AlluxioURI path, UnmountOptions options)
      throws IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.unmount(path);
      LOG.debug("Unmounted {}, options: {}", path.getPath(), options);
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }
}
