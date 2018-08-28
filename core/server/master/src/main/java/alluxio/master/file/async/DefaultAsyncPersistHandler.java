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

package alluxio.master.file.async;

import alluxio.AlluxioURI;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.thrift.PersistFile;
import alluxio.util.IdUtils;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * The default handler for async persistence that schedules the persistence on the workers that
 * contains all the blocks of a given file, and the handler returns the scheduled request whenever
 * the corresponding worker polls.
 */
public final class DefaultAsyncPersistHandler implements AsyncPersistHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultAsyncPersistHandler.class);

  private final FileSystemMasterView mFileSystemMasterView;

  /** Map from worker to the files to persist on that worker. Used by async persistence service. */
  private final Map<Long, Set<Long>> mWorkerToAsyncPersistFiles;

  /**
   * Constructs a new instance of {@link AsyncPersistHandler}.
   *
   * @param view a view of {@link FileSystemMaster}
   */
  public DefaultAsyncPersistHandler(FileSystemMasterView view) {
    mWorkerToAsyncPersistFiles = new HashMap<>();
    mFileSystemMasterView = Preconditions.checkNotNull(view, "view");
  }

  @Override
  public synchronized void scheduleAsyncPersistence(AlluxioURI path)
      throws AlluxioException, UnavailableException {
    // find the worker
    long workerId = getWorkerStoringFile(path);

    if (workerId == IdUtils.INVALID_WORKER_ID) {
      LOG.error("No worker found to schedule async persistence for file " + path);
      return;
    }

    if (!mWorkerToAsyncPersistFiles.containsKey(workerId)) {
      mWorkerToAsyncPersistFiles.put(workerId, new HashSet<Long>());
    }
    mWorkerToAsyncPersistFiles.get(workerId).add(mFileSystemMasterView.getFileId(path));
  }

  /**
   * Gets a worker where the given file is stored.
   *
   * @param path the path to the file
   * @return the id of the storing worker
   * @throws FileDoesNotExistException when the file does not exist on any worker
   * @throws AccessControlException if permission checking fails
   */
  // TODO(calvin): Propagate the exceptions in certain cases
  private long getWorkerStoringFile(AlluxioURI path)
      throws FileDoesNotExistException, AccessControlException, UnavailableException {
    long fileId = mFileSystemMasterView.getFileId(path);
    //qiniu: initialize the return workerId
    long workerId=0l;
    try {
      if (mFileSystemMasterView.getFileInfo(fileId).getLength() == 0) {
        // if file is empty, return any worker
        List<WorkerInfo> workerInfoList = mFileSystemMasterView.getWorkerInfoList();
        if (workerInfoList.isEmpty()) {
          LOG.error("No worker is available");
          return IdUtils.INVALID_WORKER_ID;
        }
        // randomly pick a worker
        int index = new Random().nextInt(workerInfoList.size());
        return workerInfoList.get(index).getId();
      }
    } catch (UnavailableException e) {
      return IdUtils.INVALID_WORKER_ID;
    }

    Map<Long, Integer> workerBlockCounts = new HashMap<>();
    //qiniu: workerAddress:blocks
    Map<String,Integer> workerAddressCounts=new HashMap<>();
     //qiniu: workerId:workerAddress
    Map<Long,String> workerIdAddress=new HashMap<>();

    List<FileBlockInfo> blockInfoList;
    try {
      blockInfoList = mFileSystemMasterView.getFileBlockInfoList(path);

      for (FileBlockInfo fileBlockInfo : blockInfoList) {
        for (BlockLocation blockLocation : fileBlockInfo.getBlockInfo().getLocations()) {
          if (workerBlockCounts.containsKey(blockLocation.getWorkerId())) {
            workerBlockCounts.put(blockLocation.getWorkerId(),
                workerBlockCounts.get(blockLocation.getWorkerId()) + 1);
          } else {
            workerBlockCounts.put(blockLocation.getWorkerId(), 1);
          }
          //qiniu:get the the blocks of every WorkerAddress
          if (workerAddressCounts.containsKey(blockLocation.getWorkerAddress().toString())) {
              workerAddressCounts.put(blockLocation.getWorkerAddress().toString(),
              workerAddressCounts.get(blockLocation.getWorkerAddress().toString()) + 1);
          } else {
              workerAddressCounts.put(blockLocation.getWorkerAddress().toString(), 1);
          }

          // all the blocks of a file must be stored on the same worker
          if (workerBlockCounts.get(blockLocation.getWorkerId()) == blockInfoList.size()) {
            return blockLocation.getWorkerId();
          }
          //qiniu:get the map workerId:workerAddress
          workerIdAddress.put(blockLocation.getWorkerId(),blockLocation.getWorkerAddress().toString());        
        }
      }
    } catch (FileDoesNotExistException e) {
      LOG.error("The file {} to persist does not exist", path);
      return IdUtils.INVALID_WORKER_ID;
    } catch (InvalidPathException e) {
      LOG.error("The file {} to persist is invalid", path);
      return IdUtils.INVALID_WORKER_ID;
    } catch (UnavailableException e) {
      return IdUtils.INVALID_WORKER_ID;
    }

    if (workerBlockCounts.size() == 0) {
      LOG.error("The file " + path + " does not exist on any worker");
      return IdUtils.INVALID_WORKER_ID;
    }

    // qiniu PMW get worker with most blocks
    Map<String,Integer> worker = new LinkedHashMap<>();
    List<String> list=new ArrayList<String>();
    //qiniu :sort the map of  workerAddressCounts and put it into map worker
    workerAddressCounts.entrySet().stream().sorted(Map.Entry.<String,Integer>comparingByValue().reversed()).
    forEachOrdered(x -> worker.put(x.getKey(), x.getValue()));
    //qiniu: blockNum is the max value of blocks
    int blockNum=worker.entrySet().iterator().next().getValue(); 
    //qiniu: get the workerAddress of the max blocks
    Iterator iter=workerAddressCounts.entrySet().iterator();
    while(iter.hasNext()){
            Map.Entry item=(Map.Entry)iter.next();
            if((Integer)item.getValue()==blockNum){
              //qiniu:put the workerAddress into the list
              list.add((String)item.getKey());
            }
    }
    //qiniu:sort the workerAddress and find the max workerAddress
    Collections.sort(list);
    String maxAddress=list.get(list.size()-1);
    //qiniu:find the workerId  corresponding to the the max workerAddress
    Iterator iter2=workerIdAddress.entrySet().iterator();
    while(iter2.hasNext()){
            Map.Entry item=(Map.Entry)iter2.next();
            if((String)item.getValue()==maxAddress){
                   workerId=(long)item.getKey();
            }
    } 
   return workerId;

    //LOG.error("Not all the blocks of file {} stored on the same worker", path);
    //return IdUtils.INVALID_WORKER_ID;
  }

  /**
   * Polls the files to send to the given worker for persistence. It also removes files from the
   * worker entry in {@link #mWorkerToAsyncPersistFiles}.
   *
   * @param workerId the worker id
   * @return the list of files
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the path is invalid
   * @throws AccessControlException if permission checking fails
   */
  @Override
  public synchronized List<PersistFile> pollFilesToPersist(long workerId)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    List<PersistFile> filesToPersist = new ArrayList<>();
    List<Long> fileIdsToPersist = new ArrayList<>();
    List<Long> fileIdsRemoved = new ArrayList<>();

    if (!mWorkerToAsyncPersistFiles.containsKey(workerId)) {
      return filesToPersist;
    }

    Set<Long> scheduledFiles = mWorkerToAsyncPersistFiles.get(workerId);
    try {
      for (long fileId : scheduledFiles) {
          try { // qiniu
              FileInfo fileInfo = mFileSystemMasterView.getFileInfo(fileId);
              if (fileInfo.isCompleted()) {
                  fileIdsToPersist.add(fileId);
                  List<Long> blockIds = new ArrayList<>();
                  for (FileBlockInfo fileBlockInfo : mFileSystemMasterView
                          .getFileBlockInfoList(mFileSystemMasterView.getPath(fileId))) {
                      blockIds.add(fileBlockInfo.getBlockInfo().getBlockId());
                          }

                  filesToPersist.add(new PersistFile(fileId, blockIds));
              }
          } catch (FileDoesNotExistException e) {
              LOG.warn("==== fileId {} removed during asyn persist", fileId);
              fileIdsRemoved.add(fileId);
          }
      }
    } catch (UnavailableException e) {
      return filesToPersist;
    }

    mWorkerToAsyncPersistFiles.get(workerId).removeAll(fileIdsToPersist);
    mWorkerToAsyncPersistFiles.get(workerId).removeAll(fileIdsRemoved); //qiniu
    return filesToPersist;
  }
}
