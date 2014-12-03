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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectorySnapshottableFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotManager;
import org.apache.hadoop.hdfs.util.ChunkedArrayList;

import java.io.IOException;
import java.util.List;

class FSDirSnapshotOp {
  /** Allow snapshot on a directory. */
  static void allowSnapshot(FSDirectory fsd, SnapshotManager snapshotManager,
                            String path) throws IOException {
    fsd.writeLock();
    try {
      snapshotManager.setSnapshottable(path, true);
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logAllowSnapshot(path);
  }

  static void disallowSnapshot(
      FSDirectory fsd, SnapshotManager snapshotManager,
      String path) throws IOException {
    fsd.writeLock();
    try {
      snapshotManager.resetSnapshottable(path);
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logDisallowSnapshot(path);
  }

  /**
   * Create a snapshot
   * @param snapshotRoot The directory path where the snapshot is taken
   * @param snapshotName The name of the snapshot
   */
  static String createSnapshot(
      FSDirectory fsd, SnapshotManager snapshotManager, String snapshotRoot,
      String snapshotName, boolean logRetryCache)
      throws IOException {
    final FSPermissionChecker pc = fsd.getPermissionChecker();

    String snapshotPath = null;
    if (fsd.isPermissionEnabled()) {
      fsd.checkOwner(pc, snapshotRoot);
    }

    if (snapshotName == null || snapshotName.isEmpty()) {
      snapshotName = Snapshot.generateDefaultSnapshotName();
    }

    if(snapshotName != null){
      if (!DFSUtil.isValidNameForComponent(snapshotName)) {
        throw new InvalidPathException("Invalid snapshot name: " +
            snapshotName);
      }
    }
    fsd.verifySnapshotName(snapshotName, snapshotRoot);
    fsd.writeLock();
    try {
      snapshotPath = snapshotManager.createSnapshot(snapshotRoot, snapshotName);
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logCreateSnapshot(snapshotRoot, snapshotName,
        logRetryCache);

    return snapshotPath;
  }

  static void renameSnapshot(FSDirectory fsd, SnapshotManager snapshotManager,
      String path, String snapshotOldName, String snapshotNewName,
      boolean logRetryCache) throws IOException {

    if (fsd.isPermissionEnabled()) {
      FSPermissionChecker pc = fsd.getPermissionChecker();
        fsd.checkOwner(pc, path);
    }
    fsd.verifySnapshotName(snapshotNewName, path);
    fsd.writeLock();
    try {
      snapshotManager.renameSnapshot(path, snapshotOldName, snapshotNewName);
    } finally {
      fsd.writeUnlock();
    }
    fsd.getEditLog().logRenameSnapshot(path, snapshotOldName,
        snapshotNewName, logRetryCache);
  }

  static SnapshottableDirectoryStatus[] getSnapshottableDirListing(
      FSDirectory fsd, SnapshotManager snapshotManager) throws IOException {
    FSPermissionChecker pc = fsd.getPermissionChecker();
    fsd.readLock();
    try {
      final String user = pc.isSuperUser()? null : pc.getUser();
      return snapshotManager.getSnapshottableDirListing(user);
    } finally {
      fsd.readUnlock();
    }
  }

  static SnapshotDiffReport getSnapshotDiffReport(
      FSDirectory fsd, SnapshotManager snapshotManager, String path,
      String fromSnapshot, String toSnapshot) throws IOException {
    SnapshotDiffReport diffs;
    final FSPermissionChecker pc = fsd.getPermissionChecker();
    fsd.readLock();
    try {
      if (fsd.isPermissionEnabled()) {
        checkSubtreeReadPermission(fsd, pc, path, fromSnapshot);
        checkSubtreeReadPermission(fsd, pc, path, toSnapshot);
      }
      diffs = snapshotManager.diff(path, fromSnapshot, toSnapshot);
    } finally {
      fsd.readUnlock();
    }
    return diffs;
  }

  /**
   * Delete a snapshot of a snapshottable directory
   * @param snapshotRoot The snapshottable directory
   * @param snapshotName The name of the to-be-deleted snapshot
   * @throws IOException
   */
  static INode.BlocksMapUpdateInfo deleteSnapshot(
      FSDirectory fsd, SnapshotManager snapshotManager, String snapshotRoot,
      String snapshotName, boolean logRetryCache)
      throws IOException {
    final FSPermissionChecker pc = fsd.getPermissionChecker();

    INode.BlocksMapUpdateInfo collectedBlocks = new INode.BlocksMapUpdateInfo();
    if (fsd.isPermissionEnabled()) {
      fsd.checkOwner(pc, snapshotRoot);
    }

    ChunkedArrayList<INode> removedINodes = new ChunkedArrayList<INode>();
    fsd.writeLock();
    try {
      snapshotManager.deleteSnapshot(snapshotRoot, snapshotName,
          collectedBlocks, removedINodes);
      fsd.removeFromInodeMap(removedINodes);
    } finally {
      fsd.writeUnlock();
    }
    removedINodes.clear();
    fsd.getEditLog().logDeleteSnapshot(snapshotRoot, snapshotName,
        logRetryCache);

    return collectedBlocks;
  }

  private static void checkSubtreeReadPermission(
      FSDirectory fsd, final FSPermissionChecker pc, String snapshottablePath,
      String snapshot) throws IOException {
    final String fromPath = snapshot == null ?
        snapshottablePath : Snapshot.getSnapshotPath(snapshottablePath,
        snapshot);
    fsd.checkPermission(pc, fromPath, false, null, null, FsAction.READ,
        FsAction.READ);
  }

  /**
   * Check if the given INode (or one of its descendants) is snapshottable and
   * already has snapshots.
   *
   * @param target The given INode
   * @param snapshottableDirs The list of directories that are snapshottable
   *                          but do not have snapshots yet
   */
  static void checkSnapshot(
      INode target, List<INodeDirectory> snapshottableDirs)
      throws SnapshotException {
    if (target.isDirectory()) {
      INodeDirectory targetDir = target.asDirectory();
      DirectorySnapshottableFeature sf = targetDir
          .getDirectorySnapshottableFeature();
      if (sf != null) {
        if (sf.getNumSnapshots() > 0) {
          String fullPath = targetDir.getFullPathName();
          throw new SnapshotException("The directory " + fullPath
              + " cannot be deleted since " + fullPath
              + " is snapshottable and already has snapshots");
        } else {
          if (snapshottableDirs != null) {
            snapshottableDirs.add(targetDir);
          }
        }
      }
      for (INode child : targetDir.getChildrenList(Snapshot.CURRENT_STATE_ID)) {
        checkSnapshot(child, snapshottableDirs);
      }
    }
  }
}
