/**
 * This class is meant for wrapping a MaprFileSystem instance and presenting
 * it as an instance of DistributedFileSystem.
 *
 * Please note that only the methods that are actually implemented from the FileSystem
 * abstraction are delegated. The rest of the methods that only belong to DistributedFileSystem
 * thrown an UnsupportedExceptiom.
 *
 * The purpose of doing this is to adapt to the MiniDFSCluster requirement of working
 * only with an instance of DistributedFileSystem.
 *
 */
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;

public class MapRDistributedFileSystem extends DistributedFileSystem {

    private FileSystem maprfs;
    private final static String MAPRFS_IMPL = "com.mapr.fs.MapRFileSystem";

    public MapRDistributedFileSystem() {
        Class <? extends FileSystem> clazz;
        try {
            clazz = getClass().getClassLoader().loadClass(MAPRFS_IMPL).asSubclass(FileSystem.class);
            maprfs = clazz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
        Overrides from FileSystem. These will actually be used
        by the test cases using MiniMapRFSCluster. All implementations
        here are simply delegating to MapRFS. Also only the public methods
        are overridden and not the private ones.
     **/
    @Override
    public InetSocketAddress[] getJobTrackerAddrs(Configuration conf) throws IOException {
        return maprfs.getJobTrackerAddrs(conf);
    }

    @Override
    public PathId createPathId() {
        return maprfs.createPathId();
    }

    @Override
    public FSDataInputStream openFid2(PathId pfid, String file, int readAheadBytesHint) throws IOException {
        return maprfs.openFid2(pfid, file, readAheadBytesHint);
    }

    @Override
    public FSDataOutputStream createFid(String pfid, String file) throws IOException {
        return maprfs.createFid(pfid, file);
    }

    @Override
    public boolean deleteFid(String pfid, String dir) throws IOException {
        return maprfs.deleteFid(pfid, dir);
    }

    @Override
    public String mkdirsFid(Path p) throws IOException {
        return maprfs.mkdirsFid(p);
    }

    @Override
    public String mkdirsFid(String pfid, String dir) throws IOException {
        return maprfs.mkdirsFid(pfid, dir);
    }

    @Override
    public void setOwnerFid(String fid, String user, String group) throws IOException {
        maprfs.setOwnerFid(fid, user, group);
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        maprfs.initialize(name, conf);
    }

    @Override
    public String getScheme() {
        return maprfs.getScheme();
    }

    @Override
    public URI getUri() {
        return maprfs.getUri();
    }

    @Override
    public String getCanonicalServiceName() {
        return maprfs.getCanonicalServiceName();
    }

    @Override
    public String getName() {
        return maprfs.getName();
    }

    @Override
    public Path makeQualified(Path path) {
        return maprfs.makeQualified(path);
    }

    @Override
    public Token<DelegationTokenIdentifier> getDelegationToken(String renewer) throws IOException {
        return (Token<DelegationTokenIdentifier>)maprfs.getDelegationToken(renewer);
    }

    @Override
    public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) throws IOException {
        return maprfs.addDelegationTokens(renewer, credentials);
    }

    @Override
    public FileSystem[] getChildFileSystems() {
        return maprfs.getChildFileSystems();
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
        return maprfs.getFileBlockLocations(file, start, len);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path p, long start, long len) throws IOException {
        return maprfs.getFileBlockLocations(p, start, len);
    }

    @Override
    public FsServerDefaults getServerDefaults() throws IOException {
        return maprfs.getServerDefaults();
    }

    @Override
    public FsServerDefaults getServerDefaults(Path p) throws IOException {
        return maprfs.getServerDefaults(p);
    }

    @Override
    public Path resolvePath(Path p) throws IOException {
        return maprfs.resolvePath(p);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return maprfs.open(f, bufferSize);
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        return maprfs.open(f);
    }

    @Override
    public FSDataOutputStream create(Path f) throws IOException {
        return maprfs.create(f);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
        return maprfs.create(f, overwrite);
    }

    @Override
    public FSDataOutputStream create(Path f, Progressable progress) throws IOException {
        return maprfs.create(f, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, short replication) throws IOException {
        return maprfs.create(f, replication);
    }

    @Override
    public FSDataOutputStream create(Path f, short replication, Progressable progress) throws IOException {
        return maprfs.create(f, replication, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize) throws IOException {
        return maprfs.create(f, overwrite, bufferSize);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress) throws IOException {
        return maprfs.create(f, overwrite, bufferSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize) throws IOException {
        return maprfs.create(f, overwrite, bufferSize, replication, blockSize);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        return maprfs.create(f, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        return maprfs.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        return maprfs.create(f, permission, flags, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt) throws IOException {
        return maprfs.create(f, permission, flags, bufferSize, replication, blockSize, progress, checksumOpt);
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        return maprfs.createNonRecursive(f, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        return maprfs.createNonRecursive(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        return maprfs.createNonRecursive(f, permission, flags, bufferSize, replication, blockSize, progress);
    }

    @Override
    public boolean createNewFile(Path f) throws IOException {
        return maprfs.createNewFile(f);
    }

    @Override
    public FSDataOutputStream append(Path f) throws IOException {
        return maprfs.append(f);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
        return maprfs.append(f, bufferSize);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        return maprfs.append(f, bufferSize, progress);
    }

    @Override
    public void concat(Path trg, Path[] psrcs) throws IOException {
        maprfs.concat(trg, psrcs);
    }

    @Override
    public short getReplication(Path src) throws IOException {
        return maprfs.getReplication(src);
    }

    @Override
    public boolean setReplication(Path src, short replication) throws IOException {
        return maprfs.setReplication(src, replication);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return maprfs.rename(src, dst);
    }

    @Override
    public boolean delete(Path f) throws IOException {
        return maprfs.delete(f);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        return maprfs.delete(f, recursive);
    }

    @Override
    public boolean deleteOnExit(Path f) throws IOException {
        return maprfs.deleteOnExit(f);
    }

    @Override
    public boolean cancelDeleteOnExit(Path f) {
        return maprfs.cancelDeleteOnExit(f);
    }

    @Override
    public boolean exists(Path f) throws IOException {
        return maprfs.exists(f);
    }

    @Override
    public boolean isDirectory(Path f) throws IOException {
        return maprfs.isDirectory(f);
    }

    @Override
    public boolean isFile(Path f) throws IOException {
        return maprfs.isFile(f);
    }

    @Override
    public long getLength(Path f) throws IOException {
        return maprfs.getLength(f);
    }

    @Override
    public ContentSummary getContentSummary(Path f) throws IOException {
        return maprfs.getContentSummary(f);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        return maprfs.listStatus(f);
    }

    @Override
    public RemoteIterator<Path> listCorruptFileBlocks(Path path) throws IOException {
        return maprfs.listCorruptFileBlocks(path);
    }

    @Override
    public FileStatus[] listStatus(Path f, PathFilter filter) throws FileNotFoundException, IOException {
        return maprfs.listStatus(f, filter);
    }

    @Override
    public FileStatus[] listStatus(Path[] files) throws FileNotFoundException, IOException {
        return maprfs.listStatus(files);
    }

    @Override
    public FileStatus[] listStatus(Path[] files, PathFilter filter) throws FileNotFoundException, IOException {
        return maprfs.listStatus(files, filter);
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern) throws IOException {
        return maprfs.globStatus(pathPattern);
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
        return maprfs.globStatus(pathPattern, filter);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws FileNotFoundException, IOException {
        return maprfs.listLocatedStatus(f);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws FileNotFoundException, IOException {
        return maprfs.listFiles(f, recursive);
    }

    @Override
    public Path getHomeDirectory() {
        return maprfs.getHomeDirectory();
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        maprfs.setWorkingDirectory(new_dir);
    }

    @Override
    public Path getWorkingDirectory() {
        return maprfs.getWorkingDirectory();
    }

    @Override
    public boolean mkdirs(Path f) throws IOException {
        return maprfs.mkdirs(f);
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        return maprfs.mkdirs(f, permission);
    }

    @Override
    public void copyFromLocalFile(Path src, Path dst) throws IOException {
        maprfs.copyFromLocalFile(src, dst);
    }

    @Override
    public void moveFromLocalFile(Path[] srcs, Path dst) throws IOException {
        maprfs.moveFromLocalFile(srcs, dst);
    }

    @Override
    public void moveFromLocalFile(Path src, Path dst) throws IOException {
        maprfs.moveFromLocalFile(src, dst);
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
        maprfs.copyFromLocalFile(delSrc, src, dst);
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) throws IOException {
        maprfs.copyFromLocalFile(delSrc, overwrite, srcs, dst);
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
        maprfs.copyFromLocalFile(delSrc, overwrite, src, dst);
    }

    @Override
    public void copyToLocalFile(Path src, Path dst) throws IOException {
        maprfs.copyToLocalFile(src, dst);
    }

    @Override
    public void moveToLocalFile(Path src, Path dst) throws IOException {
        maprfs.moveToLocalFile(src, dst);
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
        maprfs.copyToLocalFile(delSrc, src, dst);
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem) throws IOException {
        maprfs.copyToLocalFile(delSrc, src, dst, useRawLocalFileSystem);
    }

    @Override
    public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
        return maprfs.startLocalOutput(fsOutputFile, tmpLocalFile);
    }

    @Override
    public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
        maprfs.completeLocalOutput(fsOutputFile, tmpLocalFile);
    }

    @Override
    public void close() throws IOException {
        maprfs.close();
    }

    @Override
    public long getUsed() throws IOException {
        return maprfs.getUsed();
    }

    @Override
    public long getBlockSize(Path f) throws IOException {
        return maprfs.getBlockSize(f);
    }

    @Override
    public long getDefaultBlockSize() {
        return maprfs.getDefaultBlockSize();
    }

    @Override
    public long getDefaultBlockSize(Path f) {
        return maprfs.getDefaultBlockSize(f);
    }

    @Override
    public short getDefaultReplication() {
        return maprfs.getDefaultReplication();
    }

    @Override
    public short getDefaultReplication(Path path) {
        return maprfs.getDefaultReplication(path);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        return maprfs.getFileStatus(f);
    }

    @Override
    public void createSymlink(Path target, Path link, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnsupportedFileSystemException, IOException {
        maprfs.createSymlink(target, link, createParent);
    }

    @Override
    public FileStatus getFileLinkStatus(Path f) throws AccessControlException, FileNotFoundException, UnsupportedFileSystemException, IOException {
        return maprfs.getFileLinkStatus(f);
    }

    @Override
    public boolean supportsSymlinks() {
        return maprfs.supportsSymlinks();
    }

    @Override
    public Path getLinkTarget(Path f) throws IOException {
        return maprfs.getLinkTarget(f);
    }

    @Override
    public FileChecksum getFileChecksum(Path f) throws IOException {
        return maprfs.getFileChecksum(f);
    }

    @Override
    public void setVerifyChecksum(boolean verifyChecksum) {
        maprfs.setVerifyChecksum(verifyChecksum);
    }

    @Override
    public void setWriteChecksum(boolean writeChecksum) {
        maprfs.setWriteChecksum(writeChecksum);
    }

    @Override
    public FsStatus getStatus() throws IOException {
        return maprfs.getStatus();
    }

    @Override
    public FsStatus getStatus(Path p) throws IOException {
        return maprfs.getStatus(p);
    }

    @Override
    public void setPermission(Path p, FsPermission permission) throws IOException {
        maprfs.setPermission(p, permission);
    }

    @Override
    public void setOwner(Path p, String username, String groupname) throws IOException {
        maprfs.setOwner(p, username, groupname);
    }

    @Override
    public void setTimes(Path p, long mtime, long atime) throws IOException {
        maprfs.setTimes(p, mtime, atime);
    }

    @Override
    public Path createSnapshot(Path path, String snapshotName) throws IOException {
        return maprfs.createSnapshot(path, snapshotName);
    }

    @Override
    public void renameSnapshot(Path path, String snapshotOldName, String snapshotNewName) throws IOException {
        maprfs.renameSnapshot(path, snapshotOldName, snapshotNewName);
    }

    @Override
    public void deleteSnapshot(Path path, String snapshotName) throws IOException {
        maprfs.deleteSnapshot(path, snapshotName);
    }

    @Override
    public FSDataInputStream openFid(String fid, long[] ips, long chunkSize, long fileSize) throws IOException {
        return maprfs.openFid(fid, ips, chunkSize, fileSize);
    }

    @Override
    public FSDataInputStream openFid(String pfid, String file, long[] ips) throws IOException {
        return maprfs.openFid(pfid, file, ips);
    }

    @Override
    public String getZkConnectString() throws IOException {
        return maprfs.getZkConnectString();
    }

    /** Overrides from DistributedFileSystem. Everything here throws UnsupportedOperationException
     *
     */

    private static final String UNSUPPORTED_MESSAGE = "MapRFilesystem does not extend DistributedFileSystem";

    @Override
    public RemoteIterator<CachePoolEntry> listCachePools() throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public BlockStorageLocation[] getFileBlockStorageLocations(List<BlockLocation> blocks) throws IOException, UnsupportedOperationException, InvalidBlockTokenException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public boolean recoverLease(Path f) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public HdfsDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress, InetSocketAddress[] favoredNodes) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public void rename(Path src, Path dst, Options.Rename... options) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public void setQuota(Path src, long namespaceQuota, long diskspaceQuota) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public boolean mkdir(Path f, FsPermission permission) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public DFSClient getClient() {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public DiskStatus getDiskStatus() throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public long getRawCapacity() throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public long getRawUsed() throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public long getMissingBlocksCount() throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public long getUnderReplicatedBlocksCount() throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public long getCorruptBlocksCount() throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public DatanodeInfo[] getDataNodeStats() throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public DatanodeInfo[] getDataNodeStats(HdfsConstants.DatanodeReportType type) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public boolean setSafeMode(HdfsConstants.SafeModeAction action) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public boolean setSafeMode(HdfsConstants.SafeModeAction action, boolean isChecked) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public void saveNamespace() throws AccessControlException, IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public long rollEdits() throws AccessControlException, IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public boolean restoreFailedStorage(String arg) throws AccessControlException, IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public void refreshNodes() throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public void finalizeUpgrade() throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public void metaSave(String pathname) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public boolean reportChecksumFailure(Path f, FSDataInputStream in, long inPos, FSDataInputStream sums, long sumsPos) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws SecretManager.InvalidToken, IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public void cancelDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public void setBalancerBandwidth(long bandwidth) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public boolean isInSafeMode() throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public void allowSnapshot(Path path) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public void disallowSnapshot(Path path) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public SnapshottableDirectoryStatus[] getSnapshottableDirListing() throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public SnapshotDiffReport getSnapshotDiffReport(Path snapshotDir, String fromSnapshot, String toSnapshot) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public boolean isFileClosed(Path src) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public long addCacheDirective(CacheDirectiveInfo info) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public long addCacheDirective(CacheDirectiveInfo info, EnumSet<CacheFlag> flags) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public void modifyCacheDirective(CacheDirectiveInfo info) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public void modifyCacheDirective(CacheDirectiveInfo info, EnumSet<CacheFlag> flags) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public void removeCacheDirective(long id) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public RemoteIterator<CacheDirectiveEntry> listCacheDirectives(CacheDirectiveInfo filter) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public void addCachePool(CachePoolInfo info) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public void modifyCachePool(CachePoolInfo info) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public void removeCachePool(String poolName) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    protected URI canonicalizeUri(URI uri) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    protected int getDefaultPort() {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    protected Path resolveLink(Path f) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public String toString() {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    protected boolean primitiveMkdir(Path f, FsPermission absolutePermission) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    protected RemoteIterator<LocatedFileStatus> listLocatedStatus(Path p, PathFilter filter) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    protected HdfsDataOutputStream primitiveCreate(Path f, FsPermission absolutePermission, EnumSet<CreateFlag> flag, int bufferSize, short replication, long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }
}
