package org.apache.hadoop.mapred;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathId;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.MapReduceLocalData;
import org.apache.hadoop.security.UserGroupInformation;

/* Use it when mapreduce.use.maprfs is set */

class MapRFsOutputFile extends MapReduceLocalData {
  enum FidId {
    ROOT,                                   /* taskTracker                   */
    OUTPUT,                                 /* taskTracker/outputs/<jobid>   */
    OUTPUT_U,                               /* taskTracker/outputs.U/<jobid> */
    SPILL,                                  /* taskTracker/spills/<jobid>    */
    SPILL_U                                 /* taskTracker/spills.U/<jobid>  */
  }

  private static final String MAPR_LOCAL_OUT = "mapr.localoutput.dir";
  private static final String MAPR_LOCAL_SPILL = "mapr.localspill.dir";
  private static final String MAPR_UNCOMPR_SUFFIX = ".U";

  private static final Pattern FID_ARR_SPLITTER = Pattern.compile(",");

  private static final String MAPR_COMPRESS =
    "mapreduce.maprfs.use.compression";
  static final String MAPR_LOCAL_VOLS = "mapr.localvolumes.path";

  private String localOutputDir = "";
  private String localSpillDir = "";
  private String localUncompressedOutputDir = "";
  private String localUncompressedSpillDir = "";
  private JobConf conf;
  private JobID jobId;

  private String[] fidRoots;
  private String[] jobFidRoots;
  private PathId shuffleFileId;

  boolean useCompression = true;
  String taskUser;
  String taskUserGroup;

  private static final Log LOG =
    LogFactory.getLog(MapRFsOutputFile.class.getName());

  MapRFsOutputFile() {
  }

  MapRFsOutputFile(JobID id) {
    this.jobId = id;
  }

  public void setJobId(JobID id) {
    this.jobId = id;
  }

  public JobConf getConf() { return conf; }

  private void setLocalDirs() {
    localOutputDir = getMapRLocalOutputDir();
    localSpillDir = getMapRLocalSpillDir();
    localUncompressedOutputDir = getMapRLocalUncompressedOutputDir();
    localUncompressedSpillDir = getMapRLocalUncompressedSpillDir();
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;
    } else {
      this.conf = new JobConf(conf);
    }
    final String fidStrs = conf.get(TaskTracker.TT_FID_PROPERTY);
    if (fidStrs != null) {
      jobFidRoots = FID_ARR_SPLITTER.split(fidStrs);
      if (jobFidRoots.length != FidId.values().length) {
        throw new RuntimeException("Fid arr length mismatch "
          + jobFidRoots.length + " " + FidId.values().length + fidStrs);
      }
    }

    setLocalDirs();
    useCompression = useMapRCompression();
  }

  void initShuffleVolumeFid() throws IOException {
    final FileSystem maprfs = FileSystem.get(conf);
    final String shuffleRootFid =
      maprfs.mkdirsFid(new Path(getMapRFsPath(TaskTracker.LOCALHOSTNAME)));
    fidRoots = new String[] {
      shuffleRootFid,
      maprfs.mkdirsFid(shuffleRootFid,
        conf.get(MAPR_LOCAL_OUT)),
      maprfs.mkdirsFid(shuffleRootFid,
        conf.get(MAPR_LOCAL_OUT)
        + MAPR_UNCOMPR_SUFFIX),
      maprfs.mkdirsFid(shuffleRootFid,
        conf.get(MAPR_LOCAL_SPILL)),
      maprfs.mkdirsFid(shuffleRootFid,
        conf.get(MAPR_LOCAL_SPILL)
        + MAPR_UNCOMPR_SUFFIX)
    };

    shuffleFileId = FileSystem.get(conf).createPathId();
    // TODO gshegalov: hack to getFidServers
    final FSDataOutputStream fileId =
      maprfs.createFid(shuffleRootFid, "fidservers");

    shuffleFileId.setFid(shuffleRootFid);
    shuffleFileId.setIps(fileId.getFidServers());
    fileId.close();
  }

  /* Functions called by TaskTracker for creating/deleting job/task dirs
   * Always assume setConf is called before.
   */
  Path getMapRJobOutputDir(JobID jobId) {
    return new Path(localOutputDir, jobId.toString());
  }

  Path getMapRJobSpillDir(JobID jobId) {
    return new Path(localSpillDir, jobId.toString());
  }

  /* Uncompressed intermediate output */
  Path getMapRJobUncompressedOutputDir(JobID jobId) {
    return new Path(localUncompressedOutputDir, jobId.toString());
  }

  Path getMapRJobUncompressedSpillDir(JobID jobId) {
    return new Path(localUncompressedSpillDir, jobId.toString());
  }

  private Path selectMapRJobSpillDir(JobID jobId) {
    if (useCompression) {
      return getMapRJobSpillDir(jobId);
    } else {
      return getMapRJobUncompressedSpillDir(jobId);
    }
  }

  private Path getMapRTaskOutputDir(JobID jobId, TaskAttemptID taskId) {
    return new Path(
      useCompression ?
        getMapRJobOutputDir(jobId) :
        getMapRJobUncompressedOutputDir(jobId),
      taskId.toString());
  }

  private Path getMapRTaskSpillDir(JobID jobId, TaskAttemptID taskId) {
    return new Path(selectMapRJobSpillDir(jobId), taskId.toString());
  }

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  /* synchronize for NumberFormat */
  private static synchronized String getPartitionFilename(int partition) {
    return "output." + NUMBER_FORMAT.format(partition);
  }

  private String getRelOutputDir(TaskAttemptID mapTaskId) {
    final String prefix = useCompression
      ? conf.get(MAPR_LOCAL_OUT)
      :   conf.get(MAPR_LOCAL_OUT)
        + MAPR_UNCOMPR_SUFFIX;
    return prefix
         + Path.SEPARATOR + jobId.toString()
         + Path.SEPARATOR + mapTaskId.toString();
  }

  private String getRelSpillDir(TaskAttemptID mapTaskId) {
    final String prefix = useCompression
      ? conf.get(MAPR_LOCAL_SPILL)
      :   conf.get(MAPR_LOCAL_SPILL)
        + MAPR_UNCOMPR_SUFFIX;
    return prefix
         + Path.SEPARATOR + jobId.toString()
         + Path.SEPARATOR + mapTaskId.toString();
  }

  @Override
  public String getRelOutputFile(TaskAttemptID mapTaskId, int partition) {
    return getRelOutputDir(mapTaskId)
         + Path.SEPARATOR + getPartitionFilename(partition);
  }

  // Ignore size for now just send an output path
  @Override
  public Path getOutputFileForWrite(TaskAttemptID mapTaskId, long size,
         int partition) throws IOException {
    return new Path(getMapRTaskOutputDir(this.jobId, mapTaskId),
                    getPartitionFilename(partition));
  }

  @Override
  public Path getSpillFile(TaskAttemptID mapTaskId, int spillNumber)
         throws IOException {
    return new Path(getMapRTaskSpillDir(this.jobId, mapTaskId),
                    "spill" + spillNumber + ".out");
  }

  // ignore size
  @Override
  public Path getSpillFileForWrite(TaskAttemptID mapTaskId, int spillNumber,
         long size) throws IOException {
    return getSpillFile(mapTaskId, spillNumber);
  }

  @Override
  public Path getSpillIndexFile(TaskAttemptID mapTaskId, int spillNumber)
         throws IOException {
    return new Path(getMapRTaskSpillDir(this.jobId, mapTaskId),
                    "spill" + spillNumber + ".out.index");
  }

  // ignore size
  @Override
  public Path getSpillIndexFileForWrite(TaskAttemptID mapTaskId,
         int spillNumber, long size) throws IOException {
    return getSpillIndexFile(mapTaskId, spillNumber);
  }

  // files intermediate to reduce side shuffle
  @Override
  public Path getInputFile(int mapId, TaskAttemptID reduceTaskId)
         throws IOException {
      return new Path(getMapRTaskSpillDir(this.jobId, reduceTaskId),
                      "map_" + mapId + ".out");
  }

  // ignore size
  public Path getInputFileForWrite(TaskID mapId, TaskAttemptID reduceTaskId,
         long size) throws IOException {
    return getInputFile(mapId.getId(), reduceTaskId);
  }

  @Override
  public void removeAll(TaskAttemptID taskId) throws IOException {
    removeAll(taskId, true);
  }

  @Override
  public void removeAll(TaskAttemptID taskId, boolean isSetup)
    throws IOException {
    // MapR Bug 2815. Do not remove files per task. TT will cleanup job dirs.
    if (isSetup) {
      FileSystem fs = FileSystem.get(conf);
      final String[][] fidRelDirs = new String[][] {
        new String[] {
          getSpillFid(),
          getRelSpillDir(taskId)
        },
        new String[] {
          getOutputFid(),
          getRelOutputDir(taskId)
        }
      };
      try {
        /* Empty directories by removing them and creating them again */
        if (LOG.isDebugEnabled()) {
          LOG.debug("Cleaning up dirs " + Arrays.toString(fidRelDirs)
                  + " for " + jobId.toString() + " " + taskId.toString());
        }
        for (String[] fidDir : fidRelDirs) {
          fs.deleteFid(fidDir[0], fidDir[1]);
          final String taskDirFid = fs.mkdirsFid(fidDir[0], fidDir[1]);
          if (taskUser != null) {
            fs.setOwnerFid(taskDirFid, taskUser, taskUserGroup);
          }
        }
      } catch (IOException ioe) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Failed to remove directories "
                 + Arrays.toString(fidRelDirs) + " for " +
                   jobId.toString() + " " + taskId.toString(), ioe);
        }
        throw ioe;
      }
    } else {
      taskUser = null;
      taskUserGroup = null;
    }
  }

  // ignore size
  @Override
  public Path getLocalPathForWrite(String pathStr, long size) {
    return new Path(selectMapRJobSpillDir(this.jobId), pathStr);
  }

  String[] createJobDirFids(JobID jobId, String jobUser, String jobGroup)
    throws IOException
  {
    final FileSystem fs = FileSystem.get(conf);
    final String jobIdStr = jobId.toString();
    final String[] jobFids = new String[fidRoots.length];

    // TODO gshegalov
    // use only this after FileClient gets fid,relpath->fid lookup cache
    //
    jobFids[FidId.ROOT.ordinal()] = fidRoots[FidId.ROOT.ordinal()];

    for (int i = FidId.ROOT.ordinal() + 1; i < fidRoots.length; i++) {
      jobFids[i] = fs.mkdirsFid(fidRoots[i], jobIdStr);
      if (jobUser != null) {
        fs.setOwnerFid(jobFids[i], jobUser, jobGroup);
      }
    }
    return jobFids;
  }

  public void setUser(String userName, String groupName) {
    taskUser = userName;
    taskUserGroup = groupName;
  }

  private boolean useMapRCompression() {
    return conf.getBoolean(MAPR_COMPRESS, true);
  }

  String getMapRVolumeMountPoint(String hostname) {
    return conf.get(MAPR_LOCAL_VOLS)
         + Path.SEPARATOR + hostname
         + Path.SEPARATOR + "mapred" + Path.SEPARATOR;
  }

  String getMapRFsPath(String hostname) {
    return conf.get(MAPR_LOCAL_VOLS)
         + Path.SEPARATOR
         + hostname
         + Path.SEPARATOR + "mapred" + Path.SEPARATOR
         + "taskTracker" + Path.SEPARATOR;
  }

  private String getMapRLocalOutputDir() {
    return getMapRFsPath(TaskTracker.LOCALHOSTNAME)
         + conf.get(MAPR_LOCAL_OUT);
  }

  /* for uncompressed intermediate output use output.U and spill.U */
  private String getMapRLocalUncompressedOutputDir() {
    return getMapRLocalOutputDir() + MAPR_UNCOMPR_SUFFIX;
  }

  private String getMapRLocalSpillDir() {
    return getMapRFsPath(TaskTracker.LOCALHOSTNAME)
         + conf.get(MAPR_LOCAL_SPILL);
  }

  private String getMapRLocalUncompressedSpillDir() {
    return getMapRLocalSpillDir() + MAPR_UNCOMPR_SUFFIX;
  }

  @Override
  String getOutputFid() {
    return jobFidRoots[
      useCompression ? FidId.OUTPUT.ordinal() : FidId.OUTPUT_U.ordinal()];
  }

  @Override
  String getSpillFid() {
    return jobFidRoots[
      useCompression ? FidId.SPILL.ordinal() : FidId.SPILL_U.ordinal()];
  }

  PathId getShuffleRootFid() {
    return shuffleFileId;
  }

  @Override
  String getSpillFileForWriteFid(
    TaskAttemptID mapTaskId,
    int spillNumber,
    long size)
  throws IOException
  {
    return mapTaskId + Path.SEPARATOR
         + "spill" + spillNumber + ".out";
  }

  @Override
  String getOutputFileForWriteFid(
    TaskAttemptID mapTaskId,
    long size,
    int partition)
  throws IOException
  {
    return mapTaskId + Path.SEPARATOR
         + getPartitionFilename(partition);
  }
}
