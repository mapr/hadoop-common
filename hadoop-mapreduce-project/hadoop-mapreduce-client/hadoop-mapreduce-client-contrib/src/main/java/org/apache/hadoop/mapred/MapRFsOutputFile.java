package org.apache.hadoop.mapred;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathId;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;

public class MapRFsOutputFile extends MapOutputFile {

  enum FidId {
    ROOT,                                   /* nodeManager                   */
    OUTPUT,                                 /* nodeManager/output/<jobid>   */
    OUTPUT_U,                               /* nodeManager/output.U/<jobid> */
    SPILL,                                  /* nodeManager/spill/<jobid>    */
    SPILL_U;                                /* nodeManager/spill.U/<jobid>  */
  }

  private static final String MAPR_LOCAL_OUT = "mapr.localoutput.dir";
  private static final String MAPR_LOCAL_SPILL = "mapr.localspill.dir";
  private static final String MAPR_UNCOMPR_SUFFIX = ".U";

  private static final String MAPR_COMPRESS =
    "mapreduce.maprfs.use.compression";
  static final String MAPR_LOCAL_VOLS = "mapr.localvolumes.path";

  private String localOutputDir = "";
  private String localSpillDir = "";
  private String localUncompressedOutputDir = "";
  private String localUncompressedSpillDir = "";
  private String hostname;
  private JobConf conf;
  private Map<String, String> jobFidRoots;
  
  boolean useCompression = true;
  String taskUser;
  String taskUserGroup;

  private static final Log LOG =
    LogFactory.getLog(MapRFsOutputFile.class.getName());

  public MapRFsOutputFile() {
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;
    } else {
      this.conf = new JobConf(conf);
    }
    ByteBuffer serviceData = AuxiliaryServiceHelper.getServiceDataFromEnv(
        "mapr_direct_shuffle", System.getenv());
    if ( serviceData != null ) {
      DataInputByteBuffer in = new DataInputByteBuffer();
      in.reset(serviceData);
      try {
      hostname = WritableUtils.readString(in);
      int size = WritableUtils.readVInt(in);
      jobFidRoots = new HashMap<String,String>(size);
      for (int i = 0; i < size; i++) {
        String dirName = WritableUtils.readString(in);
        PathId pathId = FileSystem.get(conf).createPathId();
        pathId.readFields(in);
        jobFidRoots.put(dirName, pathId.getFid());
      }
      } catch (IOException e) {
        LOG.error("Could not parse servicedata", e);
      }
    }
    setLocalDirs();
    useCompression = useMapRCompression();
  }

  @Override
  public JobConf getConf() { return conf; }

  private boolean useMapRCompression() {
    return conf.getBoolean(MAPR_COMPRESS, true);
  }
  
  private void setLocalDirs() {
    localOutputDir = getMapRLocalOutputDir();
    localSpillDir = getMapRLocalSpillDir();
    localUncompressedOutputDir = getMapRLocalUncompressedOutputDir();
    localUncompressedSpillDir = getMapRLocalUncompressedSpillDir();
  }
  
  private Path getMapRTaskSpillDir(JobID jobId, String taskId) {
    return new Path(selectMapRJobSpillDir(jobId), taskId);
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
         + Path.SEPARATOR + mapTaskId.getJobID().toString()
         + Path.SEPARATOR + mapTaskId.toString();
  }

  private String getRelOutputDir(String mapTaskId) {
    TaskAttemptID taId = TaskAttemptID.forName(mapTaskId);
    final String prefix = useCompression
      ? conf.get(MAPR_LOCAL_OUT)
      :   conf.get(MAPR_LOCAL_OUT)
        + MAPR_UNCOMPR_SUFFIX;
    return prefix
         + Path.SEPARATOR + taId.getJobID().toString()
         + Path.SEPARATOR + mapTaskId;
  }

  private String getRelSpillDir(TaskAttemptID mapTaskId) {
    final String prefix = useCompression
      ? conf.get(MAPR_LOCAL_SPILL)
      :   conf.get(MAPR_LOCAL_SPILL)
        + MAPR_UNCOMPR_SUFFIX;
    return prefix
         + Path.SEPARATOR + mapTaskId.getJobID().toString()
         + Path.SEPARATOR + mapTaskId.toString();
  }

  private String getRelSpillDir(String mapTaskId) {
    TaskAttemptID taId = TaskAttemptID.forName(mapTaskId);
    final String prefix = useCompression
      ? conf.get(MAPR_LOCAL_SPILL)
      :   conf.get(MAPR_LOCAL_SPILL)
        + MAPR_UNCOMPR_SUFFIX;
    return prefix
         + Path.SEPARATOR + taId.getJobID().toString()
         + Path.SEPARATOR + mapTaskId;
  }
  
  @Override
  public Path getOutputFile() throws IOException {
    // Looks like defunct for MapRFS ???
    return null;
  }

  // @Override TODO - figure out where it fits - used in reduce phase
  public String getRelOutputFile(TaskAttemptID mapTaskId, int partition) {
    return getRelOutputDir(mapTaskId)
         + Path.SEPARATOR + getPartitionFilename(partition);
  }

  @Override
  public Path getOutputFileForWrite(long size) throws IOException {
    // TODO need to get partition number
    TaskAttemptID taId = TaskAttemptID.forName(conf.get(JobContext.TASK_ATTEMPT_ID));
    return new Path(getMapRTaskOutputDir(taId.getJobID(), conf.get(JobContext.TASK_ATTEMPT_ID)),
        getPartitionFilename(-1));
  }

  // Ignore size for now just send an output path
 // @Override TODO overwriting part
  public Path getOutputFileForWrite(TaskAttemptID mapTaskId, long size,
         int partition) throws IOException {
    return new Path(getMapRTaskOutputDir(mapTaskId.getJobID(), mapTaskId),
                    getPartitionFilename(partition));
  }

  @Override
  public Path getOutputFileForWriteInVolume(Path existing) {
    // TODO may be defunct?
    return null;
  }

  @Override
  public Path getOutputIndexFile() throws IOException {
    // TODO may be defunct?
    return null;
  }

  @Override
  public Path getOutputIndexFileForWrite(long size) throws IOException {
    // TODO may be defunct?
    return null;
  }

  @Override
  public Path getOutputIndexFileForWriteInVolume(Path existing) {
    // TODO may be defunct?
    return null;
  }

  @Override
  public Path getSpillFile(int spillNumber) throws IOException {
    TaskAttemptID taId = TaskAttemptID.forName(conf.get(JobContext.TASK_ATTEMPT_ID));
    return new Path(getMapRTaskSpillDir(taId.getJobID(), conf.get(JobContext.TASK_ATTEMPT_ID)),
        "spill" + spillNumber + ".out");
  }

  @Override
  public Path getSpillFileForWrite(int spillNumber, long size)
      throws IOException {
    return getSpillFile(spillNumber);
  }
  
  // ignore size
  //@Override TODO check
  public Path getLocalPathForWrite(String pathStr, long size) {
    TaskAttemptID taId = TaskAttemptID.forName(conf.get(JobContext.TASK_ATTEMPT_ID));
    return new Path(selectMapRJobSpillDir(taId.getJobID()), pathStr);
  }

  @Override
  public Path getSpillIndexFile(int spillNumber) throws IOException {
    TaskAttemptID taId = TaskAttemptID.forName(conf.get(JobContext.TASK_ATTEMPT_ID));
    return new Path(getMapRTaskSpillDir(taId.getJobID(), conf.get(JobContext.TASK_ATTEMPT_ID)),
        "spill" + spillNumber + ".out.index");
  }

  @Override
  public Path getSpillIndexFileForWrite(int spillNumber, long size)
      throws IOException {
    return getSpillIndexFile(spillNumber);
  }

  @Override
  public Path getInputFile(int mapId) throws IOException {
    TaskAttemptID taId = TaskAttemptID.forName(conf.get(JobContext.TASK_ATTEMPT_ID));
    return new Path(getMapRTaskSpillDir(taId.getJobID(), conf.get(JobContext.TASK_ATTEMPT_ID)),
        "map_" + mapId + ".out");
  }

  @Override
  public Path getInputFileForWrite(TaskID mapId, long size) throws IOException {
    return getInputFile(mapId.getId());
  }


  private Path getMapRTaskSpillDir(JobID jobId, TaskAttemptID taskId) {
    return new Path(selectMapRJobSpillDir(jobId), taskId.toString());
  }

  // files intermediate to reduce side shuffle
 // @Override TODO specific to Mapr
  public Path getInputFile(int mapId, TaskAttemptID reduceTaskId)
         throws IOException {
      return new Path(getMapRTaskSpillDir(reduceTaskId.getJobID(), reduceTaskId),
                      "map_" + mapId + ".out");
  }

  // ignore size
  public Path getInputFileForWrite(TaskID mapId, TaskAttemptID reduceTaskId,
         long size) throws IOException {
    return getInputFile(mapId.getId(), reduceTaskId);
  }

  @Override
  public void removeAll() throws IOException {
    removeAll(true);
  }
  
  public void removeAll(boolean isSetup) throws IOException {
    // MapR Bug 2815. Do not remove files per task. TT will cleanup job dirs.
    if (isSetup) {
      FileSystem fs = FileSystem.get(conf);
      final String[][] fidRelDirs = new String[][] {
        new String[] {
          getSpillFid(),
          getRelSpillDir(conf.get(JobContext.TASK_ATTEMPT_ID))
        },
        new String[] {
          getOutputFid(),
          getRelOutputDir(conf.get(JobContext.TASK_ATTEMPT_ID))
        }
      };
      try {
        /* Empty directories by removing them and creating them again */
        if (LOG.isDebugEnabled()) {
          LOG.debug("Cleaning up dirs " + Arrays.toString(fidRelDirs)
                  + " for " + conf.get(JobContext.TASK_ATTEMPT_ID));
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
                 + Arrays.toString(fidRelDirs) + " for "
              + conf.get(JobContext.TASK_ATTEMPT_ID), ioe);
        }
        throw ioe;
      }
    } else {
      taskUser = null;
      taskUserGroup = null;
    }


  }

  private String getMapRFsPath(String hostname) {
    return conf.get(MAPR_LOCAL_VOLS)
         + Path.SEPARATOR
         + hostname
         + Path.SEPARATOR + "mapred" + Path.SEPARATOR
         + "nodeManager" + Path.SEPARATOR;
  }

  private String getMapRLocalOutputDir() {
    return getMapRFsPath(hostname)
           + conf.get(MAPR_LOCAL_OUT);
  }

  /* for uncompressed intermediate output use output.U and spill.U */
  private String getMapRLocalUncompressedOutputDir() {
    return getMapRLocalOutputDir() + MAPR_UNCOMPR_SUFFIX;
  }

  private String getMapRLocalSpillDir() {
    return getMapRFsPath(hostname)
           + conf.get(MAPR_LOCAL_SPILL);
  }

  private String getMapRLocalUncompressedSpillDir() {
    return getMapRLocalSpillDir() + MAPR_UNCOMPR_SUFFIX;
  }

  // TODO will be probably public since used in cleaning up temp dirs after job is done
  /** Functions called by MR AM? for creating/deleting job/task dirs
   * Always assume setConf is called before.
   */
  Path getMapRJobOutputDir(JobID jobId) {
    return new Path(localOutputDir, jobId.toString());
  }

  private Path getMapRJobSpillDir(JobID jobId) {
    return new Path(localSpillDir, jobId.toString());
  }

  /** Uncompressed intermediate output 
   */
  private Path getMapRJobUncompressedOutputDir(JobID jobId) {
    return new Path(localUncompressedOutputDir, jobId.toString());
  }

  private Path getMapRJobUncompressedSpillDir(JobID jobId) {
    return new Path(localUncompressedSpillDir, jobId.toString());
  }

  private Path selectMapRJobSpillDir(JobID jobId) {
    if (useCompression) {
      return getMapRJobSpillDir(jobId);
    } else {
      return getMapRJobUncompressedSpillDir(jobId);
    }
  }

  private Path getMapRTaskOutputDir(JobID jobId, String taskId) {
    return new Path(
      useCompression ?
        getMapRJobOutputDir(jobId) :
        getMapRJobUncompressedOutputDir(jobId),
        taskId);
  }
  
  private Path getMapRTaskOutputDir(JobID jobId, TaskAttemptID taskId) {
    return new Path(
      useCompression ?
        getMapRJobOutputDir(jobId) :
        getMapRJobUncompressedOutputDir(jobId),
      taskId.toString());
  }


  public String getOutputFid() {
    return useCompression ? jobFidRoots.get(conf.get(MAPR_LOCAL_OUT)) : 
      jobFidRoots.get(conf.get(MAPR_LOCAL_OUT) + MAPR_UNCOMPR_SUFFIX);
  }

  public String getSpillFid() {
    return useCompression ? jobFidRoots.get(conf.get(MAPR_LOCAL_SPILL)) : 
      jobFidRoots.get(conf.get(MAPR_LOCAL_SPILL) + MAPR_UNCOMPR_SUFFIX);
  }

  public String getSpillFileForWriteFid(
    TaskAttemptID mapTaskId,
    int spillNumber,
    long size)
  throws IOException
  {
    return mapTaskId.getJobID().toString() + Path.SEPARATOR 
           + mapTaskId + Path.SEPARATOR
           + "spill" + spillNumber + ".out";
  }

  //@Override TODO: Will need to deal with Override
  public String getOutputFileForWriteFid(
    TaskAttemptID mapTaskId,
    long size,
    int partition)
  throws IOException
  {
    return mapTaskId.getJobID().toString() + Path.SEPARATOR
         + mapTaskId + Path.SEPARATOR
         + getPartitionFilename(partition);
  }


  public void setUser(String userName, String groupName) {
    taskUser = userName;
    taskUserGroup = groupName;
  }

  String getMapRVolumeMountPoint(String hostname) {
    return conf.get(MAPR_LOCAL_VOLS)
         + Path.SEPARATOR + hostname
         + Path.SEPARATOR + "mapred" + Path.SEPARATOR;
  }
}
