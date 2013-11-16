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
package org.apache.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.security.authorize.ProxyUsers;

enum NodeState {
  STOPPED, RUNNING
}

class MapRNode {
  boolean isCldb;
  boolean isFileServer;
  boolean isZookeeper;
  int nodeId;
  int port;
  Process mfsPr;
  String logFile;
  String diskName;
  String hostnameFile;
  String hostIdFile;
  String localhost;
  boolean format;
  NodeState state;

  public void Init(
    boolean isCldb, boolean isZookeeper, boolean isFileServer, int nodeId) {

    this.isCldb = isCldb;
    this.isZookeeper = isZookeeper;
    this.isFileServer = isFileServer;
    this.nodeId = nodeId;
    format = true;

    this.port = MiniDFSCluster.defaultMfsPort + nodeId;
    this.diskName = "/tmp/disk" + this.port + ".img";
    this.logFile = MiniDFSCluster.installDir + "/logs/mfs." + this.port + ".log";
    this.hostnameFile = MiniDFSCluster.installDir + "/hostname" + this.port;
    this.hostIdFile = MiniDFSCluster.installDir + "/hostid" + this.port;

    this.state = NodeState.STOPPED;

    try {
      this.localhost = InetAddress.getLocalHost().getHostAddress();
    } catch (Exception e) {
      e.printStackTrace();
    }

    String[] commands[] = {
      {"/bin/sh", "-c", "dd bs=8192 seek=1048584 count=1 if=/dev/zero of=" + this.diskName},
      {"/bin/sh", "-c", "echo host-" + this.port + " > " + this.hostnameFile},
      {"/bin/sh", "-c", MiniDFSCluster.mruuidgen + " > " + this.hostIdFile}
    };

    RunCommand rc = new RunCommand();
    for (int i = 0; i < commands.length; ++i) {
      rc.Init(commands[i], "", false, false);
      rc.Run();
    }
    if (isCldb) {
      String[] cldbCommands[] = {
        {"/bin/sh", "-c", "/bin/hostname --fqdn > " + this.hostnameFile},
        {"/bin/sh", "-c", "cp " + this.hostnameFile + " " + MiniDFSCluster.installDir + "/hostname"},
        {"/bin/sh", "-c", "cp " + this.hostIdFile + " " + MiniDFSCluster.installDir + "/hostid"},
        {"/bin/sh", "-c", "echo my.cluster.com " + localhost + ":" + MiniDFSCluster.cldbPort + " > " + MiniDFSCluster.maprClustersFile}
      };
      for (int i = 0; i < cldbCommands.length; ++i) {
        rc.Init(cldbCommands[i], "", false, false);
        rc.Run();
      }

    }
  }

  public String GetName() {
    String name = Integer.toString(port);
    return name;
  }

  public NodeState State() {
    return state;
  }
  public void CleanUp() {

    StopCldb();

    CleanUpZookeeper();
    StopZookeeper();

    StopFileServer();

    String commands[] = {
      "rm "+ this.diskName,
      "rm " + this.hostnameFile,
      "rm " + this.hostIdFile
    };
    RunCommand rc = new RunCommand();
    for (int i = 0; i < commands.length; ++i) {
      rc.Init(commands[i], "", false, false);
      rc.Run();
    }
  }


  public int Start() {

    StartZookeeper();
    if (format) {
      CleanUpZookeeper();
    }
    StartFileServer();

    if (format) {
      PrepareTheDisk();
    }

    MountTheDisk();
    ListSPs();

    StartCldb();
    format = false;

    state = NodeState.RUNNING;
    return 0;
  }

  void DecrementCldbVolMinReplica() {
    //Decrease the replication count of cldb.internal volume
    RunCommand rc = new RunCommand();
    rc.Init(MiniDFSCluster.maprCli + " volume modify -name mapr.cldb.internal -minreplication 1", "", false, false);
    rc.Run();
  }

  public int Stop() {
    DecrementCldbVolMinReplica();
    StopCldb();
    StopZookeeper();
    StopFileServer();
    return 0;
  }

  public int Kill() {
    DecrementCldbVolMinReplica();
    KillFileServer();
    StopCldb();
    StopZookeeper();
    return 0;
  }

  int ListSPs() {
    RunCommand rc = new RunCommand();
    rc.Init("/opt/mapr/server/mrconfig -p " + port + " sp list", "", false,
      false);
    rc.Run();
    return 0;

  }

  public int MountTheDisk() {
    RunCommand rc = new RunCommand();
    rc.Init(MiniDFSCluster.testConfigPy + " -h " + localhost + " -p " + port + " -m single -d " + diskName, "", false, false);
    rc.Run();
    return 0;
  }

  int PrepareTheDisk() {
    RunCommand rc = new RunCommand();
    rc.Init(MiniDFSCluster.testConfigPy + " -h " + localhost + " -p " + port + " -P -s 8192 -d " + diskName, "", false, false);
    return rc.Run();
  }

  public int StartFileServer() {
    RunCommand rc = new RunCommand();
    rc.Init(MiniDFSCluster.mfsExe + " -e -p "+port+" -m 512 both -h " + hostnameFile + " -H " +hostIdFile + " -L " + logFile, "", true, false);
    rc.Run();
    mfsPr = rc.BGProcess();
    if (mfsPr != null) {
      System.out.println("mfsPr is non null");
    } else {
      System.out.println("mfsPr is null");
    }
    return 0;
  }

  public int StopFileServer() {
    //GIRIRunCommand rc = new RunCommand();
    //GIRI rc.Init(MiniDFSCluster.mfsExe + " -e -p "+port+" -m 512 both -h " + hostnameFile + " -H " +hostIdFile + " -L " + logFile, "", true, false);
    //GIRIrc.Run();

    return KillFileServer();
  }

  int KillFileServer() {
    if (mfsPr != null) {

      System.out.println("Killing mfs");
      mfsPr.destroy();
      mfsPr = null;
    }
    return 0;
  }

  int StartCldb() {

    if (!isCldb) {
      return 0;
    }

    RunCommand rc = new RunCommand();
    rc.Init(MiniDFSCluster.cldbInitScript + " start", "", false, false);
    return rc.Run();
  }

  int StopCldb() {

    if (!isCldb) {
      return 0;
    }

    RunCommand rc = new RunCommand();
    rc.Init(MiniDFSCluster.cldbInitScript + " stop", "", false, false);
    return rc.Run();
  }

  int StartZookeeper() {
    if (!isZookeeper) {
      return 0;
    }

    String commands[] = {
      "/opt/mapr/zookeeper/zookeeper-3.3.6/bin/zkServer.sh start"
    };

    RunCommand rc = new RunCommand();
    for (int i = 0; i < commands.length; ++i) {
      rc.Init(commands[i], "", false, false);
      rc.Run();
    }
    return 0;
  }

  int CleanUpZookeeper() {
    if (!isZookeeper) {
      return 0;
    }

    String commands[] = {
      "/opt/mapr/zookeeper/zookeeper-3.3.6/bin/zkCli.sh -server localhost:5181 delete /datacenter/controlnodes/cldb/active/CLDBMaster",
      "/opt/mapr/zookeeper/zookeeper-3.3.6/bin/zkCli.sh -server localhost:5181 delete /datacenter/controlnodes/cldb/epoch/1/KvStoreContainerInfo"
    };

    RunCommand rc = new RunCommand();
    for (int i = 0; i < commands.length; ++i) {
      rc.Init(commands[i], "", false, false);
      rc.Run();
    }
    return 0;
  }

  int StopZookeeper() {
    if (!isZookeeper) {
      return 0;
    }

    String commands[] = {
      "/opt/mapr/zookeeper/zookeeper-3.3.6/bin/zkServer.sh stop"
    };

    RunCommand rc = new RunCommand();
    for (int i = 0; i < commands.length; ++i) {
      rc.Init(commands[i], "", false, false);
      rc.Run();
    }
    return 0;
  }

  public long getBlockNumber(String fid, long offset) throws Exception
  {
    Stop();
    RunCommand rc = new RunCommand();
    String[] cmd = {
      "/bin/sh", "-c",
      MiniDFSCluster.mfsdbFile + " " + diskName + " -c \"fid blocknum " +
      fid+"."+offset+"\""
    };

    rc.Init(cmd, "", false, true);
    rc.Run();
    Start();
    return Long.parseLong(rc.OutPutStr());
  }

  boolean corruptBlock(long blockNum) throws Exception
  {

    Stop();
    Random random = new Random();
    String badString = "BADBAD";
    int rand = random.nextInt(MiniDFSCluster.blockSize/2);
    boolean corrupted = false;
    RunCommand rc = new RunCommand();
    String[] cmd = {
      "/bin/sh", "-c",
      MiniDFSCluster.mfsdbFile + " " + diskName + " -c \"write " + blockNum +
      " " + rand + " " + badString+ " string "+ " \" "
    };

    rc.Init(cmd, "", false, false);
    rc.Run();
    Start();

    return true;
  }
}

class RunCommand {

  String[] command;
  String singleCommand;
  String args;
  boolean isBG;
  Process prCreated;
  boolean reqOutput;
  String outputStr;

  public void Init(String command, String args, boolean isBG,
    boolean reqOutput) {

    this.singleCommand = command;
    this.args = args;
    this.isBG = isBG;
    this.reqOutput = reqOutput;
  }

  public void Init(String[] command, String args, boolean isBG,
    boolean reqOutput) {
    this.command = command;
    this.args = args;
    this.isBG = isBG;
    this.reqOutput = reqOutput;
  }

  public Process BGProcess() {
    return prCreated;
  }

  public String OutPutStr() {
    return outputStr;
  }

  public int Run() {
    int retval = 0;
    prCreated = null;

    try {
      Runtime rt = Runtime.getRuntime();
      Process pr;

      if (singleCommand == null) {
        System.out.print("Command ran: ");
        for (int i = 0; i < command.length; ++i) {
          System.out.print(command[i]);
        }
        System.out.println("");
        pr = rt.exec(command);
      } else {
        System.out.println("Command ran: " + singleCommand);
        pr = rt.exec(singleCommand);
      }

      if (! isBG) {
        BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));

        String line=null;

        while((line=input.readLine()) != null) {
          System.out.println(line);
          if (reqOutput) {
            //Saves only last line of the output
            outputStr = line;
          }
        }
        retval = pr.waitFor();
      } else {
        prCreated = pr;
        System.out.println("created BG process: " + prCreated);
      }

      System.out.println("Exited with error code "+retval);
    } catch (Exception e) {
      System.out.println(e.toString());
      e.printStackTrace();
      retval = -1;
    }

    return retval;
  }
}

/**
 * This class creates a single-process MFS cluster for junit testing.
 * The data directories for non-simulated DFS are under the testing directory.
 * For simulated data nodes, no underlying fs storage is used.
 */
public class MiniDFSCluster {

  // TODO: this class is to suppress compiler errors
  public class DataNodeProperties {
    DataNode datanode;
    Configuration conf;
    String[] dnArgs;

    DataNodeProperties(DataNode node, Configuration conf, String[] args) {
      this.datanode = node;
      this.conf = conf;
      this.dnArgs = args;
    }
  }

  /**
   * Stores the information related to a namenode in the cluster
   * Needed for compilation only.
   */
  static class NameNodeInfo {
    final NameNode nameNode;
    final Configuration conf;
    final String nameserviceId;
    final String nnId;
    NameNodeInfo(NameNode nn, String nameserviceId, String nnId,
        Configuration conf) {
      this.nameNode = nn;
      this.nameserviceId = nameserviceId;
      this.nnId = nnId;
      this.conf = conf;
    }
  }

  /**
   * Class to construct instances of MiniDFSClusters with specific options.
   */
  public static class Builder {
    private int nameNodePort = 0;
    private int nameNodeHttpPort = 0;
    private final Configuration conf;
    private int numDataNodes = 1;
    private boolean format = true;
    private boolean manageNameDfsDirs = true;
    private boolean manageNameDfsSharedDirs = true;
    private boolean enableManagedDfsDirsRedundancy = true;
    private boolean manageDataDfsDirs = true;
    private StartupOption option = null;
    private String[] racks = null;
    private String [] hosts = null;
    private long [] simulatedCapacities = null;
    private String clusterId = null;
    private boolean waitSafeMode = true;
    private boolean setupHostsFile = false;
    private MiniDFSNNTopology nnTopology = null;
    private boolean checkExitOnShutdown = true;
    private boolean checkDataNodeAddrConfig = false;
    private boolean checkDataNodeHostConfig = false;

    public Builder(Configuration conf) {
      this.conf = conf;
    }

    /**
     * Default: 0
     */
    public Builder nameNodePort(int val) {
      this.nameNodePort = val;
      return this;
    }

    /**
     * Default: 0
     */
    public Builder nameNodeHttpPort(int val) {
      this.nameNodeHttpPort = val;
      return this;
    }

    /**
     * Default: 1
     */
    public Builder numDataNodes(int val) {
      this.numDataNodes = val;
      return this;
    }

    /**
     * Default: true
     */
    public Builder format(boolean val) {
      this.format = val;
      return this;
    }

    /**
     * Default: true
     */
    public Builder manageNameDfsDirs(boolean val) {
      this.manageNameDfsDirs = val;
      return this;
    }

    /**
     * Default: true
     */
    public Builder manageNameDfsSharedDirs(boolean val) {
      this.manageNameDfsSharedDirs = val;
      return this;
    }

    /**
     * Default: true
     */
    public Builder enableManagedDfsDirsRedundancy(boolean val) {
      this.enableManagedDfsDirsRedundancy = val;
      return this;
    }

    /**
     * Default: true
     */
    public Builder manageDataDfsDirs(boolean val) {
      this.manageDataDfsDirs = val;
      return this;
    }

    /**
     * Default: null
     */
    public Builder startupOption(StartupOption val) {
      this.option = val;
      return this;
    }

    /**
     * Default: null
     */
    public Builder racks(String[] val) {
      this.racks = val;
      return this;
    }

    /**
     * Default: null
     */
    public Builder hosts(String[] val) {
      this.hosts = val;
      return this;
    }

    /**
     * Default: null
     */
    public Builder simulatedCapacities(long[] val) {
      this.simulatedCapacities = val;
      return this;
    }

    /**
     * Default: true
     */
    public Builder waitSafeMode(boolean val) {
      this.waitSafeMode = val;
      return this;
    }

    /**
     * Default: true
     */
    public Builder checkExitOnShutdown(boolean val) {
      this.checkExitOnShutdown = val;
      return this;
    }

    /**
     * Default: false
     */
    public Builder checkDataNodeAddrConfig(boolean val) {
      this.checkDataNodeAddrConfig = val;
      return this;
    }

    /**
     * Default: false
     */
    public Builder checkDataNodeHostConfig(boolean val) {
      this.checkDataNodeHostConfig = val;
      return this;
    }

    /**
     * Default: null
     */
    public Builder clusterId(String cid) {
      this.clusterId = cid;
      return this;
    }

    /**
     * Default: false
     * When true the hosts file/include file for the cluster is setup
     */
    public Builder setupHostsFile(boolean val) {
      this.setupHostsFile = val;
      return this;
    }

    /**
     * Default: a single namenode.
     * See {@link MiniDFSNNTopology#simpleFederatedTopology(int)} to set up
     * federated nameservices
     */
    public Builder nnTopology(MiniDFSNNTopology topology) {
      this.nnTopology = topology;
      return this;
    }

    /**
     * Construct the actual MiniDFSCluster
     */
    public MiniDFSCluster build() throws IOException {
      return new MiniDFSCluster(this);
    }
  }

  static String installDir="/opt/mapr";
  static String tmpPath="/tmp/mapr-scratch/";
  static String mfsExe=installDir+"/server/mfs";
  static String hadoopExe=installDir+"/hadoop/hadoop-2.2.0/bin/hadoop";
  static String testConfigPy=installDir + "/server/testconfig.py";
  static String cldbInitScript="/opt/mapr/cldb/cldb ";
  static String mruuidgen=installDir + "/server/mruuidgen";
  static String maprClustersFile=installDir + "/conf/mapr-clusters.conf";
  static String mfsdbFile=installDir + "/server/tools/mfsdb";
  static String maprCli=installDir + "/bin/maprcli";
  static int cldbPort=7222;
  static int defaultMfsPort=5660;
  static int blockSize=8192;
  static int clusterSize=8*blockSize;
  static int chunkSize=64*1024*1024;
  public static final String PROP_TEST_BUILD_DATA = "test.build.data";

  private String volName;
  private int numReplicas;  //GIRI: req?
  private int numNodes;
  private MapRNode[] nodes;
  private boolean isClusterUp;

  private Configuration conf;

  /**
   * This null constructor is used only when wishing to start a data node cluster
   * without a name node (ie when the name node is started elsewhere).
   */
  public MiniDFSCluster() {
  }

  /**
   * Used by builder to create and return an instance of MiniDFSCluster
   */
  protected MiniDFSCluster(Builder builder) throws IOException {
    this (0,
            builder.conf,
            builder.numDataNodes,
            builder.format,
            builder.manageNameDfsDirs,
            builder.manageDataDfsDirs,
            builder.option,
            builder.racks, builder.hosts,
            builder.simulatedCapacities);
  }

  /**
   * Modify the config and start up the servers with the given operation.
   * Servers will be started on free ports.
   * <p>
   * The caller must manage the creation of NameNode and DataNode directories
   * and have already set dfs.name.dir and dfs.data.dir in the given conf.
   *
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param nameNodeOperation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   */
  public MiniDFSCluster(Configuration conf,
                        int numDataNodes,
                        StartupOption nameNodeOperation) throws IOException {
    this(0, conf, numDataNodes, false, false, false,  nameNodeOperation,
          null, null, null);
  }

  /**
   * Modify the config and start up the servers.  The rpc and info ports for
   * servers are guaranteed to use free ports.
   * <p>
   * NameNode and DataNode directory creation and configuration will be
   * managed by this class.
   *
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param racks array of strings indicating the rack that each DataNode is on
   */
  public MiniDFSCluster(Configuration conf,
                        int numDataNodes,
                        boolean format,
                        String[] racks) throws IOException {
    this(0, conf, numDataNodes, format, true, true,  null, racks, null, null);
  }

  /**
   * Modify the config and start up the servers.  The rpc and info ports for
   * servers are guaranteed to use free ports.
   * <p>
   * NameNode and DataNode directory creation and configuration will be
   * managed by this class.
   *
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param hosts array of strings indicating the hostname for each DataNode
   */
  public MiniDFSCluster(Configuration conf,
                        int numDataNodes,
                        boolean format,
                        String[] racks, String[] hosts) throws IOException {
    this(0, conf, numDataNodes, format, true, true, null, racks, hosts, null);
  }

  /**
   * NOTE: if possible, the other constructors that don't have nameNode port
   * parameter should be used as they will ensure that the servers use free ports.
   * <p>
   * Modify the config and start up the servers.
   *
   * @param nameNodePort suggestion for which rpc port to use.  caller should
   *          use getNameNodePort() to get the actual port used.
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param manageDfsDirs if true, the data directories for servers will be
   *          created and dfs.name.dir and dfs.data.dir will be set in the conf
   * @param operation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   */
  public MiniDFSCluster(int nameNodePort,
                        Configuration conf,
                        int numDataNodes,
                        boolean format,
                        boolean manageDfsDirs,
                        StartupOption operation,
                        String[] racks) throws IOException {
    this(nameNodePort, conf, numDataNodes, format, manageDfsDirs, manageDfsDirs,
         operation, racks, null, null);
  }

  /**
   * NOTE: if possible, the other constructors that don't have nameNode port
   * parameter should be used as they will ensure that the servers use free ports.
   * <p>
   * Modify the config and start up the servers.
   *
   * @param nameNodePort suggestion for which rpc port to use.  caller should
   *          use getNameNodePort() to get the actual port used.
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param manageDfsDirs if true, the data directories for servers will be
   *          created and dfs.name.dir and dfs.data.dir will be set in the conf
   * @param operation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param simulatedCapacities array of capacities of the simulated data nodes
   */
  public MiniDFSCluster(int nameNodePort,
                        Configuration conf,
                        int numDataNodes,
                        boolean format,
                        boolean manageDfsDirs,
                        StartupOption operation,
                        String[] racks,
                        long[] simulatedCapacities) throws IOException {
    this(nameNodePort, conf, numDataNodes, format, manageDfsDirs, manageDfsDirs,
          operation, racks, null, simulatedCapacities);
  }

  public static final String MAPRFS_URI = "maprfs:///";
  /**
   * NOTE: if possible, the other constructors that don't have nameNode port
   * parameter should be used as they will ensure that the servers use free ports.
   * <p>
   * Modify the config and start up the servers.
   *
   * @param nameNodePort suggestion for which rpc port to use.  caller should
   *          use getNameNodePort() to get the actual port used.
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param manageNameDfsDirs if true, the data directories for servers will be
   *          created and dfs.name.dir and dfs.data.dir will be set in the conf
   * @param manageDataDfsDirs if true, the data directories for datanodes will
   *          be created and dfs.data.dir set to same in the conf
   * @param operation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param hosts array of strings indicating the hostnames of each DataNode
   * @param simulatedCapacities array of capacities of the simulated data nodes
   */
  public MiniDFSCluster(int nameNodePort,
                        Configuration conf,
                        int numDataNodes,
                        boolean format,
                        boolean manageNameDfsDirs,
                        boolean manageDataDfsDirs,
                        StartupOption operation,
                        String[] racks, String hosts[],
                        long[] simulatedCapacities) throws IOException {

    this.conf = conf;
    //Use mfs cluster.
    conf.set("fs.default.name", MAPRFS_URI);
    int replication = conf.getInt("dfs.replication", 3);
    conf.setInt("dfs.replication", Math.min(replication, numDataNodes));
    conf.set("fs.maprfs.impl", "com.mapr.fs.MapRFileSystem");
    conf.set("io.file.buffer.size", "65536");  //TODO: other sizes?

    conf.set("dfs.http.address", "127.0.0.1:0");
    //conf.set("fs.mapr.trace", "debug");

    // Hack to handle any test case that failed to shut down the cluter.
    //teardownServices();

    InitNodes("TestVolume", numDataNodes);
    // Start the DataNodes
    startDataNodes(conf, numDataNodes, manageDataDfsDirs,
                    operation, racks, hosts, simulatedCapacities);

    waitClusterUp();
    if (!isClusterUp) {
      throw new IOException("Failed to create mapr cluster");
    }

    //make sure ProxyUsers uses the latest conf
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
  }

  // This is meant for debugging purpose only. Not to be used in normal automated tests as the script is not checked in.
  private void teardownServices() {
    RunCommand rc = new RunCommand();
    String[] cmd = {
      "/bin/sh", "-c", "/usr/local/bin/teardown_mapr.sh"
    };

    rc.Init(cmd, "", false, true);
    rc.Run();
  }

  void InitNodes(String volName, int numNodes)
  {
    this.volName = volName;
    this.numNodes = numNodes;


    nodes = new MapRNode[numNodes];

    for (int i = 0; i < numNodes; ++i) {
      nodes[i] = new MapRNode();
      if (i == 0) {
        nodes[i].Init(true, true, true, i);
      } else {
        nodes[i].Init(false, false, true, i);
      }
    }
  }

  /**
   * wait for the cluster to get out of
   * safemode. max wait 30 mins
   */
  public void waitClusterUp() {
    RunCommand rc = new RunCommand();
    rc.Init(hadoopExe+" fs -lsr /", "", false, false);
    for(int i=0; i < 6; ++i) {
      if (rc.Run() == 0) {
        isClusterUp = true;
        break;
      }
      try {
        Thread.sleep(5*1000);
        System.out.println("Waiting for cluster to come up");
      } catch(InterruptedException e) {
      }
    }
  }

  public void Start() {
    for (int i = 0; i < numNodes; ++i) {
      nodes[i].Start();
    }
  }

  public void Stop() {
    for (int i = 0; i < numNodes; ++i) {
      nodes[i].Stop();
    }
  }

  /**
   * Modify the config and start up additional DataNodes.  The info port for
   * DataNodes is guaranteed to use a free port.
   *
   *  Data nodes can run with the name node in the mini cluster or
   *  a real name node. For example, running with a real name node is useful
   *  when running simulated data nodes with a real name node.
   *  If minicluster's name node is null assume that the conf has been
   *  set with the right address:port of the name node.
   *
   * @param conf the base configuration to use in starting the DataNodes.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param manageDfsDirs if true, the data directories for DataNodes will be
   *          created and dfs.data.dir will be set in the conf
   * @param operation the operation with which to start the DataNodes.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param hosts array of strings indicating the hostnames for each DataNode
   * @param simulatedCapacities array of capacities of the simulated data nodes
   *
   * @throws IllegalStateException if NameNode has been shutdown
   */
  public synchronized void startDataNodes(Configuration conf, int numDataNodes,
                             boolean manageDfsDirs, StartupOption operation,
                             String[] racks, String[] hosts,
                             long[] simulatedCapacities) throws IOException {

    Start();
  }




  /**
   * Modify the config and start up the DataNodes.  The info port for
   * DataNodes is guaranteed to use a free port.
   *
   * @param conf the base configuration to use in starting the DataNodes.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param manageDfsDirs if true, the data directories for DataNodes will be
   *          created and dfs.data.dir will be set in the conf
   * @param operation the operation with which to start the DataNodes.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   *
   * @throws IllegalStateException if NameNode has been shutdown
   */

  public void startDataNodes(Configuration conf, int numDataNodes,
      boolean manageDfsDirs, StartupOption operation,
      String[] racks
      ) throws IOException {
    startDataNodes( conf,  numDataNodes, manageDfsDirs,  operation, racks, null, null);
  }

  /**
   * Modify the config and start up additional DataNodes.  The info port for
   * DataNodes is guaranteed to use a free port.
   *
   *  Data nodes can run with the name node in the mini cluster or
   *  a real name node. For example, running with a real name node is useful
   *  when running simulated data nodes with a real name node.
   *  If minicluster's name node is null assume that the conf has been
   *  set with the right address:port of the name node.
   *
   * @param conf the base configuration to use in starting the DataNodes.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param manageDfsDirs if true, the data directories for DataNodes will be
   *          created and dfs.data.dir will be set in the conf
   * @param operation the operation with which to start the DataNodes.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param simulatedCapacities array of capacities of the simulated data nodes
   *
   * @throws IllegalStateException if NameNode has been shutdown
   */
  public void startDataNodes(Configuration conf, int numDataNodes,
                             boolean manageDfsDirs, StartupOption operation,
                             String[] racks,
                             long[] simulatedCapacities) throws IOException {
    startDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, null,
                   simulatedCapacities);

  }
  /**
   * If the NameNode is running, attempt to finalize a previous upgrade.
   * When this method return, the NameNode should be finalized, but
   * DataNodes may not be since that occurs asynchronously.
   *
   * @throws IllegalStateException if the Namenode is not running.
   */
  public void finalizeCluster(Configuration conf) throws Exception {
    throw new RuntimeException("Unsupported by MapR");
  }

  /**
   * Gets the started NameNode.  May be null.
   */
  public NameNode getNameNode() {
    throw new RuntimeException("Unsupported by MapR");
  }

  /**
   * Gets the NameNode for the index.  May be null.
   */
  public NameNode getNameNode(int nnIndex) {
    throw new RuntimeException("Unsupported by MapR");
  }

  /**
   * Gets a list of the started DataNodes.  May be empty.
   */
  public ArrayList<DataNode> getDataNodes() {
    throw new RuntimeException("Unsupported by MapR");
  }

  /**
   * Gets a list of started MapRNodes.
   */
  public ArrayList<MapRNode> getMapRNodes() {
    ArrayList<MapRNode> list = new ArrayList<MapRNode>();
    for (int i = 0; i < nodes.length; i++) {
      list.add(nodes[i]);
    }
    return list;
  }

  /** @return the datanode having the ipc server listen port */
  public DataNode getDataNode(int ipcPort) {
    throw new RuntimeException("Unsupported by MapR");
  }

  /**
   * Gets the rpc port used by the NameNode, because the caller
   * supplied port is not necessarily the actual port used.
   */
  public int getNameNodePort() {
    return MiniDFSCluster.cldbPort;
  }

  public int getNameNodePort(int i) {
    return getNameNodePort();
  }

  /**
   * Shut down the servers that are up.
   */
  public void shutdown() {
    System.out.println("Shutting down the MiniMapRCluster");
    for (int i = 0; i < numNodes; ++i) {
      nodes[i].Stop();
    }

    for (int i = 0; i < nodes.length; ++i) {
      nodes[i].CleanUp();
    }
  }

  public void Restart() {
    Stop();
    Start();
  }

  /**
   * Shutdown all DataNodes started by this class.  The NameNode
   * is left running so that new DataNodes may be started.
   */
  public void shutdownDataNodes() {
    //0Th node is cldbNode.
    for (int i = 1; i < numNodes; ++i) {
      nodes[i].Stop();
    }
  }

  /**
   * Shutdown namenode.
   */
  public synchronized void shutdownNameNode() {
    nodes[0].Stop();
  }

  public synchronized void shutdownNameNode(int i) {
        shutdownNameNode();
  }

  /**
   * Restart namenode.
   */
  public synchronized void restartNameNode() throws IOException {
    nodes[0].Stop();
    nodes[0].Start();
  }

  /*
   * Corrupt a block on all datanodes
   * This is new interface added for MapR cluster.
   */
  public boolean corruptBlock(String file, long offset)
  {
    boolean retVal = false;
    RunCommand rc = new RunCommand();
    String[] cmd = {
      "/bin/sh",
      "-c",
      MiniDFSCluster.hadoopExe+" mfs -ls "+ file +
      "| (offset=" + offset +
      "; chunkSize=" + chunkSize +
      "; read line; read line; read line; if [ $offset -lt " + clusterSize +
      " ]; then echo $line; exit; fi;" +
      "reqIdx=$[offset/chunkSize];" +
      "i=0;" +
      "while read line; " +
      "do " +
      "if [ $i -eq $reqIdx ]; then echo $line; break; fi; " +
      "i=$[i+1]; " +
      "done)"
    };
    rc.Init(cmd, "", false, true);
    rc.Run();
    if (rc.OutPutStr() == null) {
      // no fid for the given offset
      return false;
    }

    try {
      String tokens[] = rc.OutPutStr().split(" ");
      String fid = tokens[1];
      for (int i=2; i < tokens.length; ++i) {
        String port=tokens[i].split(":")[1];
        int nodeId = Integer.parseInt(port) - defaultMfsPort;
        if (nodeId == 0) {
          //skip CLDB node
          continue;
        }
        Long block = nodes[nodeId].getBlockNumber(fid, offset % chunkSize);
        retVal = nodes[nodeId].corruptBlock(block);
      }
    } catch (Exception e) {
        e.printStackTrace();
    }

    return true;
  }

  /*
   * Corrupt a block on all datanode
   */
  void corruptBlockOnDataNodes(String blockName) throws Exception{
    throw new RuntimeException("Unsupported by MapR");
  }

  public int corruptBlockOnDataNodes(ExtendedBlock block) throws IOException{
    throw new RuntimeException("Unsupported by MapR");
  }

  /*
   * Corrupt a block on a particular datanode
   */
  boolean corruptBlockOnDataNode(int i, String blockName) throws Exception {
    throw new RuntimeException("Unsupported by MapR");
  }


  /*
   * Shutdown a particular datanode
   */
  public synchronized DataNodeProperties stopDataNode(int i) {
    if (i < 1 || i >= nodes.length) {
      return null;
    }
    nodes[i].Stop();
    return null;
  }

  /*
   * Shutdown a datanode by name.
   */
  public synchronized DataNodeProperties stopDataNode(String name) {
    int i;
    for (i = 0; i < nodes.length; i++) {
      if (nodes[i].GetName().equals(name)) {
        break;
      }
    }
    return stopDataNode(i);
  }

  /**
   * Restart a datanode
   * @param dnprop datanode's property
   * @return true if restarting is successful
   * @throws IOException
   */
  public boolean restartDataNode(DataNodeProperties dnprop) throws IOException {
    throw new RuntimeException("Unsupported by MapR");
  }

  /**
   * Restart a datanode, on the same port if requested
   * @param dnprop, the datanode to restart
   * @param keepPort, whether to use the same port
   * @return true if restarting is successful
   * @throws IOException
   */
  public synchronized boolean restartDataNode(DataNodeProperties dnprop,
      boolean keepPort) throws IOException {
    throw new RuntimeException("Unsupported by MapR");
  }

  /*
   * Restart a particular datanode, use newly assigned port
   */
  public boolean restartDataNode(int i) throws IOException {
    return restartDataNode(i, false);
  }

  /*
   * Restart a particular datanode, on the same port if keepPort is true
   */
  public synchronized boolean restartDataNode(int i, boolean keepPort)
      throws IOException {
    nodes[i].Stop();
    nodes[i].Start();
    return true;
  }

  /*
   * Restart all datanodes, on the same ports if keepPort is true
   */
  public synchronized boolean restartDataNodes(boolean keepPort)
      throws IOException {
    //0Th node is cldbNode.
    for (int i = 1; i < numNodes; ++i) {
      nodes[i].Stop();
      nodes[i].Start();
    }
    return true;
  }

  /*
   * Restart all datanodes, use newly assigned ports
   */
  public boolean restartDataNodes() throws IOException {
    return restartDataNodes(false);
  }

  /**
   * Returns true if the NameNode is running and is out of Safe Mode.
   */
  public boolean isClusterUp() {
    return isClusterUp;
  }

  /**
   * Returns true if there is at least one DataNode running.
   */
  public boolean isDataNodeUp() {
    return (nodes[0].State() == NodeState.RUNNING);
  }

  /**
   * Get a client handle to the DFS cluster.
   */
  public FileSystem getFileSystem() throws IOException {
    return FileSystem.get(conf);
  }

  public FileSystem getFileSystem(int i) throws IOException {
    return getFileSystem();
  }

  /**
   * @return a {@link HftpFileSystem} object.
   */
  public HftpFileSystem getHftpFileSystem() throws IOException {
    throw new RuntimeException("Unsupported by MapR");
  }

  public Collection<URI> getNameDirs(int i) {
    throw new RuntimeException("Unsupported by MapR");
  }

  public Collection<URI> getNameEditsDirs(int i) {
    throw new RuntimeException("Unsupported by MapR");
  }

  /**
   * Wait until the cluster is active and running.
   */
  public void waitActive() throws IOException {
    waitActive(true);
  }

  public void waitActive(int nnIndex) throws IOException {
    waitActive();
  }

  /**
   * Wait until the cluster is active.
   * @param waitHeartbeats if true, will wait until all DNs have heartbeat
   */
  public void waitActive(boolean waitHeartbeats) throws IOException {
    waitClusterUp();
    if (!isClusterUp)
      throw new IOException("Failed to create mapr cluster");
  }

  private synchronized boolean shouldWait(DatanodeInfo[] dnInfo,
                                          boolean waitHeartbeats) {
    return false;
  }

  public Configuration getConfiguration(int nnIndex) {
    return this.conf;
  }

  /**
   * Wait for the given datanode to heartbeat once.
   */
  public void waitForDNHeartbeat(int dnIndex, long timeoutMillis) {
  }

  public void formatDataNodeDirs() throws IOException {
  }

  /**
   *
   * @param dataNodeIndex - data node whose block report is desired - the index is same as for getDataNodes()
   * @return the block report for the specified data node
   */
  public Block[] getBlockReport(int dataNodeIndex) {
    throw new RuntimeException("Unsupported by MapR");
  }

  /**
   *
   * @return block reports from all data nodes
   *    Block[] is indexed in the same order as the list of datanodes returned by getDataNodes()
   */
  public Block[][] getAllBlockReports() {
    throw new RuntimeException("Unsupported by MapR");
  }

  public Iterable<Block>[] getAllBlockReports(String bpid) {
    throw new RuntimeException("Unsupported by MapR");
  }


  /**
   * This method is valid only if the data nodes have simulated data
   * @param dataNodeIndex - data node i which to inject - the index is same as for getDataNodes()
   * @param blocksToInject - the blocks
   * @throws IOException
   *              if not simulatedFSDataset
   *             if any of blocks already exist in the data node
   *
   */
  public void injectBlocks(int dataNodeIndex, Block[] blocksToInject) throws IOException {
    throw new RuntimeException("Unsupported by MapR");
  }

  /**
   * This method is valid only if the data nodes have simulated data
   * @param blocksToInject - blocksToInject[] is indexed in the same order as the list
   *             of datanodes returned by getDataNodes()
   * @throws IOException
   *             if not simulatedFSDataset
   *             if any of blocks already exist in the data nodes
   *             Note the rest of the blocks are not injected.
   */
  public void injectBlocks(Block[][] blocksToInject) throws IOException {
  }

  public void injectBlocks(int nameNodeIndex, int dataNodeIndex,
      Iterable<Block> blocksToInject) throws IOException {
  }

  public void injectBlocks(Iterable<Block>[] blocksToInject)
      throws IOException {
  }

  /**
   * Set the softLimit and hardLimit of client lease periods
   */
  void setLeasePeriod(long soft, long hard) {
  }

  public void setLeasePeriod(long soft, long hard, int nnIndex) {
  }

  /**
   * Returns the current set of datanodes
   */
  DataNode[] listDataNodes() {
    throw new RuntimeException("Unsupported by MapR");
  }

  /**
   * Access to the data directory used for Datanodes
   * @throws IOException
   */
  public String getDataDirectory() {
    throw new RuntimeException("Unsupported by MapR");
  }

  public static File getBaseDir() {
    throw new RuntimeException("Unsupported by MapR");
  }

  public NamenodeProtocols getNameNodeRpc() {
    throw new RuntimeException("Unsupported by MapR");
  }

  public NamenodeProtocols getNameNodeRpc(int i) {
    throw new RuntimeException("Unsupported by MapR");
  }

  public FSNamesystem getNamesystem() {
    throw new RuntimeException("Unsupported by MapR");
  }

  public FSNamesystem getNamesystem(int i) {
    throw new RuntimeException("Unsupported by MapR");
  }

  public URI getSharedEditsDir(int minNN, int maxNN) throws IOException {
    throw new RuntimeException("Unsupported by MapR");
  }

  public File getInstanceStorageDir(int dnIndex, int dirIndex) {
    throw new RuntimeException("Unsupported by MapR");
  }

  public void transitionToActive(int nnIndex) throws IOException, ServiceFailedException {
    throw new RuntimeException("Unsupported by MapR");
  }

  /**
   * Get finalized directory for a block pool
   * @param storageDir storage directory
   * @param bpid Block pool Id
   * @return finalized directory for a block pool
   */
  public static File getFinalizedDir(File storageDir, String bpid) {
    throw new RuntimeException("Unsupported by MapR");
  }

  public synchronized void restartNameNodes() throws IOException {
      restartNameNode();
  }

  /**
   * Restart the namenode. Optionally wait for the cluster to become active.
   */
  public synchronized void restartNameNode(boolean waitActive) throws IOException {
      restartNameNode();
  }

  /**
   * Restart the namenode at a given index.
   */
  public synchronized void restartNameNode(int nnIndex) throws IOException {
      restartNameNode();
  }

  /**
   * Restart the namenode at a given index. Optionally wait for the cluster
   * to become active.
   */
  public synchronized void restartNameNode(int nnIndex, boolean waitActive) throws IOException {
      restartNameNode();
  }


  public void transitionToStandby(int nnIndex) throws IOException, ServiceFailedException {
  }

  /**
   * Shutdown all the namenodes.
   */
  public synchronized void shutdownNameNodes() {
      shutdownNameNode();
  }

  public void triggerHeartbeats() throws IOException {
    for (DataNode dn : getDataNodes()) {
      DataNodeTestUtils.triggerHeartbeat(dn);
    }
  }

  public void triggerBlockReports() throws IOException {
    for (DataNode dn : getDataNodes()) {
      DataNodeTestUtils.triggerBlockReport(dn);
    }
  }

  public void triggerDeletionReports() throws IOException {
    for (DataNode dn : getDataNodes()) {
      DataNodeTestUtils.triggerDeletionReport(dn);
    }
  }

  /**
   * Corrupt a block on a particular datanode.
   *
   * @param i index of the datanode
   * @param blk name of the block
   * @throws IOException on error accessing the given block or if
   * the contents of the block (on the same datanode) differ.
   * @return true if a replica was corrupted, false otherwise
   * Types: delete, write bad data, truncate
   */
  public static boolean corruptReplica(int i, ExtendedBlock blk)
      throws IOException {
    File blockFile = getBlockFile(i, blk);
    // TODO Implement the corrupt logic.
    return true;
  }

  /**
   * Get file correpsonding to a block
   * @param storageDir storage directory
   * @param blk the block
   * @return data file corresponding to the block
   */
  public static File getBlockFile(File storageDir, ExtendedBlock blk) {
    return new File(getFinalizedDir(storageDir, blk.getBlockPoolId()),
        blk.getBlockName());
  }

  /**
   * Get the block data file for a block from a given datanode
   * @param dnIndex Index of the datanode to get block files for
   * @param block block for which corresponding files are needed
   */
  public static File getBlockFile(int dnIndex, ExtendedBlock block) {
    // Check for block file in the two storage directories of the datanode
    for (int i = 0; i <=1 ; i++) {
      File storageDir = MiniDFSCluster.getStorageDir(dnIndex, i);
      File blockFile = getBlockFile(storageDir, block);
      if (blockFile.exists()) {
        return blockFile;
      }
    }
    return null;
  }

  /**
   * Get a storage directory for a datanode. There are two storage directories
   * per datanode:
   * <ol>
   * <li><base directory>/data/data<2*dnIndex + 1></li>
   * <li><base directory>/data/data<2*dnIndex + 2></li>
   * </ol>
   *
   * @param dnIndex datanode index (starts from 0)
   * @param dirIndex directory index (0 or 1). Index 0 provides access to the
   *          first storage directory. Index 1 provides access to the second
   *          storage directory.
   * @return Storage directory
   */
  public static File getStorageDir(int dnIndex, int dirIndex) {
    return new File(getBaseDirectory(), getStorageDirPath(dnIndex, dirIndex));
  }

  /**
   * Get the base directory for any DFS cluster whose configuration does
   * not explicitly set it. This is done by retrieving the system property
   * {@link #PROP_TEST_BUILD_DATA} (defaulting to "build/test/data" ),
   * and returning that directory with a subdir of /dfs.
   * @return a directory for use as a miniDFS filesystem.
   */
  public static String getBaseDirectory() {
    return System.getProperty(PROP_TEST_BUILD_DATA, "build/test/data") + "/dfs/";
  }

  /**
   * Calculate the DN instance-specific path for appending to the base dir
   * to determine the location of the storage of a DN instance in the mini cluster
   * @param dnIndex datanode index
   * @param dirIndex directory index (0 or 1).
   * @return
   */
  private static String getStorageDirPath(int dnIndex, int dirIndex) {
    return "data/data" + (2 * dnIndex + 1 + dirIndex);
  }

  public String readBlockOnDataNode(int i, ExtendedBlock block)
      throws IOException {
    throw new RuntimeException("Unsupported by MapR");
  }

  public NameNodeInfo[] getNameNodeInfos() {
    throw new RuntimeException("Unsupported by MapR");
  }

  /**
   * @return URI of the namenode from a single namenode MiniDFSCluster
   */
  public URI getURI() {
    throw new RuntimeException("Unsupported by MapR");
  }

  /**
   * @return URI of the given namenode in MiniDFSCluster
   */
  public URI getURI(int nnIndex) {
    throw new RuntimeException("Unsupported by MapR");
  }

  public int getNumNameNodes() {
    return 1;
  }

  public File[] getAllBlockFiles(ExtendedBlock block) {
    throw new RuntimeException("Unsupported by MapR");
  }

  public int getInstanceId() {
    return 1;
  }
}
