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
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.security.authorize.ProxyUsers;

enum NodeState {
  STOPPED, RUNNING
}

class MapRNode {
  public static final String ZOOKEEPER_HOME="/opt/mapr/zookeeper/zookeeper-3.4.5";

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

  public void init(
    boolean isCldb, boolean isZookeeper, boolean isFileServer, int nodeId) {

    this.isCldb = isCldb;
    this.isZookeeper = isZookeeper;
    this.isFileServer = isFileServer;
    this.nodeId = nodeId;
    format = true;

    this.port = MiniMapRFSCluster.defaultMfsPort + nodeId;
    this.diskName = "/tmp/disk" + this.port + ".img";
    this.logFile = MiniMapRFSCluster.installDir + "/logs/mfs." + this.port + ".log";
    this.hostnameFile = MiniMapRFSCluster.installDir + "/hostname" + this.port;
    this.hostIdFile = MiniMapRFSCluster.installDir + "/hostid" + this.port;

    this.state = NodeState.STOPPED;

    try {
      this.localhost = InetAddress.getLocalHost().getHostAddress();
    } catch (Exception e) {
      e.printStackTrace();
    }

    String[] commands[] = {
      {"/bin/sh", "-c", "dd bs=8192 seek=1048584 count=1 if=/dev/zero of=" + this.diskName},
      {"/bin/sh", "-c", "echo host-" + this.port + " > " + this.hostnameFile},
      {"/bin/sh", "-c", MiniMapRFSCluster.mruuidgen + " > " + this.hostIdFile}
    };

    RunCommand rc = new RunCommand();
    for (int i = 0; i < commands.length; ++i) {
      rc.init(commands[i], "", false, false);
      rc.Run();
    }
    if (isCldb) {
      String[] cldbCommands[] = {
        {"/bin/sh", "-c", "/bin/hostname --fqdn > " + this.hostnameFile},
        {"/bin/sh", "-c", "cp " + this.hostnameFile + " " + MiniMapRFSCluster.installDir + "/hostname"},
        {"/bin/sh", "-c", "cp " + this.hostIdFile + " " + MiniMapRFSCluster.installDir + "/hostid"},
        {"/bin/sh", "-c", "echo my.cluster.com " + localhost + ":" + MiniMapRFSCluster.cldbPort + " > " + MiniMapRFSCluster.maprClustersFile}
      };
      for (int i = 0; i < cldbCommands.length; ++i) {
        rc.init(cldbCommands[i], "", false, false);
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
      rc.init(commands[i], "", false, false);
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
    rc.init(MiniMapRFSCluster.maprCli + " volume modify -name mapr.cldb.internal -minreplication 1", "", false, false);
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
    rc.init("/opt/mapr/server/mrconfig -p " + port + " sp list", "", false,
      false);
    rc.Run();
    return 0;

  }

  public int MountTheDisk() {
    RunCommand rc = new RunCommand();
    rc.init(MiniMapRFSCluster.testConfigPy + " -h " + localhost + " -p " + port + " -m single -d " + diskName, "", false, false);
    rc.Run();
    return 0;
  }

  int PrepareTheDisk() {
    RunCommand rc = new RunCommand();
    rc.init(MiniMapRFSCluster.testConfigPy + " -h " + localhost + " -p " + port + " -P -s 8192 -d " + diskName, "", false, false);
    return rc.Run();
  }

  public int StartFileServer() {
    RunCommand rc = new RunCommand();
    rc.init(MiniMapRFSCluster.mfsExe + " -e -p "+port+" -m 512 both -h " + hostnameFile + " -H " +hostIdFile + " -L " + logFile, "", true, false);
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
    rc.init(MiniMapRFSCluster.cldbInitScript + " start", "", false, false);
    return rc.Run();
  }

  int StopCldb() {

    if (!isCldb) {
      return 0;
    }

    RunCommand rc = new RunCommand();
    rc.init(MiniMapRFSCluster.cldbInitScript + " stop", "", false, false);
    return rc.Run();
  }

  int StartZookeeper() {
    if (!isZookeeper) {
      return 0;
    }

    String commands[] = {
      ZOOKEEPER_HOME + "/bin/zkServer.sh start"
    };

    RunCommand rc = new RunCommand();
    for (int i = 0; i < commands.length; ++i) {
      rc.init(commands[i], "", false, false);
      rc.Run();
    }
    return 0;
  }

  int CleanUpZookeeper() {
    if (!isZookeeper) {
      return 0;
    }

    String commands[] = {
      ZOOKEEPER_HOME + "/bin/zkCli.sh -server localhost:5181 delete /datacenter/controlnodes/cldb/active/CLDBMaster",
      ZOOKEEPER_HOME + "/bin/zkCli.sh -server localhost:5181 delete /datacenter/controlnodes/cldb/epoch/1/KvStoreContainerInfo"
    };

    RunCommand rc = new RunCommand();
    for (int i = 0; i < commands.length; ++i) {
      rc.init(commands[i], "", false, false);
      rc.Run();
    }
    return 0;
  }

  int StopZookeeper() {
    if (!isZookeeper) {
      return 0;
    }

    String commands[] = {
      ZOOKEEPER_HOME + "/bin/zkServer.sh stop"
    };

    RunCommand rc = new RunCommand();
    for (int i = 0; i < commands.length; ++i) {
      rc.init(commands[i], "", false, false);
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
      MiniMapRFSCluster.mfsdbFile + " " + diskName + " -c \"fid blocknum " +
      fid+"."+offset+"\""
    };

    rc.init(cmd, "", false, true);
    rc.Run();
    Start();
    return Long.parseLong(rc.OutPutStr());
  }

  boolean corruptBlock(long blockNum) throws Exception
  {

    Stop();
    Random random = new Random();
    String badString = "BADBAD";
    int rand = random.nextInt(MiniMapRFSCluster.blockSize/2);
    boolean corrupted = false;
    RunCommand rc = new RunCommand();
    String[] cmd = {
      "/bin/sh", "-c",
      MiniMapRFSCluster.mfsdbFile + " " + diskName + " -c \"write " + blockNum +
      " " + rand + " " + badString+ " string "+ " \" "
    };

    rc.init(cmd, "", false, false);
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

  public void init(String command, String args, boolean isBG,
    boolean reqOutput) {

    this.singleCommand = command;
    this.args = args;
    this.isBG = isBG;
    this.reqOutput = reqOutput;
  }

  public void init(String[] command, String args, boolean isBG,
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

public class MiniMapRFSCluster extends MiniDFSCluster {
  public static final String MAPRFS_SCHEME = "maprfs:///";

  static String installDir="/opt/mapr";
  static String tmpPath="/tmp/mapr-scratch/";
  static String mfsExe=installDir+"/server/mfs";
  static String hadoopExe=installDir+"/hadoop/hadoop-common-2.3.0/bin/hadoop";
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
   * Used by builder to create and return an instance of MiniMapRFSCluster
   */
  protected MiniMapRFSCluster(Builder builder) throws IOException {
    // This is a temp workaround for tests that do not fail gracefully and end
    // up leaving MapR processes running. For e.g., CLDB, MFS, etc. Until we
    // figure out why the cleanup does not happen, this will be useful.
    teardownServices();

    this.conf = builder.conf;
    //Use mfs cluster.
    conf.set("fs.default.name", MAPRFS_SCHEME);
    int replication = conf.getInt("dfs.replication", 3);
    conf.setInt("dfs.replication", Math.min(replication, builder.numDataNodes));
    conf.set("fs.maprfs.impl", "com.mapr.fs.MapRFileSystem");
    conf.set("fs.AbstractFileSystem.maprfs.impl", "com.mapr.fs.MFS");
    conf.set("io.file.buffer.size", "65536");  //TODO: other sizes?

    conf.set("dfs.http.address", "127.0.0.1:0");
    //conf.set("fs.mapr.trace", "debug");

    // HDFS tests can create a cluster with 0 data nodes as the name node
    // initialization is not tied to it. But in our case, CLDB and other
    // services are started as part of the node. So default to 1 data node.
    if (builder.numDataNodes == 0) {
      builder.numDataNodes = 1;
    }
    initNodes("TestVolume", builder.numDataNodes);
    // Start the DataNodes
    startDataNodes(conf, builder.numDataNodes, builder.manageDataDfsDirs,
                    builder.option, builder.racks, builder.hosts, builder.simulatedCapacities);

    waitClusterUp();
    if (!isClusterUp) {
      throw new IOException("Failed to create mapr cluster");
    }

    //make sure ProxyUsers uses the latest conf
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
  }

  /**
   * Terminates all MapR related processes.
   */
  private void teardownServices() {
    String commands[] = {
      "/opt/mapr/cldb/cldb stop",
      MapRNode.ZOOKEEPER_HOME + "/bin/zkServer.sh stop",
      "pkill -9 mfs",
      "ps -ef | grep QuorumPeerMain | grep -v grep | awk '{print $2}' | xargs --no-run-if-empty kill -9",
      "ps -ef | grep FsShell | grep -v grep | awk '{print $2}' | xargs --no-run-if-empty kill -9"
    };

    RunCommand rc = new RunCommand();
    for (int i = 0; i < commands.length; ++i) {
      rc.init(commands[i], "", false, false);
      rc.Run();
    }
  }

  void initNodes(String volName, int numNodes)
  {
    this.volName = volName;
    this.numNodes = numNodes;


    nodes = new MapRNode[numNodes];

    for (int i = 0; i < numNodes; ++i) {
      nodes[i] = new MapRNode();
      if (i == 0) {
        nodes[i].init(true, true, true, i);
      } else {
        nodes[i].init(false, false, true, i);
      }
    }
  }

  /**
   * wait for the cluster to get out of
   * safemode. max wait 30 mins
   */
  public void waitClusterUp() {
    RunCommand rc = new RunCommand();
    rc.init(hadoopExe+" fs -lsr /", "", false, false);
    for(int i=0; i < 6; ++i) {
      if (rc.Run() == 0) {
        isClusterUp = true;
        break;
      }

      try {
        System.out.println("Waiting for cluster to come up");
        Thread.sleep(5*1000);
      } catch (InterruptedException e) {
        System.out.println("Got interrupted while wating for cluster to come up");
        return;
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
   * {@inheritDoc}
   * @see MiniDFSCluster#startDataNodes(Configuration,int,boolean,StartupOption,String[],String[],long[])
   */
  public synchronized void startDataNodes(Configuration conf, int numDataNodes,
      boolean manageDfsDirs, StartupOption operation,
      String[] racks, String[] hosts,
      long[] simulatedCapacities) throws IOException {

    Start();
  }

  /**
   * {@inheritDoc}
   * @see MiniDFSCluster#startDataNodes(Configuration,int,boolean,StartupOption,String[])
   */
  public synchronized void startDataNodes(Configuration conf, int numDataNodes,
      boolean manageDfsDirs, StartupOption operation, String[] racks
      ) throws IOException {

    startDataNodes( conf,  numDataNodes, manageDfsDirs,  operation, racks, null, null);
  }

  /**
   * {@inheritDoc}
   * @see MiniDFSCluster#startDataNodes(Configuration,int,boolean,StartupOption,String[],long[])
   */
  public synchronized void startDataNodes(Configuration conf, int numDataNodes,
      boolean manageDfsDirs, StartupOption operation,
      String[] racks, long[] simulatedCapacities) throws IOException {

    startDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, null,
                   simulatedCapacities);
  }

  public synchronized void startDataNodes(Configuration conf, int numDataNodes,
      boolean manageDfsDirs, StartupOption operation,
      String[] racks, String[] hosts,
      long[] simulatedCapacities,
      boolean setupHostsFile) throws IOException {

    Start();
  }

  public synchronized void startDataNodes(Configuration conf, int numDataNodes,
      boolean manageDfsDirs, StartupOption operation,
      String[] racks, String[] hosts,
      long[] simulatedCapacities,
      boolean setupHostsFile,
      boolean checkDataNodeAddrConfig,
      boolean checkDataNodeHostConfig) throws IOException {

    Start();
  }

  public synchronized void startDataNodes(Configuration conf, int numDataNodes,
      boolean manageDfsDirs, StartupOption operation,
      String[] racks, String[] hosts,
      long[] simulatedCapacities,
      boolean setupHostsFile,
      boolean checkDataNodeAddrConfig) throws IOException {

    Start();
  }

  /**
   * Shut down the servers that are up.
   */
  public void doShutdown() {
    for (int i = 0; i < numNodes; ++i) {
      nodes[i].Stop();
    }

    for (int i = 0; i < nodes.length; ++i) {
      nodes[i].CleanUp();
    }
  }

  public void shutdownDataNodes() {
    //0Th node is cldbNode.
    for (int i = 1; i < numNodes; ++i) {
        nodes[i].Stop();
    }
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
      MiniMapRFSCluster.hadoopExe+" mfs -ls "+ file +
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
    rc.init(cmd, "", false, true);
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

  public synchronized Object stopDataNode(int i) {
    if (i < 1 || i >= nodes.length) {
        return null;
    }

    nodes[i].Stop();
    return null;
  }

  /*
   * Shutdown a datanode by name.
   */
  public synchronized Object stopDataNode(String name) {
    int i;
    for (i = 0; i < nodes.length; i++) {
      if (nodes[i].GetName().equals(name)) {
        break;
      }
    }
    return stopDataNode(i);
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
   * Wait until the cluster is active and running.
   */
  public void waitActive() throws IOException {
    waitActive(true);
  }

  /**
   * Wait until the cluster is active.
   * @param waitHeartbeats if true, will wait until all DNs have heartbeat
   */
  public void waitActive(boolean waitHeartbeats) throws IOException {
    waitClusterUp();
    if (!isClusterUp) {
      throw new IOException("Failed to create mapr cluster");
    }
  }

  public Configuration getConfiguration(int nnIndex) {
    return this.conf;
  }

  public void formatDataNodeDirs() throws IOException {
  }

  /**
   * Returns the URI of CLDB node. This is the first node in <code>nodes</code>.
   */
  public URI getURI() {
    URI uri = null;
    try {
      uri = new URI(MAPRFS_SCHEME + nodes[0].localhost + ":" + nodes[0].port);
    } catch (URISyntaxException e) {
      LOG.warn("Unexpected URISyntaxException: ", e);
    }

    return uri;
  }

  public int getNameNodePort() {
    return cldbPort;
  }

}
