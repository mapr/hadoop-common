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
package org.apache.hadoop.ipc;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.MapRModified;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;

@MapRModified(summary = "Used for Job tracker HA")
/**
 * All the code except FailoverInvoker and getProxy methods are taken from
 * {@link WritableRpcEngine}. The FailoverInvoker is used instead of the default
 * Invoker to figure out the job tracker server that is currently active.
 *
 * At the time of writing, this code is only used by MR1. So instead of
 * changing WritableRpcEngine to make it extensible and take a custom Invoker
 * as input, we choose to copy over the code as its fairly small.
 */
public class FailoverRPC {
  private static final Log LOG = LogFactory.getLog(FailoverRPC.class);

  private static ClientCache CLIENTS=new ClientCache();

  // 2L - added declared class to Invocation
  public static final long writableRpcVersion = 2L;

  /** A method invocation, including the method name and its parameters.*/
  private static class Invocation implements Writable, Configurable {
    private String methodName;
    private Class<?>[] parameterClasses;
    private Object[] parameters;
    private Configuration conf;
    private long clientVersion;
    private int clientMethodsHash;
    private String declaringClassProtocolName;

    //This could be different from static writableRpcVersion when received
    //at server, if client is using a different version.
    private long rpcVersion;

    @SuppressWarnings("unused") // called when deserializing an invocation
    public Invocation() {}

    public Invocation(Method method, Object[] parameters) {
      this.methodName = method.getName();
      this.parameterClasses = method.getParameterTypes();
      this.parameters = parameters;
      rpcVersion = writableRpcVersion;
      if (method.getDeclaringClass().equals(VersionedProtocol.class)) {
        //VersionedProtocol is exempted from version check.
        clientVersion = 0;
        clientMethodsHash = 0;
      } else {
        this.clientVersion = RPC.getProtocolVersion(method.getDeclaringClass());
        this.clientMethodsHash = ProtocolSignature.getFingerprint(method
            .getDeclaringClass().getMethods());
      }
      this.declaringClassProtocolName =
          RPC.getProtocolName(method.getDeclaringClass());
    }

    /** The name of the method invoked. */
    public String getMethodName() { return methodName; }

    /** The parameter classes. */
    public Class<?>[] getParameterClasses() { return parameterClasses; }

    /** The parameter instances. */
    public Object[] getParameters() { return parameters; }

    private long getProtocolVersion() {
      return clientVersion;
    }

    @SuppressWarnings("unused")
    private int getClientMethodsHash() {
      return clientMethodsHash;
    }

    /**
     * Returns the rpc version used by the client.
     * @return rpcVersion
     */
    public long getRpcVersion() {
      return rpcVersion;
    }

    @Override
    @SuppressWarnings("deprecation")
    public void readFields(DataInput in) throws IOException {
      rpcVersion = in.readLong();
      declaringClassProtocolName = UTF8.readString(in);
      methodName = UTF8.readString(in);
      clientVersion = in.readLong();
      clientMethodsHash = in.readInt();
      parameters = new Object[in.readInt()];
      parameterClasses = new Class[parameters.length];
      ObjectWritable objectWritable = new ObjectWritable();
      for (int i = 0; i < parameters.length; i++) {
        parameters[i] =
            ObjectWritable.readObject(in, objectWritable, this.conf);
        parameterClasses[i] = objectWritable.getDeclaredClass();
      }
    }

    @Override
    @SuppressWarnings("deprecation")
    public void write(DataOutput out) throws IOException {
      out.writeLong(rpcVersion);
      UTF8.writeString(out, declaringClassProtocolName);
      UTF8.writeString(out, methodName);
      out.writeLong(clientVersion);
      out.writeInt(clientMethodsHash);
      out.writeInt(parameterClasses.length);
      for (int i = 0; i < parameterClasses.length; i++) {
        ObjectWritable.writeObject(out, parameters[i], parameterClasses[i],
                                   conf, true);
      }
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append(methodName);
      buffer.append("(");
      for (int i = 0; i < parameters.length; i++) {
        if (i != 0)
          buffer.append(", ");
        buffer.append(parameters[i]);
      }
      buffer.append(")");
      buffer.append(", rpc version="+rpcVersion);
      buffer.append(", client version="+clientVersion);
      buffer.append(", methodsFingerPrint="+clientMethodsHash);
      return buffer.toString();
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Configuration getConf() {
      return this.conf;
    }

  }

  private static class Invoker implements RpcInvocationHandler, Closeable {
    private Client.ConnectionId remoteId;
    private Client client;
    private boolean isClosed = false;

    public Invoker(Class<?> protocol,
                   InetSocketAddress address, UserGroupInformation ticket,
                   Configuration conf, SocketFactory factory,
                   int rpcTimeout) throws IOException {
      this.remoteId = Client.ConnectionId.getConnectionId(address, protocol,
          ticket, rpcTimeout, conf);
      this.client = CLIENTS.getClient(conf, factory);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
      long startTime = 0;
      if (LOG.isDebugEnabled()) {
        startTime = Time.now();
      }

      ObjectWritable value = (ObjectWritable)
        client.call(RPC.RpcKind.RPC_WRITABLE, new Invocation(method, args), remoteId);
      if (LOG.isDebugEnabled()) {
        long callTime = Time.now() - startTime;
        LOG.debug("Call: " + method.getName() + " " + callTime);
      }
      return value.get();
    }

    /* close the IPC client that's responsible for this invoker's RPCs */
    @Override
    synchronized public void close() {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }

    @Override
    public ConnectionId getConnectionId() {
      return remoteId;
    }
  }

  private static class FailoverInvoker implements InvocationHandler, Closeable {
    private InetSocketAddress[] addresses;
    private UserGroupInformation ticket;
    private Client client;
    private boolean isClosed = false;
    private Configuration conf;
    Class<?> protocol;
    long clientVersion;
    SocketFactory factory;
    int activeServer, lastActiveServer, totalServers;
    FileSystem fs = null;
    boolean usefs = false;
    private boolean firstAttempt = true;
    private int maxFirstTimeAttempts = 20; // 20 mins by default

    public FailoverInvoker(Class<?> protocol,
        long clientVersion,
        FileSystem fs,
        UserGroupInformation ticket, Configuration conf,
        SocketFactory factory) throws IOException {
      this.protocol = protocol;
      this.clientVersion = clientVersion;
      this.ticket = ticket;
      this.client = CLIENTS.getClient(conf, factory);
      this.conf = conf;
      this.factory = factory;
      this.activeServer = 0;
      this.lastActiveServer = 0;
      if (fs != null) {
        this.fs = fs;
      } else {
        this.fs = FileSystem.get(conf);
      }
      this.addresses = null;
      this.totalServers = 0;
      this.usefs = true;
      this.maxFirstTimeAttempts =
        conf.getInt("ipc.client.max.connection.setup.timeout",  20);
      // first 2 mins there are 15 attempts
      if (this.maxFirstTimeAttempts <= 1) {
        this.maxFirstTimeAttempts = 10;
      } else if (this.maxFirstTimeAttempts == 2) {
        this.maxFirstTimeAttempts = 15;
      } else {
        // take out 2 mins and after that every 30s one attempt
        this.maxFirstTimeAttempts = 2*(this.maxFirstTimeAttempts - 2) + 15;
      }
    }

    /* Setting jt's ip addr using mapred-site.xml*/
    /** findActiveServer: called by Failover Proxy to find active server */
    public synchronized void searchActiverServer() throws IOException {
      Object quickProxy;
      Invoker quickInvoker;
      final boolean logInfo = LOG.isInfoEnabled();
      if (logInfo)
        LOG.info("Searching for the Active Server ...");
      boolean found  = false;
      long attempts = 1;
      while (!found) {
        /* create a failover proxy first. It is used to find new server */
        if (logInfo)
          LOG.info("Attempt# " + attempts + " . Trying to connect Server at " +
              addresses[activeServer]);
        // Timeout quickly here (current is 10s which is default connection timeout)
        int maxIdleTime = conf.getInt("ipc.client.connection.maxidletime", 10000); // 10s
        int maxRetries = conf.getInt("ipc.client.connect.max.retries", 10);
        // 1 retry and 10 ms timeout
        conf.setInt("ipc.client.connection.maxidletime", 10);
        conf.setInt("ipc.client.connect.max.retries", 1);
        quickInvoker = new Invoker(protocol, addresses[activeServer], ticket, conf, factory, 0);
        // set original values
        conf.setInt("ipc.client.connection.maxidletime", maxIdleTime);
        conf.setInt("ipc.client.connect.max.retries", maxRetries);
        quickProxy = Proxy.newProxyInstance(
            protocol.getClassLoader(), new Class[] { protocol }, quickInvoker);
        if (quickProxy instanceof VersionedProtocol) {
          try {
            long serverVersion = ((VersionedProtocol)quickProxy)
              .getProtocolVersion(protocol.getName(), clientVersion);
            if (serverVersion != clientVersion) {
              LOG.warn("Version mistmatch while searching for the Active Server");
            } else {
              found = true;
            }
          } catch (IOException e) {
            LOG.warn("Error connecting server at " + addresses[activeServer]
                     + " " + e);
          }
        } else {
          LOG.error("Not a versioned protocol?");
        }
        //quickClient.reInitTimeOuts();
        quickInvoker.close();
        quickProxy = null;
        quickInvoker = null;
        if (found == false) {
          /*
          if (useZookeeper) {
            // we may get job tracker address but jobtracker is not yet ready
            // to receive rpcs hence retry.
            try {
              //sleep -> 2, 4, 6, 8 ... 28, 30, 30..
              if ((attempts * 2) > 30) {
                Thread.currentThread().sleep(30*1000);
              } else {
                Thread.currentThread().sleep(attempts*2*1000);
              }
            } catch (InterruptedException e) {
            }
            attempts++;
            addresses[activeServer] = jtw.findJobTrackerAddr();
          }
          */
          // move on to next server address
          activeServer = (activeServer + 1) % totalServers;
          if (activeServer == lastActiveServer) {
            if (logInfo)
              LOG.info("Tried all servers sleeping");
            try {
              //sleep -> 2, 4, 6, 8 ... 28, 30, 30..
              if ((attempts * 2) > 30) {
                Thread.currentThread().sleep(30*1000);
              } else {
                Thread.currentThread().sleep(attempts*2*1000);
              }
            } catch (InterruptedException e) {
            }
            // we have tried to all servers for last 5 mins and this is the very first attempt to talk to JT
            if (firstAttempt && attempts >= maxFirstTimeAttempts) {
              throw new IOException(
                  "Failed to establish initial contact with all servers. " +
                  "mapred.job.tracker = " + conf.get("mapred.job.tracker",
                                              "maprdummy"));
            }
            attempts++;
            // fetch jt address again
            if (usefs) {
              try {
                addresses = fs.getJobTrackerAddrs(conf);
                totalServers = addresses.length;
                activeServer = 0;
                lastActiveServer = 0;
              } catch (IOException ioe) {
                LOG.error("Error while fetching JobTracker location " + ioe);
                throw ioe;
              }
            }
          }
        }
      }
      firstAttempt = false;
      lastActiveServer = activeServer;
      if (logInfo)
        LOG.info("New Active server found on " + addresses[activeServer]);
    }

    public Object invoke(Object proxy, Method method, Object[] args)
    throws Throwable {
      boolean done = false;
      ObjectWritable value = null;
      final boolean logDebug = LOG.isDebugEnabled();
      final boolean logInfo = LOG.isInfoEnabled();
      long startTime = 0;
      if (logDebug) {
        startTime = System.currentTimeMillis();
      }

      if (usefs && addresses == null) {
        // MapR bug 4792 synchronize this block
        synchronized (this) {
          try {
            addresses = fs.getJobTrackerAddrs(conf);
            totalServers = addresses.length;
            activeServer = 0;
            lastActiveServer = 0;
          } catch (IOException ioe) {
            LOG.error("FailoverProxy: Failing this Call: "+ method.getName() +
                      ". Error while fetching JobTracker location " + ioe);
            throw ioe;
          }
        }
      }

      while(!done) {
        try {
          value = (ObjectWritable) client.call(RPC.RpcKind.RPC_WRITABLE, new Invocation(method, args),
              addresses[activeServer], protocol, ticket, 0, conf);
          done = true;
          // no need to lock anything here
          firstAttempt = false;
        } catch (IOException exception) {
          Throwable cause = exception.getCause();
          if (exception instanceof SocketTimeoutException ||
              exception instanceof EOFException ||
              exception instanceof SocketException ||
              exception instanceof ConnectException ||
              cause instanceof SocketTimeoutException ||
              cause instanceof EOFException ||
              cause instanceof SocketException ||
              cause instanceof ConnectException ||
              (cause != null && cause.getMessage() != null
                  && cause.getMessage().equals("Connection reset by peer"))) {

            // JT Fail Over
            if (logInfo)
              LOG.info("FailoverProxy: Server on " + addresses[activeServer]
                       + " is lost due to " + exception +
                       " in call " + method.getName());
            searchActiverServer();
          } else {
            LOG.error("FailoverProxy: Failing this Call: "+ method.getName() +
                      " for error(RemoteException): "+ exception);
            throw exception;
          }
        }
      }
      if (logDebug) {
        long callTime = System.currentTimeMillis() - startTime;
        LOG.debug("Call: " + method.getName() + " " + callTime);
      }
      return value.get();
    }

    /* close the IPC client that's responsible for this invoker's RPCs */
    synchronized public void close() {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
        /*
        if (jtw != null)
          JTWATCHERS.releaseWatcher(jtw);
        */
      }
    }
  }

  public static VersionedProtocol getProxy(
      Class<? extends VersionedProtocol> protocol,
      long clientVersion, FileSystem fs, Configuration conf)
      throws IOException {
        return getProxy(protocol, clientVersion, fs, conf, NetUtils
            .getDefaultSocketFactory(conf));
  }

  public static VersionedProtocol getProxy(
      Class<? extends VersionedProtocol> protocol,
      long clientVersion, FileSystem fs, Configuration conf,
      SocketFactory factory) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return getProxy(protocol, clientVersion, fs, ugi, conf, factory);
  }

  // Failover version of getProxy using FileSystem
  public static VersionedProtocol getProxy(
      // Cannot use extends VersionedProtocol as RefreshUserMappingsProtocol has
      // been changed in Hadoop2 to not extend it.
      //Class<? extends VersionedProtocol> protocol,
      Class<?> protocol,
      long clientVersion, FileSystem fs, UserGroupInformation ticket,
      Configuration conf, SocketFactory factory) throws IOException {

    if (UserGroupInformation.isSecurityEnabled()) {
      SaslRpcServer.init(conf);
    }

    FailoverInvoker failoverInvoker;
    failoverInvoker = new FailoverInvoker(protocol, clientVersion,
                                          fs, ticket, conf, factory);

    VersionedProtocol proxy = (VersionedProtocol) Proxy.newProxyInstance(
        protocol.getClassLoader(), new Class[] { protocol },
        failoverInvoker);
    return proxy;
  }
}
