/*
 * Copyright 2011 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.security;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Master;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

import azkaban.security.commons.HadoopSecurityManager;
import azkaban.security.commons.HadoopSecurityManagerException;
import azkaban.utils.Props;
import azkaban.utils.UndefinedPropertyException;

public class HadoopSecurityManager_H_2_0 extends HadoopSecurityManager {

  private static final String FS_HDFS_IMPL_DISABLE_CACHE =
      "fs.hdfs.impl.disable.cache";

  /** The Kerberos principal for the job tracker. */
  public static final String JT_PRINCIPAL = JTConfig.JT_USER_NAME;
  // "mapreduce.jobtracker.kerberos.principal";
  /** The Kerberos principal for the resource manager. */
  public static final String RM_PRINCIPAL = "yarn.resourcemanager.principal";

  public static final String HADOOP_JOB_TRACKER = "mapred.job.tracker";
  public static final String HADOOP_JOB_TRACKER_2 =
      "mapreduce.jobtracker.address";
  public static final String HADOOP_YARN_RM = "yarn.resourcemanager.address";

  private static final String OTHER_NAMENODES_TO_GET_TOKEN = "other_namenodes";

  public static final Text DEFAULT_RENEWER = new Text("azkaban mr tokens");

  private static final String AZKABAN_KEYTAB_LOCATION = "proxy.keytab.location";
  private static final String AZKABAN_PRINCIPAL = "proxy.user";
  private static final String OBTAIN_JOBHISTORYSERVER_TOKEN =
      "obtain.jobhistoryserver.token";

  private UserGroupInformation loginUser = null;
  private final static Logger logger = Logger
      .getLogger(HadoopSecurityManager_H_2_0.class);
  private Configuration conf;

  private String keytabLocation;
  private String keytabPrincipal;
  private boolean shouldProxy = false;
  private boolean securityEnabled = false;

  private static HadoopSecurityManager hsmInstance = null;
  private ConcurrentMap<String, UserGroupInformation> userUgiMap;

  private static URLClassLoader ucl;

  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private HadoopSecurityManager_H_2_0(Props props)
      throws HadoopSecurityManagerException, IOException {

    // for now, assume the same/compatible native library, the same/compatible
    // hadoop-core jar
    String hadoopHome = props.getString("hadoop.home", null);
    String hadoopConfDir = props.getString("hadoop.conf.dir", null);

    if (hadoopHome == null) {
      hadoopHome = System.getenv("HADOOP_HOME");
    }
    if (hadoopConfDir == null) {
      hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
    }

    List<URL> resources = new ArrayList<URL>();
    URL urlToHadoop = null;
    if (hadoopConfDir != null) {
      urlToHadoop = new File(hadoopConfDir).toURI().toURL();
      logger.info("Using hadoop config found in " + urlToHadoop);
      resources.add(urlToHadoop);
    } else if (hadoopHome != null) {
      urlToHadoop = new File(hadoopHome, "conf").toURI().toURL();
      logger.info("Using hadoop config found in " + urlToHadoop);
      resources.add(urlToHadoop);
    } else {
      logger.info("HADOOP_HOME not set, using default hadoop config.");
    }

    ucl = new URLClassLoader(resources.toArray(new URL[resources.size()]));

    conf = new Configuration();
    conf.setClassLoader(ucl);

    if (props.containsKey(FS_HDFS_IMPL_DISABLE_CACHE)) {
      logger.info("Setting " + FS_HDFS_IMPL_DISABLE_CACHE + " to "
          + props.get(FS_HDFS_IMPL_DISABLE_CACHE));
      conf.setBoolean(FS_HDFS_IMPL_DISABLE_CACHE,
          Boolean.valueOf(props.get(FS_HDFS_IMPL_DISABLE_CACHE)));
    }

    logger.info(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION + ": "
        + conf.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION));
    logger.info(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION + ":  "
        + conf.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION));
    logger.info(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY + ": "
        + conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));

    UserGroupInformation.setConfiguration(conf);

    securityEnabled = UserGroupInformation.isSecurityEnabled();
    if (securityEnabled) {
      logger.info("The Hadoop cluster has enabled security");
      shouldProxy = true;
      try {

        keytabLocation = props.getString(AZKABAN_KEYTAB_LOCATION);
        keytabPrincipal = props.getString(AZKABAN_PRINCIPAL);
      } catch (UndefinedPropertyException e) {
        throw new HadoopSecurityManagerException(e.getMessage());
      }

      // try login
      try {
        if (loginUser == null) {
          logger.info("No login user. Creating login user");
          logger.info("Using principal from " + keytabPrincipal + " and "
              + keytabLocation);
          UserGroupInformation.loginUserFromKeytab(keytabPrincipal,
              keytabLocation);
          loginUser = UserGroupInformation.getLoginUser();
          logger.info("Logged in with user " + loginUser);
        } else {
          logger.info("loginUser (" + loginUser
              + ") already created, refreshing tgt.");
          loginUser.checkTGTAndReloginFromKeytab();
        }
      } catch (IOException e) {
        throw new HadoopSecurityManagerException(
            "Failed to login with kerberos ", e);
      }

    }

    userUgiMap = new ConcurrentHashMap<String, UserGroupInformation>();

    logger.info("Hadoop Security Manager initialized");
  }

  public static HadoopSecurityManager getInstance(Props props)
      throws HadoopSecurityManagerException, IOException {
    if (hsmInstance == null) {
      synchronized (HadoopSecurityManager_H_2_0.class) {
        if (hsmInstance == null) {
          logger.info("getting new instance");
          hsmInstance = new HadoopSecurityManager_H_2_0(props);
        }
      }
    }
    return hsmInstance;
  }

  /**
   * Create a proxied user based on the explicit user name, taking other
   * parameters necessary from properties file.
   * 
   * @throws IOException
   */
  @Override
  public synchronized UserGroupInformation getProxiedUser(String userToProxy)
      throws HadoopSecurityManagerException {

    if (userToProxy == null) {
      throw new HadoopSecurityManagerException("userToProxy can't be null");
    }

    UserGroupInformation ugi = userUgiMap.get(userToProxy);
    if (ugi == null) {
      logger.info("proxy user " + userToProxy
          + " not exist. Creating new proxy user");
      if (shouldProxy) {
        try {
          ugi =
              UserGroupInformation.createProxyUser(userToProxy,
                  UserGroupInformation.getLoginUser());
        } catch (IOException e) {
          throw new HadoopSecurityManagerException(
              "Failed to create proxy user", e);
        }
      } else {
        ugi = UserGroupInformation.createRemoteUser(userToProxy);
      }
      userUgiMap.putIfAbsent(userToProxy, ugi);
    }
    return ugi;
  }

  /**
   * Create a proxied user, taking all parameters, including which user to proxy
   * from provided Properties.
   */
  @Override
  public UserGroupInformation getProxiedUser(Props userProp)
      throws HadoopSecurityManagerException {
    String userToProxy = verifySecureProperty(userProp, USER_TO_PROXY);
    UserGroupInformation user = getProxiedUser(userToProxy);
    if (user == null) {
      throw new HadoopSecurityManagerException(
          "Proxy as any user in unsecured grid is not supported!");
    }
    return user;
  }

  public String verifySecureProperty(Props props, String s)
      throws HadoopSecurityManagerException {
    String value = props.getString(s);
    if (value == null) {
      throw new HadoopSecurityManagerException(s + " not set in properties.");
    }
    return value;
  }

  @Override
  public FileSystem getFSAsUser(String user)
      throws HadoopSecurityManagerException {
    FileSystem fs;
    try {
      logger.info("Getting file system as " + user);
      UserGroupInformation ugi = getProxiedUser(user);

      if (ugi != null) {
        fs = ugi.doAs(new PrivilegedAction<FileSystem>() {

          @Override
          public FileSystem run() {
            try {
              return FileSystem.get(conf);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
      } else {
        fs = FileSystem.get(conf);
      }
    } catch (Exception e) {
      throw new HadoopSecurityManagerException("Failed to get FileSystem. ", e);
    }
    return fs;
  }

  public boolean shouldProxy() {
    return shouldProxy;
  }

  @Override
  public boolean isHadoopSecurityEnabled() {
    return securityEnabled;
  }

  /*
   * Gets hadoop tokens for a user to run mapred/pig jobs on a secured cluster
   */
  @Override
  public synchronized void prefetchToken(final File tokenFile,
      final String userToProxy, final Logger logger)
      throws HadoopSecurityManagerException {

    logger.info("Getting hadoop tokens for " + userToProxy);

    try {
      getProxiedUser(userToProxy).doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          getToken(userToProxy);
          return null;
        }

        private void getToken(String userToProxy) throws InterruptedException,
            IOException, HadoopSecurityManagerException {

          FileSystem fs = FileSystem.get(conf);
          // check if we get the correct FS, and most importantly, the conf
          logger.info("Getting DFS token from " + fs.getCanonicalServiceName()
              + fs.getUri());
          Token<?> fsToken = fs.getDelegationToken(userToProxy);
          if (fsToken == null) {
            logger.error("Failed to fetch DFS token for ");
            throw new HadoopSecurityManagerException(
                "Failed to fetch DFS token for " + userToProxy);
          }
          logger.info("Created DFS token: " + fsToken.toString());
          logger.info("Token kind: " + fsToken.getKind());
          logger.info("Token id: " + fsToken.getIdentifier());
          logger.info("Token service: " + fsToken.getService());

          JobConf jc = new JobConf(conf);
          JobClient jobClient = new JobClient(jc);
          logger.info("Pre-fetching JT token: Got new JobClient: " + jc);

          Token<DelegationTokenIdentifier> mrdt =
              jobClient.getDelegationToken(new Text("mr token"));
          if (mrdt == null) {
            logger.error("Failed to fetch JT token for ");
            throw new HadoopSecurityManagerException(
                "Failed to fetch JT token for " + userToProxy);
          }
          logger.info("Created JT token: " + mrdt.toString());
          logger.info("Token kind: " + mrdt.getKind());
          logger.info("Token id: " + mrdt.getIdentifier());
          logger.info("Token service: " + mrdt.getService());

          jc.getCredentials().addToken(mrdt.getService(), mrdt);
          jc.getCredentials().addToken(fsToken.getService(), fsToken);

          FileOutputStream fos = null;
          DataOutputStream dos = null;
          try {
            fos = new FileOutputStream(tokenFile);
            dos = new DataOutputStream(fos);
            jc.getCredentials().writeTokenStorageToStream(dos);
          } finally {
            if (dos != null) {
              try {
                dos.close();
              } catch (Throwable t) {
                // best effort
                logger
                    .error(
                        "encountered exception while closing DataOutputStream of the tokenFile",
                        t);
              }
            }
            if (fos != null) {
              fos.close();
            }
          }
          // stash them to cancel after use.
          logger.info("Tokens loaded in " + tokenFile.getAbsolutePath());
        }
      });
    } catch (Exception e) {
      throw new HadoopSecurityManagerException("Failed to get hadoop tokens! "
          + e.getMessage() + e.getCause());

    }
  }

  private void cancelNameNodeToken(final Token<? extends TokenIdentifier> t,
      String userToProxy) throws HadoopSecurityManagerException {
    try {
      getProxiedUser(userToProxy).doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          cancelToken(t);
          return null;
        }

        private void cancelToken(Token<?> nt) throws IOException,
            InterruptedException {
          nt.cancel(conf);
        }
      });
    } catch (Exception e) {
      throw new HadoopSecurityManagerException("Failed to cancel token. "
          + e.getMessage() + e.getCause(), e);
    }
  }

  private void cancelMRJobTrackerToken(
      final Token<? extends TokenIdentifier> t, String userToProxy)
      throws HadoopSecurityManagerException {
    try {
      getProxiedUser(userToProxy).doAs(new PrivilegedExceptionAction<Void>() {
        @SuppressWarnings("unchecked")
        @Override
        public Void run() throws Exception {
          cancelToken((Token<DelegationTokenIdentifier>) t);
          return null;
        }

        private void cancelToken(Token<DelegationTokenIdentifier> jt)
            throws IOException, InterruptedException {
          JobConf jc = new JobConf(conf);
          JobClient jobClient = new JobClient(jc);
          jobClient.cancelDelegationToken(jt);
        }
      });
    } catch (Exception e) {
      throw new HadoopSecurityManagerException("Failed to cancel token. "
          + e.getMessage() + e.getCause(), e);
    }
  }

  private void cancelJhsToken(final Token<? extends TokenIdentifier> t,
      String userToProxy) throws HadoopSecurityManagerException {
    // it appears yarn would clean up this token after app finish, after a long
    // while though.
    org.apache.hadoop.yarn.api.records.Token token =
        org.apache.hadoop.yarn.api.records.Token.newInstance(t.getIdentifier(),
            t.getKind().toString(), t.getPassword(), t.getService().toString());
    final YarnRPC rpc = YarnRPC.create(conf);
    final InetSocketAddress jhsAddress = SecurityUtil.getTokenServiceAddr(t);
    MRClientProtocol jhsProxy = null;
    try {
      jhsProxy =
          UserGroupInformation.getCurrentUser().doAs(
              new PrivilegedAction<MRClientProtocol>() {
                @Override
                public MRClientProtocol run() {
                  return (MRClientProtocol) rpc.getProxy(
                      HSClientProtocol.class, jhsAddress, conf);
                }
              });
      CancelDelegationTokenRequest request =
          Records.newRecord(CancelDelegationTokenRequest.class);
      request.setDelegationToken(token);
      jhsProxy.cancelDelegationToken(request);
    } catch (Exception e) {
      throw new HadoopSecurityManagerException("Failed to cancel token. "
          + e.getMessage() + e.getCause(), e);
    } finally {
      RPC.stopProxy((VersionedProtocol) jhsProxy);
    }

  }

  private void cancelHiveToken(final Token<? extends TokenIdentifier> t,
      String userToProxy) throws HadoopSecurityManagerException {
    try {
      HiveConf hiveConf = new HiveConf();
      HiveMetaStoreClient hiveClient = new HiveMetaStoreClient(hiveConf);
      hiveClient.cancelDelegationToken(t.encodeToUrlString());
    } catch (Exception e) {
      throw new HadoopSecurityManagerException("Failed to cancel Token. "
          + e.getMessage() + e.getCause(), e);
    }
  }

  @Override
  public void cancelTokens(File tokenFile, String userToProxy, Logger logger)
      throws HadoopSecurityManagerException {
    // nntoken
    Credentials cred = null;
    try {
      cred =
          Credentials.readTokenStorageFile(new Path(tokenFile.toURI()),
              new Configuration());
      for (Token<? extends TokenIdentifier> t : cred.getAllTokens()) {

        logger.info("Got token: " + t.toString());
        logger.info("Token kind: " + t.getKind());
        logger.info("Token id: " + new String(t.getIdentifier()));
        logger.info("Token service: " + t.getService());

        if (t.getKind().equals(new Text("HIVE_DELEGATION_TOKEN"))) {
          logger.info("Cancelling hive token " + new String(t.getIdentifier()));
          cancelHiveToken(t, userToProxy);
        } else if (t.getKind().equals(new Text("RM_DELEGATION_TOKEN"))) {
          logger.info("Cancelling mr job tracker token "
              + new String(t.getIdentifier()));
          // cancelMRJobTrackerToken(t, userToProxy);
        } else if (t.getKind().equals(new Text("HDFS_DELEGATION_TOKEN"))) {
          logger.info("Cancelling namenode token "
              + new String(t.getIdentifier()));
          // cancelNameNodeToken(t, userToProxy);
        } else if (t.getKind().equals(new Text("MR_DELEGATION_TOKEN"))) {
          logger.info("Cancelling jobhistoryserver mr token "
              + new String(t.getIdentifier()));
          // cancelJhsToken(t, userToProxy);
        } else {
          logger.info("unknown token type " + t.getKind());
        }
      }
    } catch (Exception e) {
      throw new HadoopSecurityManagerException("Failed to cancel tokens "
          + e.getMessage() + e.getCause(), e);
    }

  }

  /*
   * Gets hadoop tokens for a user to run mapred/hive jobs on a secured cluster
   */
  @Override
  public synchronized void prefetchToken(final File tokenFile,
      final Props props, final Logger logger)
      throws HadoopSecurityManagerException {

    final String userToProxy = props.getString(USER_TO_PROXY);

    logger.info("Getting hadoop tokens based on props for " + userToProxy);

    final Credentials cred = new Credentials();

    if (props.getBoolean(OBTAIN_HCAT_TOKEN, false)) {
      try {
        logger.info("Pre-fetching Hive MetaStore token from hive");

        HiveConf hiveConf = new HiveConf();

        logger.info(HiveConf.ConfVars.METASTOREURIS.varname + ": "
            + hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname));

        logger
            .info(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname
                + ": "
                + hiveConf
                    .get(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname));

        logger.info(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname
            + ": "
            + hiveConf
                .get(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname));

        HiveMetaStoreClient hiveClient = new HiveMetaStoreClient(hiveConf);
        String hcatTokenStr =
            hiveClient.getDelegationToken(userToProxy, UserGroupInformation
                .getLoginUser().getShortUserName());
        Token<DelegationTokenIdentifier> hcatToken =
            new Token<DelegationTokenIdentifier>();
        hcatToken.decodeFromUrlString(hcatTokenStr);
        logger.info("Created hive metastore token: " + hcatTokenStr);
        logger.info("Token kind: " + hcatToken.getKind());
        logger.info("Token id: " + hcatToken.getIdentifier());
        logger.info("Token service: " + hcatToken.getService());
        cred.addToken(hcatToken.getService(), hcatToken);
      } catch (Exception e) {
        logger.error(
            "Failed to get hive metastore token." + e.getMessage()
                + e.getCause(), e);
      } catch (Throwable t) {
        logger.error(
            "Failed to get hive metastore token." + t.getMessage()
                + t.getCause(), t);
      }
    }

    if (props.getBoolean(OBTAIN_JOBHISTORYSERVER_TOKEN, false)) {
      YarnRPC rpc = YarnRPC.create(conf);
      final String serviceAddr = conf.get(JHAdminConfig.MR_HISTORY_ADDRESS);

      logger.debug("Connecting to HistoryServer at: " + serviceAddr);
      HSClientProtocol hsProxy =
          (HSClientProtocol) rpc.getProxy(HSClientProtocol.class,
              NetUtils.createSocketAddr(serviceAddr), conf);
      logger.info("Pre-fetching JH token from job history server");

      Token<?> jhsdt = null;
      try {
        jhsdt = getDelegationTokenFromHS(hsProxy);
      } catch (Exception e) {
        logger.error("Failed to fetch JH token", e);
        throw new HadoopSecurityManagerException(
            "Failed to fetch JH token for " + userToProxy);
      }

      if (jhsdt == null) {
        logger.error("getDelegationTokenFromHS() returned null");
        throw new HadoopSecurityManagerException(
            "Unable to fetch JH token for " + userToProxy);
      }

      logger.info("Created JH token: " + jhsdt.toString());
      logger.info("Token kind: " + jhsdt.getKind());
      logger.info("Token id: " + jhsdt.getIdentifier());
      logger.info("Token service: " + jhsdt.getService());

      cred.addToken(jhsdt.getService(), jhsdt);
    }

    try {
      getProxiedUser(userToProxy).doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          getToken(userToProxy);
          return null;
        }

        private void getToken(String userToProxy) throws InterruptedException,
            IOException, HadoopSecurityManagerException {
          logger.info("Here is the props for " + OBTAIN_NAMENODE_TOKEN + ": "
              + props.getBoolean(OBTAIN_NAMENODE_TOKEN));
          if (props.getBoolean(OBTAIN_NAMENODE_TOKEN, false)) {
            FileSystem fs = FileSystem.get(conf);
            // check if we get the correct FS, and most importantly, the
            // conf
            logger.info("Getting DFS token from " + fs.getUri());
            Token<?> fsToken =
                fs.getDelegationToken(getMRTokenRenewerInternal(new JobConf())
                    .toString());
            if (fsToken == null) {
              logger.error("Failed to fetch DFS token for ");
              throw new HadoopSecurityManagerException(
                  "Failed to fetch DFS token for " + userToProxy);
            }
            logger.info("Created DFS token: " + fsToken.toString());
            logger.info("Token kind: " + fsToken.getKind());
            logger.info("Token id: " + fsToken.getIdentifier());
            logger.info("Token service: " + fsToken.getService());

            cred.addToken(fsToken.getService(), fsToken);

            // getting additional name nodes tokens
            String otherNamenodes = props.get(OTHER_NAMENODES_TO_GET_TOKEN);
            if ((otherNamenodes != null) && (otherNamenodes.length() > 0)) {
              logger.info(OTHER_NAMENODES_TO_GET_TOKEN + ": '" + otherNamenodes
                  + "'");
              String[] nameNodeArr = otherNamenodes.split(",");
              Path[] ps = new Path[nameNodeArr.length];
              for (int i = 0; i < ps.length; i++) {
                ps[i] = new Path(nameNodeArr[i].trim());
              }
              TokenCache.obtainTokensForNamenodes(cred, ps, conf);
              logger.info("Successfully fetched tokens for: " + otherNamenodes);
            } else {
              logger.info(OTHER_NAMENODES_TO_GET_TOKEN + " was not configured");
            }
          }

          if (props.getBoolean(OBTAIN_JOBTRACKER_TOKEN, false)) {
            JobConf jobConf = new JobConf();
            JobClient jobClient = new JobClient(jobConf);
            logger.info("Pre-fetching JT token from JobTracker");

            Token<DelegationTokenIdentifier> mrdt =
                jobClient
                    .getDelegationToken(getMRTokenRenewerInternal(jobConf));
            if (mrdt == null) {
              logger.error("Failed to fetch JT token");
              throw new HadoopSecurityManagerException(
                  "Failed to fetch JT token for " + userToProxy);
            }
            logger.info("Created JT token: " + mrdt.toString());
            logger.info("Token kind: " + mrdt.getKind());
            logger.info("Token id: " + mrdt.getIdentifier());
            logger.info("Token service: " + mrdt.getService());
            cred.addToken(mrdt.getService(), mrdt);
          }

        }
      });

      FileOutputStream fos = null;
      DataOutputStream dos = null;
      try {
        fos = new FileOutputStream(tokenFile);
        dos = new DataOutputStream(fos);
        cred.writeTokenStorageToStream(dos);
      } finally {
        if (dos != null) {
          try {
            dos.close();
          } catch (Throwable t) {
            // best effort
            logger
                .error(
                    "encountered exception while closing DataOutputStream of the tokenFile",
                    t);
          }
        }
        if (fos != null) {
          fos.close();
        }
      }
      // stash them to cancel after use.

      logger.info("Tokens loaded in " + tokenFile.getAbsolutePath());

    } catch (Exception e) {
      throw new HadoopSecurityManagerException("Failed to get hadoop tokens! "
          + e.getMessage() + e.getCause(), e);
    } catch (Throwable t) {
      throw new HadoopSecurityManagerException("Failed to get hadoop tokens! "
          + t.getMessage() + t.getCause(), t);
    }

  }

  private Text getMRTokenRenewerInternal(JobConf jobConf) throws IOException {
    // Taken from Oozie
    //
    // Getting renewer correctly for JT principal also though JT in hadoop
    // 1.x does not have
    // support for renewing/cancelling tokens
    String servicePrincipal =
        jobConf.get(RM_PRINCIPAL, jobConf.get(JT_PRINCIPAL));
    Text renewer;
    if (servicePrincipal != null) {
      String target =
          jobConf.get(HADOOP_YARN_RM, jobConf.get(HADOOP_JOB_TRACKER_2));
      if (target == null) {
        target = jobConf.get(HADOOP_JOB_TRACKER);
      }

      String addr = NetUtils.createSocketAddr(target).getHostName();
      renewer =
          new Text(SecurityUtil.getServerPrincipal(servicePrincipal, addr));
    } else {
      // No security
      renewer = DEFAULT_RENEWER;
    }

    return renewer;
  }

  private Token<?> getDelegationTokenFromHS(HSClientProtocol hsProxy)
      throws IOException, InterruptedException {
    GetDelegationTokenRequest request =
        recordFactory.newRecordInstance(GetDelegationTokenRequest.class);
    request.setRenewer(Master.getMasterPrincipal(conf));
    org.apache.hadoop.yarn.api.records.Token mrDelegationToken;
    mrDelegationToken =
        hsProxy.getDelegationToken(request).getDelegationToken();
    return ConverterUtils.convertFromYarn(mrDelegationToken,
        hsProxy.getConnectAddress());
  }

  private void cancelDelegationTokenFromHS(
      final org.apache.hadoop.yarn.api.records.Token t, HSClientProtocol hsProxy)
      throws IOException, InterruptedException {
    CancelDelegationTokenRequest request =
        recordFactory.newRecordInstance(CancelDelegationTokenRequest.class);
    request.setDelegationToken(t);
    hsProxy.cancelDelegationToken(request);
  }

}
