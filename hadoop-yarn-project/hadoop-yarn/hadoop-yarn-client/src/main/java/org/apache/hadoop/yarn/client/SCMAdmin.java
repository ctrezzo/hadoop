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

package org.apache.hadoop.yarn.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.server.api.SCMAdminProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.RunSharedCacheCleanerTaskRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RunSharedCacheCleanerTaskResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;

public class SCMAdmin extends Configured implements Tool {

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  public SCMAdmin() {
    super();
  }

  public SCMAdmin(Configuration conf) {
    super(conf);
  }

  private static void printHelp(String cmd) {
    String summary = "scmadmin is the command to execute shared cache manager" +
        "administrative commands.\n" +
        "The full syntax is: \n\n" +
        "hadoop scmadmin" +
        " [-runCleanerTask]" +
        " [-help [cmd]]\n";

    String runCleanerTask =
        "-runCleanerTask: Run cleaner task right away.\n";

    String help = "-help [cmd]: \tDisplays help for the given command or all commands if none\n" +
        "\t\tis specified.\n";

    if ("runCleanerTask".equals(cmd)) {
      System.out.println(runCleanerTask);
    } else if ("help".equals(cmd)) {
      System.out.println(help);
    } else {
      System.out.println(summary);
      System.out.println(runCleanerTask);
      System.out.println(help);
      System.out.println();
      ToolRunner.printGenericCommandUsage(System.out);
    }
  }

  /**
   * Displays format of commands.
   * @param cmd The command that is being executed.
   */
  private static void printUsage(String cmd) {
    if ("-runCleanerTask".equals(cmd)) {
      System.err.println("Usage: yarn scmadmin" + " [-runCleanerTask]");
    } else {
      System.err.println("Usage: yarn scmadmin");
      System.err.println("           [-runCleanerTask]");
      System.err.println("           [-help [cmd]]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  private static UserGroupInformation getUGI(Configuration conf
  ) throws IOException {
    return UserGroupInformation.getCurrentUser();
  }

  private SCMAdminProtocol createSCMAdminProtocol() throws IOException {
    // Get the current configuration
    final YarnConfiguration conf = new YarnConfiguration(getConf());

    // Create the admin client
    final InetSocketAddress addr = conf.getSocketAddr(
        YarnConfiguration.SCM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_ADMIN_PORT);
    final YarnRPC rpc = YarnRPC.create(conf);
    
    SCMAdminProtocol scmAdminProtocol =
      getUGI(conf).doAs(new PrivilegedAction<SCMAdminProtocol>() {
        @Override
        public SCMAdminProtocol run() {
          return (SCMAdminProtocol) rpc.getProxy(SCMAdminProtocol.class,
              addr, conf);
        }
      });

    return scmAdminProtocol;
  }
  
  private int runCleanerTask() throws YarnException, IOException {
    // run cleaner task right away
    SCMAdminProtocol scmAdminProtocol = createSCMAdminProtocol();
    RunSharedCacheCleanerTaskRequest request =
      recordFactory.newRecordInstance(RunSharedCacheCleanerTaskRequest.class);
    RunSharedCacheCleanerTaskResponse response =
        scmAdminProtocol.runCleanerTask(request);
    if (response.getAccepted()) {
      System.out.println("request accepted by shared cache manager");
      return 0;
    } else {
      System.out.println("request rejected by shared cache manager");
      return 1;
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage("");
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = args[i++];
    //
    // verify that we have enough command line parameters
    //
    if ("-runCleanerTask".equals(cmd)) {
      if (args.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    }
    
    exitCode = 0;
    try {
      if ("-runCleanerTask".equals(cmd)) {
        exitCode = runCleanerTask();
      } else if ("-help".equals(cmd)) {
        if (i < args.length) {
          printUsage(args[i]);
        } else {
          printHelp("");
        }
      } else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
        printUsage("");
        printUsage("");
      }

    } catch (IllegalArgumentException arge) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage(cmd);
    } catch (RemoteException e) {
      //
      // This is a error returned by hadoop server. Print
      // out the first line of the error mesage, ignore the stack trace.
      exitCode = -1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        System.err.println(cmd.substring(1) + ": "
                           + content[0]);
      } catch (Exception ex) {
        System.err.println(cmd.substring(1) + ": "
                           + ex.getLocalizedMessage());
      }
    } catch (Exception e) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": "
                         + e.getLocalizedMessage());
    }
    return exitCode;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new SCMAdmin(), args);
    System.exit(result);
  }
}
