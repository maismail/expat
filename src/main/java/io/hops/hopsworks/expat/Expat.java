/*
 * This file is part of Expat
 * Copyright (C) 2018, Logical Clocks AB. All rights reserved
 *
 * Expat is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * Expat is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with
 * this program. If not, see <https://www.gnu.org/licenses/>.
 *
 */

package io.hops.hopsworks.expat;

import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.executor.ProcessExecutor;
import io.hops.hopsworks.expat.executor.ShutdownManager;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.conda.CreateKagentLogsIndeces;
import io.hops.hopsworks.expat.migrations.elk.BeamKibana;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.kohsuke.args4j.OptionHandlerFilter.ALL;

public class Expat {

  @Option(name="-a", usage="Action to take: migrate/rollback")
  private String command = "migrate";

  @Option(name="-v", usage="Version to migrate to or to rollback")
  private String version;
  
  private final static Logger LOGGER = LoggerFactory.getLogger(Expat.class);
  public Expat(String[] args) {
    CmdLineParser parser = new CmdLineParser(this);

    try {
      // parse the arguments.
      parser.parseArgument(args);

    } catch( CmdLineException e ) {
      // if there's a problem in the command line,
      // you'll get this exception. this will report
      // an error message.
      System.err.println(e.getMessage());
      System.err.println("java Expat [options...]");
      // print the list of available options
      parser.printUsage(System.err);
      System.err.println();

      // print option sample. This is useful some time
      System.err.println("  Example: java Expat"+parser.printExample(ALL));
      System.exit(1);
    }
  }

  public void run() throws ConfigurationException, MigrationException, RollbackException,
      ClassNotFoundException, InstantiationException, IllegalAccessException {

    Configuration config = ConfigurationBuilder.getConfiguration();
    // If the version contains SNAPSHOT, remove it
    version = version.replace("-SNAPSHOT", "");
    // Remove minor version until HOPSWORKS-814 is fixed
    version = version.substring(0, version.lastIndexOf("."));

    String migrations = config.getString("version-" + version.replace(".", ""));
    if (migrations == null) {
      // No migrations to apply
      return;
    }

    String[] migrationClasses = migrations.split("\n");
    for (String migration : migrationClasses) {
      MigrateStep step = (MigrateStep) Class.forName(migration.trim()).newInstance();
      if (command.equalsIgnoreCase("migrate")) {
        step.migrate();
      } else {
        step.rollback();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Expat e = new Expat(args);
  
    ShutdownManager.getManager().addShutdownHook(new ProcessExecutor.ShutdownHook(), 100);
    ShutdownManager.getManager().addShutdownHook(new CreateKagentLogsIndeces.ShutdownHook(), 150);
    ShutdownManager.getManager().addShutdownHook(new BeamKibana.ShutdownHook(), 200);
    
    e.run();
  }
}
