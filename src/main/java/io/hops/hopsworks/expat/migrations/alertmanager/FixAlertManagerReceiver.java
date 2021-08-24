/*
 * This file is part of Expat
 * Copyright (C) 2021, Logical Clocks AB. All rights reserved
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
 */
package io.hops.hopsworks.expat.migrations.alertmanager;

import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;

public class FixAlertManagerReceiver implements MigrateStep {
  private static final Logger LOGGER = LogManager.getLogger(FixAlertManagerReceiver.class);
  
  private AlertManagerConfigHelper alertManagerConfigHelper;
  
  private void setup() throws ConfigurationException, SQLException {
    alertManagerConfigHelper = new AlertManagerConfigHelper();
    alertManagerConfigHelper.setup();
  }
  
  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Alert manager receiver fix started...");
    try {
      setup();
      alertManagerConfigHelper.migrate();
    } catch (Exception e) {
      throw new MigrationException("Alert manager receiver fix error", e);
    } finally {
      alertManagerConfigHelper.close();
    }
  }
  
  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("Alert manager receiver fix rollback started...");
    try {
      setup();
      alertManagerConfigHelper.rollback();
    } catch (Exception e) {
      throw new RollbackException("Alert manager receiver fix rollback error", e);
    } finally {
      alertManagerConfigHelper.close();
    }
  }
}
