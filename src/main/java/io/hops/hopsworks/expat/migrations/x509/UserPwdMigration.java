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

package io.hops.hopsworks.expat.migrations.x509;

import io.hops.hopsworks.common.util.HopsUtils;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.certificates.CertificatesFacade;
import io.hops.hopsworks.expat.db.dao.certificates.ExpatCertificate;
import io.hops.hopsworks.expat.db.dao.user.ExpatUser;
import io.hops.hopsworks.expat.db.dao.user.ExpatUserFacade;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;

public class UserPwdMigration implements MigrateStep {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateProjectCertificates.class);
  private static final String MASTER_PWD = "5fcf82bc15aef42cd3ec93e6d4b51c04df110cf77ee715f62f3f172ff8ed9de9";

  private CertificatesFacade certificatesFacade = new CertificatesFacade();
  private ExpatUserFacade expatUserFacade = new ExpatUserFacade();

  private Configuration config = null;
  private boolean dryrun = false;

  public UserPwdMigration()
      throws ConfigurationException {
    config = ConfigurationBuilder.getConfiguration();
    dryrun = config.getBoolean(ExpatConf.DRY_RUN);
  }

  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Migrating");
    Connection connection = null;
    try {
      connection = DbConnectionFactory.getConnection();
      connection.setAutoCommit(false);
      ExpatUser expatUser = expatUserFacade.getExpatUserByEmail(connection, "");
      String newPwd = DigestUtils.sha256Hex( "" + expatUser.getSalt());

      List<ExpatCertificate> certificates = certificatesFacade.getUserCertificates(connection, expatUser);
      for (ExpatCertificate certificate : certificates) {
        String certPwd = HopsUtils.decrypt(expatUser.getPassword(), certificate.getCipherPassword(), MASTER_PWD);
        String newEncryptedPwd = HopsUtils.encrypt(newPwd, certPwd, MASTER_PWD);
        certificate.setCipherPassword(newEncryptedPwd);
        certificatesFacade.updateCertPassword(connection, certificate, newEncryptedPwd, dryrun);
      }
      expatUserFacade.updateUserPassword(connection, expatUser, newPwd, dryrun);
      connection.commit();
      LOGGER.info("Successfully changed pwd");
    } catch (Exception e) {
      try {
        connection.rollback();
      } catch (Exception e1) {}
      LOGGER.error("Error", e);
    } finally {
      try {
        if (connection != null) {
          connection.setAutoCommit(true);
        }
      } catch (Exception e) {}
    }
  }

  @Override
  public void rollback() throws RollbackException {
    // Do nothing
  }
}
