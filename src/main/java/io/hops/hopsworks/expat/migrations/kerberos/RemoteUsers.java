/*
 * This file is part of Expat
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
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

package io.hops.hopsworks.expat.migrations.kerberos;

import com.google.common.io.Files;
import io.hops.hopsworks.common.util.HopsUtils;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.certificates.CertificatesFacade;
import io.hops.hopsworks.expat.db.dao.certificates.ExpatCertificate;
import io.hops.hopsworks.expat.db.dao.user.ExpatUser;
import io.hops.hopsworks.expat.db.dao.user.ExpatUserFacade;
import io.hops.hopsworks.expat.db.dao.user.RemoteUser;
import io.hops.hopsworks.expat.db.dao.user.RemoteUserFacade;
import io.hops.hopsworks.expat.ldap.LDAPQuery;
import io.hops.hopsworks.expat.ldap.LdapUserNotFound;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NamingException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class RemoteUsers implements MigrateStep {

  private Connection dbConnection = null;
  private LDAPQuery ldapQuery = null;
  private Configuration config = null;

  private ExpatUserFacade expatUserFacade = null;
  private RemoteUserFacade remoteUserFacade = null;
  private CertificatesFacade certificatesFacade = null;

  private String masterPassword = null;

  private boolean dryrun = true;

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteUsers.class);

  public RemoteUsers()
      throws SQLException, ConfigurationException, NamingException, IOException {
    config = ConfigurationBuilder.getConfiguration();

    dryrun = config.getBoolean(ExpatConf.DRY_RUN);

    dbConnection = DbConnectionFactory.getConnection();

    ldapQuery = new LDAPQuery(config);

    expatUserFacade = new ExpatUserFacade();
    remoteUserFacade = new RemoteUserFacade();
    certificatesFacade = new CertificatesFacade();

    Path masterPwdPath = Paths.get(config.getString(ExpatConf.MASTER_PWD_FILE_KEY));
    masterPassword = Files.toString(masterPwdPath.toFile(), Charset.defaultCharset());
  }

  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Starting Kerberos migration");
    try {
      if (dryrun){
        dbConnection.setAutoCommit(false);
      }

      List<ExpatUser> expatUsers = expatUserFacade.getLocalUsers(dbConnection);
      for (ExpatUser expatUser : expatUsers) {
        LOGGER.info("Processing user: " + expatUser.getEmail());
        try {
          dbConnection.setAutoCommit(false);
          String uuid = ldapQuery.getUUID(expatUser);
          addRemoteUser(dbConnection, uuid, expatUser, dryrun);
          updateUserPassword(expatUser, dryrun);
          if (!dryrun) {
            dbConnection.commit();
          }
          LOGGER.info("Processed LDAP user for email: " + expatUser.getEmail());
        } catch (LdapUserNotFound notFound) {
          LOGGER.warn("Could not find LDAP user for email: " + expatUser.getEmail());
        } catch (Exception e) {
          LOGGER.warn("Error processing password update for user: " + expatUser.getEmail());
          if (!dryrun) {
            dbConnection.rollback();
            dbConnection.setAutoCommit(true);
          }
        }
      }
    } catch (SQLException e)  {
      throw new MigrationException(e.getMessage());
    }
  }

  @Override
  public void rollback() throws RollbackException {
    // No rollback. Once you tried krb you never go back
  }

  private void addRemoteUser(Connection connection, String uuid, ExpatUser expatUser, boolean dryRun)
      throws SQLException {
    RemoteUser remoteUser = new RemoteUser(2, expatUser.getPassword(), uuid, expatUser.getUid());
    remoteUserFacade.insertRemoteUser(connection, remoteUser, dryRun);
  }

  private void updateUserPassword(ExpatUser expatUser, boolean dryRun) throws Exception {
    List<ExpatCertificate> userCertificates = certificatesFacade.getUserCertificates(dbConnection, expatUser);
    String newUserPwd = DigestUtils.sha256Hex(expatUser.getPassword() + expatUser.getSalt());

    for (ExpatCertificate certificate : userCertificates) {
      LOGGER.info("Updating password for certificate: " + certificate.getProjectName());
      try {
        String decryptedCertPwd =
            HopsUtils.decrypt(expatUser.getPassword(), certificate.getCipherPassword(), masterPassword);
        LOGGER.info("Certificate PWD: " + decryptedCertPwd);
        String newCertPwd = HopsUtils.encrypt(newUserPwd, decryptedCertPwd, masterPassword);
        certificatesFacade.updateCertPassword(dbConnection, certificate, newCertPwd, dryRun);
      } catch (Exception e) {
        LOGGER.info("Error Decrypting password for project certificate: " + certificate.getProjectName());
      }
    }

    LOGGER.info("Updating password for user");
    expatUserFacade.updateUserPassword(dbConnection, expatUser, newUserPwd, dryRun);
    LOGGER.info("Updating mode for user");
    expatUserFacade.updateMode(dbConnection, expatUser, 1, dryRun);
  }
}
