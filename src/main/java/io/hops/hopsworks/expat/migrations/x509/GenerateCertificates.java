/**
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

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.hops.hopsworks.common.util.ProcessDescriptor;
import io.hops.hopsworks.common.util.ProcessResult;
import io.hops.hopsworks.expat.db.dao.certificates.ExpatCertificate;
import io.hops.hopsworks.expat.db.dao.user.ExpatUser;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.user.ExpatUserFacade;
import io.hops.hopsworks.expat.executor.ProcessExecutor;
import io.hops.hopsworks.expat.migrations.MigrationException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;

public abstract class GenerateCertificates {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateCertificates.class);

  protected Path certsBackupDir;
  protected Configuration config;
  protected Path masterPwdPath;
  protected String intermediateCA;
  protected String masterPassword;
  private Path generateUserCertificatesSh;
  protected Connection connection;
  protected ExpatUserFacade expatUserFacade;

  protected void setup(String backupDirPrefix)
      throws MigrationException, ConfigurationException, IOException, SQLException {
    String userHome = System.getProperty("user.home");
    if (userHome == null) {
      throw new MigrationException("Could not get user home");
    }
    LocalDateTime now = LocalDateTime.now();
    certsBackupDir = Paths.get(userHome, backupDirPrefix + "_certs_backup_" + now.toString());
  
    try {
      FileUtils.forceMkdir(certsBackupDir.toFile());
    } catch (IOException ex) {
      throw new MigrationException("Could not create certs backup directory", ex);
    }
    
    config = ConfigurationBuilder.getConfiguration();
    masterPwdPath = Paths.get(config.getString(ExpatConf.MASTER_PWD_FILE_KEY));
    intermediateCA = config.getString(ExpatConf.INTERMEDIATE_CA_PATH);
    masterPassword = Files.toString(masterPwdPath.toFile(), Charset.defaultCharset());
    generateUserCertificatesSh = Paths.get(config.getString(ExpatConf.EXPAT_PATH), "bin",
        "generate_user_certificates.sh");
    connection = DbConnectionFactory.getConnection();
    expatUserFacade = new ExpatUserFacade();
  }
  
  protected void generateNewCertsAndUpdateDb(Map<ExpatCertificate, ExpatUser> certificates, String print)
      throws SQLException, IOException {
    int total = certificates.size();
    LOGGER.info("Going to regenerate " + total + " certificates");
    LOGGER.info("Start generating new " + print + " Certificates");
    int idx = 1;
    for (Map.Entry<ExpatCertificate, ExpatUser> entry : certificates.entrySet()) {
      generateCertificate(entry.getKey(), entry.getValue(), idx, total);
      idx++;
    }
    LOGGER.info("Start updating certificates");
    updateCertificatesInDB(certificates.keySet(), connection);
  }
  
  private void generateCertificate(ExpatCertificate userCert, ExpatUser user, int idx, int total) throws IOException {
    // Move previous certificates for backup
    String id = userCert.getProjectName() + "__" + userCert.getUsername();
    LOGGER.info("Generating new certificate for " + userCert);
    
    String certId = id + ".cert.pem";
    File oldCert = Paths.get(intermediateCA, "certs", certId).toFile();
    File backupCert = Paths.get(certsBackupDir.toString(), certId).toFile();
    String keyId = id + ".key.pem";
    File oldKey = Paths.get(intermediateCA, "private", keyId).toFile();
    File backupKey = Paths.get(certsBackupDir.toString(), keyId).toFile();
    
    if (oldCert.exists()) {
      FileUtils.moveFile(oldCert, backupCert);
    }
    if (oldKey.exists()) {
      FileUtils.moveFile(oldKey, backupKey);
    }
    
    // Generate certificate
    ProcessDescriptor processDescriptor = new ProcessDescriptor.Builder()
        .addCommand("/usr/bin/sudo")
        .addCommand(generateUserCertificatesSh.toString())
        .addCommand(id)
        .addCommand(userCert.getPlainPassword())
        .addCommand(config.getString(ExpatConf.VALIDITY_DAYS, "3650"))
        .addCommand(config.getString(ExpatConf.CA_PASSWORD))
        .redirectErrorStream(true)
        .build();
    
    ProcessResult result = ProcessExecutor.getExecutor().execute(processDescriptor);
    if (result.getExitCode() != 0) {
      throw new IOException("Failed to generate certificate for " + id
        + " Reason: " + result.getStdout());
    }
    File keyStoreFile = Paths.get("/tmp", id + "__kstore.jks").toFile();
    File trustStoreFile = Paths.get("/tmp", id + "__tstore.jks").toFile();
    byte[] keyStore, trustStore;
    try (FileInputStream fis = new FileInputStream(keyStoreFile)) {
      keyStore = ByteStreams.toByteArray(fis);
    }
    try (FileInputStream fis = new FileInputStream(trustStoreFile)) {
      trustStore = ByteStreams.toByteArray(fis);
    }
    userCert.setKeyStore(keyStore);
    userCert.setTrustStore(trustStore);
    LOGGER.info("Finished generating new certificate for " + userCert + " - " + idx + "/" + total);
  }

  abstract void updateCertificatesInDB(Set<ExpatCertificate> certificates, Connection connection)
      throws SQLException;
}
