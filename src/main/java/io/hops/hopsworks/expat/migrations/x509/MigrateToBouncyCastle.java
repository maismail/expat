/**
 * This file is part of Expat
 * Copyright (C) 2022, Logical Clocks AB. All rights reserved
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

import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.certificates.CRLFacade;
import io.hops.hopsworks.expat.db.dao.certificates.CertificatesFacade;
import io.hops.hopsworks.expat.db.dao.certificates.KeysFacade;
import io.hops.hopsworks.expat.db.dao.certificates.SerialNumberFacade;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.bouncycastle.cert.X509CRLHolder;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CRLConverter;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.Security;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class MigrateToBouncyCastle implements MigrateStep {
  private static final Logger LOGGER = LoggerFactory.getLogger(MigrateToBouncyCastle.class);
  private Connection dbConnection;
  private final JcaPEMKeyConverter pemKeyConverter;
  private final JcaX509CertificateConverter x509CertificateConverter;
  private final JcaX509CRLConverter crlConverter;
  private KeysFacade keysFacade;
  private SerialNumberFacade serialNumberFacade;
  private CertificatesFacade certificatesFacade;
  private CRLFacade crlFacade;
  private Configuration config;
  private boolean dryRun = true;
  private final Set<String> certificatesToIgnore = new HashSet<>();

  static {
    Security.addProvider(new BouncyCastleProvider());
  }

  public MigrateToBouncyCastle() {
    pemKeyConverter = new JcaPEMKeyConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME);
    x509CertificateConverter = new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME);
    crlConverter = new JcaX509CRLConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME);

    certificatesToIgnore.add("/srv/hops/certs-dir/intermediate/certs/intermediate.cert.pem");
  }

  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Running migrations");
    try {
      setup();
    } catch (ConfigurationException | SQLException ex) {
      String msg = "Failed to setup connection to database";
      LOGGER.error(msg, ex);
      throw new MigrationException(msg, ex);
    }

    if (!Paths.get("/srv/hops/certs-dir/private/ca.key.pem").toFile().exists()) {
      LOGGER.info("ROOT CA private key does not exist. Skipping migration "
          + MigrateToBouncyCastle.class.getName());
      return;
    }

    // Migrate CA Private keys
    LOGGER.info("Migrating Certificate Authorities key pairs");
    try {
      migrateKeyPairs();
    } catch (Exception ex) {
      LOGGER.error("Failed to migrate key pairs", ex);
      throw new MigrationException("Failed to migrate key pairs", ex);
    }
    LOGGER.info("Finished migrating key pairs");

    // Migrate serial numbers
    LOGGER.info("Migrating Certificate Authorities serial number");
    try {
      migrateSerialNumbers();
    } catch (IOException | SQLException ex) {
      LOGGER.error("Failed to migrated CA serial number", ex);
      throw new MigrationException("Failed to migrated CA serial number", ex);
    }
    LOGGER.info("Finished migrating serial numbers");

    // Migrate certificates
    LOGGER.info("Migrating certificates for Certificate Authorities");
    try {
      migrateCertificates();
    } catch (Exception ex) {
      LOGGER.error("Failed to migrate certificates");
      throw new MigrationException("Failed to migrate certificates", ex);
    }
    LOGGER.info("Finished migrating certificates");

    // Migrate CRL
    LOGGER.info("Migrating CRL for CAs");
    try {
      migrateCRLs();
    } catch (Exception ex) {
      LOGGER.error("Failed to migrate CRLs", ex);
      throw new MigrationException("Failed to migrate CRLs", ex);
    }
    LOGGER.info("Finished migrating CRLs");
  }

  @Override
  public void rollback() throws RollbackException {
    try {
      if (serialNumberFacade != null) {
        serialNumberFacade.truncate();
      }
      if (keysFacade != null) {
        keysFacade.truncate();
      }
      if (crlFacade != null) {
        crlFacade.truncate();
      }
      if (certificatesFacade != null) {
        certificatesFacade.truncatePKICertificates(dbConnection);
      }
    } catch (Exception ex) {
      LOGGER.error("Error while rollback", ex);
      throw new RollbackException("Error while rollback", ex);
    }
  }

  private void setup() throws ConfigurationException, SQLException {
    config = ConfigurationBuilder.getConfiguration();
    dryRun = config.getBoolean(ExpatConf.DRY_RUN);

    dbConnection = DbConnectionFactory.getConnection();
    keysFacade = new KeysFacade(dbConnection, dryRun);
    serialNumberFacade = new SerialNumberFacade(dbConnection, dryRun);
    certificatesFacade = new CertificatesFacade();
    crlFacade = new CRLFacade(dbConnection, dryRun);
  }

  /*
   * CA key pairs
   */
  private void migrateKeyPairs() throws IOException, SQLException {
    // Root CA
    String password = config.getString(ExpatConf.CA_PASSWORD);
    LOGGER.info("Migrating ROOT Certificate Authority keys");
    migrateKeyPair(
        "ROOT",
        Paths.get("/srv/hops/certs-dir/private/ca.key.pem"),
        password);
    LOGGER.info("Finished successfully ROOT CA keys migration");

    // Intermediate CA
    LOGGER.info("Migrating INTERMEDIATE Certificate Authority keys");
    migrateKeyPair(
        "INTERMEDIATE",
        Paths.get("/srv/hops/certs-dir/intermediate/private/intermediate.key.pem"),
        password);
    LOGGER.info("Finished successfully INTERMEDIATE CA keys migration");

    // Kubernetes CA
    Path keyPath = Paths.get("/srv/hops/certs-dir/kube/private/kube-ca.key.pem");
    if (keyPath.toFile().exists()) {
      LOGGER.info("Migrating Kubernetes Certificate Authority keys");
      migrateKeyPair(
          "KUBECA",
          keyPath,
          password);
      LOGGER.info("Finished successfully Kubernetes CA keys migration");
    }
  }

  private void migrateKeyPair(String owner, Path path, String password) throws IOException, SQLException {
    LOGGER.info("Loading keypair for " + owner + " from " + path.toString());
    KeyPair keyPair = loadKeyPair(path, password);
    if (keysFacade.exists(owner)) {
      LOGGER.info("Key for " + owner + " has already been migrated. Skipping...");
      return;
    }
    LOGGER.info("Saving private key");
    keysFacade.insertKey(owner, 0, keyPair.getPrivate().getEncoded());
    LOGGER.info("Saving public key");
    keysFacade.insertKey(owner, 1, keyPair.getPublic().getEncoded());
  }

  private KeyPair loadKeyPair(Path path, String password) throws IOException  {
    PEMParser pemParser = new PEMParser(new FileReader(path.toFile()));
    Object object = pemParser.readObject();
    KeyPair kp;
    if (object instanceof PEMEncryptedKeyPair) {
      PEMEncryptedKeyPair ekp = (PEMEncryptedKeyPair) object;
      PEMDecryptorProvider decryptorProvider = new JcePEMDecryptorProviderBuilder().build(password.toCharArray());
      kp = pemKeyConverter.getKeyPair(ekp.decryptKeyPair(decryptorProvider));
    } else {
      PEMKeyPair ukp = (PEMKeyPair) object;
      kp = pemKeyConverter.getKeyPair(ukp);
    }
    return kp;
  }

  /*
   * Serial number
   */
  private void migrateSerialNumbers() throws IOException, SQLException {
    LOGGER.info("Migrating Serial Number for ROOT");
    migrateSerialNumber("ROOT", Paths.get("/srv/hops/certs-dir/serial"));

    LOGGER.info("Migrating Serial Number for INTERMEDIATE");
    migrateSerialNumber("INTERMEDIATE", Paths.get("/srv/hops/certs-dir/intermediate/serial"));

    Path serialNumberPath = Paths.get("/srv/hops/certs-dir/kube/serial");
    if (serialNumberPath.toFile().exists()) {
      LOGGER.info("Migrating Serial Number for Kubernetes");
      migrateSerialNumber("KUBECA", serialNumberPath);
    }
  }

  private void migrateSerialNumber(String type, Path path) throws IOException, SQLException {
    Long sn = getSerialNumber(path);
    if (serialNumberFacade.exists(type)) {
      LOGGER.info("Serial number for " + type + " has already been migrated. Skipping...");
      return;
    }
    serialNumberFacade.initializeSerialNumber(type, sn);
    LOGGER.info("Migrated Serial Number for " + type + " with next number " + sn);
  }

  private Long getSerialNumber(Path path) throws IOException {
    String hex = FileUtils.readFileToString(path.toFile(), Charset.defaultCharset());
    return Long.parseUnsignedLong(hex.trim(), 16);
  }

  /*
   * x.509
   */
  private void migrateCertificates() throws IOException {

    // ROOT
    LOGGER.info("Migrating certificates for ROOT CA");
    migrateCertificatesForCA(Paths.get("/srv/hops/certs-dir/certs"), 0);
    LOGGER.info("Finished certificates migration for ROOT CA");

    // INTERMEDIATE
    LOGGER.info("Migrating certificates for INTERMEDIATE CA");
    migrateCertificatesForCA(Paths.get("/srv/hops/certs-dir/intermediate/certs"), 1);
    LOGGER.info("Finished certificates migration for INTERMEDIATE CA");

    Path kubeCertsDir = Paths.get("/srv/hops/certs-dir/kube/certs");
    if (kubeCertsDir.toFile().exists()) {
      // Kubernetes
      LOGGER.info("Migrating certificates for Kubernetes CA");
      migrateCertificatesForCA(kubeCertsDir, 2);
      LOGGER.info("Finished certificates migration for Kubernetes CA");
    }
  }

  private void migrateCertificatesForCA(Path path, Integer ca) throws IOException {
    try (Stream<Path> files = Files.walk(path, 1)) {
      files
          .filter(Files::isRegularFile)
          .filter(f -> f.toString().endsWith(".pem"))
          .filter(f -> !certificatesToIgnore.contains(f.toString()))
          .forEach(f -> {
            try {
              LOGGER.info("Migrating certificate " + f);
              migrateCertificate(f, ca);
            } catch (IOException | CertificateException | SQLException ex) {
              throw new RuntimeException(ex);
            }
          });
    }
  }
  private void migrateCertificate(Path path, Integer ca) throws IOException, CertificateException, SQLException {
    X509Certificate certificate = loadCertificate(path);
    if (certificate != null) {
      Long serialNumber = certificate.getSerialNumber().longValue();
      Integer status = 0;
      String subject = certificate.getSubjectDN().toString();
      byte[] encoded = certificate.getEncoded();
      Date notBefore = certificate.getNotBefore();
      Date notAfter = certificate.getNotAfter();

      if (certificatesFacade.exists(dbConnection, subject, dryRun)) {
        LOGGER.info("Certificate for " + subject + " has already been migrated. Skipping...");
        return;
      }
      certificatesFacade.insertPKICertificate(
          dbConnection,
          ca,
          serialNumber,
          status,
          subject,
          encoded,
          notBefore.toInstant(),
          notAfter.toInstant(),
          dryRun);
    }
  }

  private X509Certificate loadCertificate(Path path) throws IOException, CertificateException {
    PEMParser pemParser = new PEMParser(new FileReader(path.toFile()));
    Object object = pemParser.readObject();
    if (object instanceof X509CertificateHolder) {
      return x509CertificateConverter.getCertificate((X509CertificateHolder) object);
    }
    return null;
  }

  /*
   * Certificate Revocation List
   */
  private void migrateCRLs() throws IOException, CRLException, SQLException {
    // ROOT CA does not have CRL in old setup. It will be automatically be initialized by Hopsworks
    LOGGER.info("Migrating CRL for INTERMEDIATE CA");
    migrateCRL(Paths.get("/srv/hops/certs-dir/intermediate/crl/intermediate.crl.pem"), "INTERMEDIATE");
    // Kubernetes CA does not have CRL in old setup
  }

  private void migrateCRL(Path path, String type) throws IOException, CRLException, SQLException {
    X509CRL crl = loadCRL(path);
    if (crl != null) {
      LOGGER.info("Migrating " + type + " CRL from " + path);
      if (crlFacade.exists(type)) {
        LOGGER.info("CRL for " + type + " has already been migrated. Skipping...");
        return;
      }
      crlFacade.insertCRL(type, crl.getEncoded());
    }
  }

  private X509CRL loadCRL(Path path) throws IOException, CRLException {
    PEMParser pemParser = new PEMParser(new FileReader(path.toFile()));
    Object object = pemParser.readObject();
    if (object instanceof X509CRLHolder) {
      return crlConverter.getCRL((X509CRLHolder) object);
    }
    return null;
  }
}
