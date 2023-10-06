package io.hops.hopsworks.expat.migrations.airflow;

import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.hdfs.FsPermissions;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.core.dto.ProvCoreDTO;
import io.hops.hopsworks.common.provenance.core.dto.ProvTypeDTO;
import io.hops.hopsworks.common.util.ProcessDescriptor;
import io.hops.hopsworks.common.util.ProcessResult;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.executor.ProcessExecutor;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.projects.util.HopsClient;
import io.hops.hopsworks.expat.migrations.projects.util.XAttrException;
import io.hops.hopsworks.expat.migrations.projects.util.XAttrHelper;
import io.hops.hopsworks.persistence.entity.hdfs.inode.Inode;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.persistence.jaxb.JAXBContextFactory;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.eclipse.persistence.oxm.MediaType;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DagsMigration implements MigrateStep {
  private static final Logger LOGGER = LogManager.getLogger(DagsMigration.class);
  private static final String AIRFLOW_USER = "airflow";
  private static final String AIRFLOW_USER_EMAIL = "airflow@hopsworks.ai";
  private static final String AIRFLOW_DATASET_NAME = "Airflow";
  public static final String README_TEMPLATE = "*This is an auto-generated README.md"
      + " file for your Dataset!*\n"
      + "To replace it, go into your DataSet and edit the README.md file.\n"
      + "\n" + "*%s* DataSet\n" + "===\n" + "\n"
      + "## %s";
  private static final String AIRFLOW_DATASET_DESCRIPTION = "Contains airflow dags";
  private Connection connection;
  DistributedFileSystemOps dfso = null;


  String masterPassword = null;
  private String expatPath = null;
  private String hadoopHome = null;
  private String hopsClientUser = null;
  private boolean kubernetesInstalled = false;
  private boolean dryRun;



  private void setup() throws ConfigurationException, SQLException, IOException, MigrationException {
    connection = DbConnectionFactory.getConnection();
    Configuration config = ConfigurationBuilder.getConfiguration();
    expatPath = config.getString(ExpatConf.EXPAT_PATH);
    hopsClientUser = config.getString(ExpatConf.HOPS_CLIENT_USER);
    if (hopsClientUser == null) {
      throw new ConfigurationException(ExpatConf.HOPS_CLIENT_USER + " cannot be null");
    }
    dfso = HopsClient.getDFSO(hopsClientUser);
    hadoopHome = System.getenv("HADOOP_HOME");
    dryRun = config.getBoolean(ExpatConf.DRY_RUN);
    java.nio.file.Path masterPwdPath = Paths.get(config.getString(ExpatConf.MASTER_PWD_FILE_KEY));
    masterPassword = FileUtils.readFileToString(masterPwdPath.toFile(), Charset.defaultCharset());
  }

  @Override
  public void migrate() throws MigrationException {
    try {
      setup();
      Statement stmt = connection.createStatement();
      ResultSet resultSet = stmt.executeQuery("SELECT project.id, projectname,users.username FROM project " +
          "JOIN users ON project.username=users.email;");
      while (resultSet.next()) {
        String projectName = resultSet.getString("projectname");
        Integer projectId = resultSet.getInt("id");
        String username = resultSet.getString("username");
        String hdfsUsername = getHdfsUserName(username, projectName);
        String projectSecret = DigestUtils.sha256Hex(Integer.toString(projectId));
        createAirflowDataset(projectId, projectName, hdfsUsername);
        addAirflowUserToProject(projectId);
        try {
          ProcessDescriptor processDescriptor = new ProcessDescriptor.Builder()
              .addCommand(expatPath + "/bin/dags_migrate.sh")
              .addCommand(projectName)
              .addCommand(projectSecret)
              .addCommand(hdfsUsername)
              .addCommand(hopsClientUser)
              .addCommand(hadoopHome)
              .ignoreOutErrStreams(false)
              .setWaitTimeout(30, TimeUnit.MINUTES)
              .build();

          ProcessResult processResult = ProcessExecutor.getExecutor().execute(processDescriptor);
          if (processResult.getExitCode() == 0) {
            LOGGER.info("Successfully moved dags for project: " + projectName);
          } else if (processResult.getExitCode() == 2) {
            LOGGER.info("Dags directory for project: " + projectName + ", was not configured. So it does " +
                "not have any dags.");
          } else {
            LOGGER.error("Failed to copy dags for project: " + projectName +
                " " + processResult.getStdout());
          }
        } catch (IOException e) {
          // Keep going
          LOGGER.error("Failed to copy dags for project: " + projectName + " " + e.getMessage());
        }
      }
    } catch (Exception ex) {
      throw new MigrationException("Error in migration step " + DagsMigration.class.getSimpleName(), ex);
    } finally {
      close();
    }
  }

  private void addAirflowUserToProject(Integer projectId) throws Exception {
    Statement stmt = connection.createStatement();
    ResultSet resultSet = stmt.executeQuery("SELECT project_id FROM project_team WHERE project_id=" + projectId
        + " AND team_member='" + AIRFLOW_USER_EMAIL  + "';");
    if (!resultSet.next()) {
      connection.setAutoCommit(false);
      PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO " +
          "project_team (project_id, team_member, team_role, added) VALUES (?, ? ,?, ?)");
      preparedStatement.setInt(1, projectId);
      preparedStatement.setString(2, AIRFLOW_USER_EMAIL);
      preparedStatement.setString(3, "Data scientist");
      preparedStatement.setDate(4, new java.sql.Date(System.currentTimeMillis()));
      preparedStatement.execute();
      preparedStatement.close();
      connection.commit();
      connection.setAutoCommit(true);
    }
  }

  private void createAirflowDataset(Integer projectId, String projectName, String hdfsUsername)
      throws IOException, SQLException, MigrationException {
    Path airflowDatasetPath = getAirflowDatasetPath(projectName);
    if (!dfso.exists(airflowDatasetPath) && !dryRun) {
      try {
        LOGGER.info("Creating dataset Airflow in " + projectName);
        FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE, false);
        dfso.mkdir(airflowDatasetPath, fsPermission);
        createAirflowDatasetInDB(projectId);
        setAirflowDatasetPermissions(projectId, projectName, hdfsUsername);
        createAirflowDatasetReadme(hdfsUsername, projectName);
        setAirflowDatasetProvType(projectId, projectName);
      } catch (IOException | SQLException | MigrationException e) {
        LOGGER.error("Failed to create the airflow dataset in project: " + projectName, e);
        if (dfso.exists(airflowDatasetPath)) {
          LOGGER.info("Deleting the airflow dataset in project: " + projectName);
          try {
            dfso.rm(airflowDatasetPath, true);
          } catch (IOException ex) {
            LOGGER.error("Failed to delete the airflow dataset in project: " + projectName, ex);
          }
          deleteAirflowDatasetInDB(projectId);
        }
        throw  e;
      }
    } else {
      LOGGER.info("Airflow dataset already exist for project: " + projectName);
    }
  }

  private void createAirflowDatasetInDB(Integer projectId) throws SQLException {
    // check if it does not already exist
    Statement stmt = connection.createStatement();
    ResultSet resultSet = stmt.executeQuery("SELECT id FROM dataset WHERE projectId=" + projectId
        + " AND inode_name='" + AIRFLOW_DATASET_NAME   + "';");
    if (!resultSet.next()) {
      connection.setAutoCommit(false);
      PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO " +
          "dataset (inode_name, projectId, description, searchable, permission) VALUES (?, ? ,?, ?, ?)");
      preparedStatement.setString(1, AIRFLOW_DATASET_NAME);
      preparedStatement.setInt(2, projectId);
      preparedStatement.setString(3, AIRFLOW_DATASET_DESCRIPTION);
      preparedStatement.setInt(4, 1);
      preparedStatement.setString(5, "EDITABLE");
      preparedStatement.execute();
      preparedStatement.close();
      connection.commit();
      connection.setAutoCommit(true);
    }
  }

  private void deleteAirflowDatasetInDB(Integer projectId) throws SQLException {
    try {
      PreparedStatement statement = connection.prepareStatement("DELETE FROM dataset " +
          "WHERE projectId=? AND inode_name=?");
      statement.setInt(1, projectId);
      statement.setString(2, AIRFLOW_DATASET_NAME);
      statement.execute();
      statement.close();
    } catch (SQLException e) {
      LOGGER.error("Failed to delete airflow database in DB", e);
    }
  }

  private void setAirflowDatasetPermissions(Integer projectId, String projectName, String hdfsUsername)
      throws IOException, MigrationException, SQLException {
    Path airflowDatasetPath = getAirflowDatasetPath(projectName);
    String datasetGroup = getAirflowDatasetGroup(projectName);
    dfso.setOwner(airflowDatasetPath, hdfsUsername, datasetGroup);
    String datasetAclGroup = getAirflowDatasetAclGroup(projectName);
    addGroup(datasetAclGroup);
    addUserToGroup(hdfsUsername, datasetAclGroup);
    dfso.setPermission(airflowDatasetPath, getDefaultDatasetAcl(datasetAclGroup));
    addProjectMembersToAirflowDataset(projectId, projectName);
    // set the airflow acls
    dfso.getFilesystem().modifyAclEntries(airflowDatasetPath, getAirflowAcls());
  }

  private void setAirflowDatasetProvType(Integer projectId, String projectName) throws MigrationException {
    ProvCoreDTO provCore = new ProvCoreDTO(Provenance.Type.META.dto, projectId.longValue());
    try {
      dfso.setMetaStatus(getAirflowDatasetPath(projectName), Inode.MetaStatus.META_ENABLED);
      JAXBContext jaxbContext = jaxbContext();
      Marshaller marshaller = jaxbContext.createMarshaller();
      StringWriter sw = new StringWriter();
      marshaller.marshal(provCore, sw);
      byte[] bProvCore = sw.toString().getBytes();
      XAttrHelper.upsertProvXAttr(dfso, "/Projects/" + projectName, "core", bProvCore);
    } catch (JAXBException | XAttrException | IOException e) {
      throw new MigrationException("Error setting airflow dataset provenance", e);
    }
  }

  private void createAirflowDatasetReadme(String hdfsUsername, String projectName) {
    Path datasetPath = getAirflowDatasetPath(projectName);
    Path readMeFilePath = new Path(datasetPath, "README.md");
    String readmeFile = String.format(README_TEMPLATE, AIRFLOW_DATASET_NAME, AIRFLOW_DATASET_DESCRIPTION);
    try (FSDataOutputStream fsOut = dfso.create(readMeFilePath)) {
      fsOut.writeBytes(readmeFile);
      fsOut.flush();
      dfso.setPermission(readMeFilePath, FsPermissions.rwxrwx___);
      dfso.setOwner(readMeFilePath, hdfsUsername, getAirflowDatasetGroup(projectName));
    } catch (IOException ex) {
      LOGGER.info("Failed to create README for project " + projectName, ex.getMessage());
    }
  }

  private void addProjectMembersToAirflowDataset(Integer projectId, String projectName)
      throws SQLException, IOException {
    // Get the members
    Statement stmt = connection.createStatement();
    ResultSet resultSet = stmt.executeQuery("SELECT  username FROM users JOIN project_team WHERE " +
        "users.email = project_team.team_member AND project_id=" + projectId);
    while (resultSet.next()) {
      String username = resultSet.getString("username");
      String hdfsUserName = getHdfsUserName(username, projectName);
      addUserToGroup(hdfsUserName, getAirflowDatasetGroup(projectName));
    }
  }

  private void addGroup(String group) throws IOException {
    try {
      dfso.addGroup(group);
    } catch (IOException e) {
      if (e.getMessage().contains(group + " already exists")) {
        // continue
        LOGGER.info("Group " + group + " already exist");
      } else {
        throw e;
      }
    }
  }

  private void addUserToGroup(String username, String group) throws IOException {
    try {
      dfso.addUserToGroup(username, group);
    } catch (IOException e) {
      if (e.getMessage().contains(username + " is already part of Group: " + group)) {
        // continue
        LOGGER.info(username + " is already part of Group: " + group);
      } else {
        throw e;
      }
    }
  }

  private Path getAirflowDatasetPath(String projectName) {
    return new Path( "hdfs:///Projects/" + projectName + "/"  + AIRFLOW_DATASET_NAME);
  }

  private String getHdfsUserName(String username, String projectName) {
    return projectName + "__" + username;
  }

  private String getAirflowDatasetGroup(String projectName) {
    return projectName + "__" + AIRFLOW_DATASET_NAME;
  }

  private String getAirflowDatasetAclGroup(String projectName) {
    return getAirflowDatasetGroup(projectName) + "__read";
  }

  private List<AclEntry> getAirflowAcls() {
    List<AclEntry> aclEntries = new ArrayList<>();
    AclEntry accessAcl = new AclEntry.Builder()
        .setType(AclEntryType.USER)
        .setName(AIRFLOW_USER)
        .setScope(AclEntryScope.ACCESS)
        .setPermission(FsAction.READ_EXECUTE)
        .build();
    AclEntry defaultAcl = new AclEntry.Builder()
        .setType(AclEntryType.USER)
        .setName(AIRFLOW_USER)
        .setScope(AclEntryScope.DEFAULT)
        .setPermission(FsAction.READ_EXECUTE)
        .build();
    aclEntries.add(accessAcl);
    aclEntries.add(defaultAcl);
    return aclEntries;
  }

  private List<AclEntry> getDefaultDatasetAcl(String aclGroup) {
    List<AclEntry> aclEntries = new ArrayList<>();
    AclEntry aclEntryUser = new AclEntry.Builder()
        .setType(AclEntryType.USER)
        .setScope(AclEntryScope.ACCESS)
        .setPermission(FsAction.ALL)
        .build();
    aclEntries.add(aclEntryUser);
    AclEntry aclEntryGroup = new AclEntry.Builder()
        .setType(AclEntryType.GROUP)
        .setScope(AclEntryScope.ACCESS)
        .setPermission(FsAction.ALL)
        .build();
    aclEntries.add(aclEntryGroup);
    AclEntry aclEntryDatasetGroup = new AclEntry.Builder()
        .setType(AclEntryType.GROUP)
        .setName(aclGroup)
        .setScope(AclEntryScope.ACCESS)
        .setPermission(FsAction.READ_EXECUTE)
        .build();
    aclEntries.add(aclEntryDatasetGroup);
    AclEntry aclEntryOther = new AclEntry.Builder()
        .setType(AclEntryType.OTHER)
        .setScope(AclEntryScope.ACCESS)
        .setPermission(FsAction.NONE)
        .build();
    aclEntries.add(aclEntryOther);
    AclEntry aclEntryDefault = new AclEntry.Builder()
        .setType(AclEntryType.GROUP)
        .setName(aclGroup)
        .setScope(AclEntryScope.DEFAULT)
        .setPermission(FsAction.READ_EXECUTE)
        .build();
    aclEntries.add(aclEntryDefault);
    return aclEntries;
  }

  private JAXBContext jaxbContext() throws JAXBException {
    Map<String, Object> properties = new HashMap<>();
    properties.put(MarshallerProperties.JSON_INCLUDE_ROOT, false);
    properties.put(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
    JAXBContext context = JAXBContextFactory.createContext(
        new Class[] {
            ProvCoreDTO.class,
            ProvTypeDTO.class
        },
        properties);
    return context;
  }

  protected void close() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException ex) {
        LOGGER.error("failed to close jdbc connection", ex);
      }
    }
    if(dfso != null) {
      dfso.close();
    }
  }

  @Override
  public void rollback() throws RollbackException {
    try {
      this.setup();
    } catch (ConfigurationException | SQLException | IOException | MigrationException e) {
      String errorMsg = "Rollback failed. Could not initialize database connection.";
      LOGGER.error(errorMsg);
      close();
      throw new RollbackException(errorMsg, e);
    }
    projectAirflowDatasetRollback();
    close();
    LOGGER.info("Finished external airflow dags rollback");
  }

  public void projectAirflowDatasetRollback() throws RollbackException {
    try {
      Statement stmt = connection.createStatement();
      ResultSet resultSet = stmt.executeQuery("SELECT project.id, projectname,users.username FROM project " +
          "JOIN users ON project.username=users.email;");
      while (resultSet.next()) {
        String projectName = resultSet.getString("projectname");
        LOGGER.info("Deleting airflow dataset for project: " + projectName);
        dfso.rm(getAirflowDatasetPath(projectName), true);
      }
    } catch (SQLException | IOException e) {
      throw new RollbackException("Failed airflow dataset rollback", e);
    }
  }
}
