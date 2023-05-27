/*
 * This file is part of Expat
 * Copyright (C) 2020, Logical Clocks AB. All rights reserved
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
package io.hops.hopsworks.expat.migrations.dataset;

import io.hops.hopsworks.common.constants.auth.AllowedRoles;
import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.hdfs.FsPermissions;
import io.hops.hopsworks.common.hdfs.HdfsUsersController;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.dataset.ExpatDataset;
import io.hops.hopsworks.expat.db.dao.dataset.ExpatDatasetFacade;
import io.hops.hopsworks.expat.db.dao.dataset.ExpatDatasetSharedWith;
import io.hops.hopsworks.expat.db.dao.dataset.ExpatDatasetSharedWithFacade;
import io.hops.hopsworks.expat.db.dao.hdfs.inode.ExpatHdfsInode;
import io.hops.hopsworks.expat.db.dao.hdfs.inode.ExpatHdfsInodeFacade;
import io.hops.hopsworks.expat.db.dao.hdfs.user.ExpatHdfsGroup;
import io.hops.hopsworks.expat.db.dao.hdfs.user.ExpatHdfsGroupFacade;
import io.hops.hopsworks.expat.db.dao.hdfs.user.ExpatHdfsUser;
import io.hops.hopsworks.expat.db.dao.hdfs.user.ExpatHdfsUserFacade;
import io.hops.hopsworks.expat.db.dao.project.ExpatProject;
import io.hops.hopsworks.expat.db.dao.project.ExpatProjectFacade;
import io.hops.hopsworks.expat.db.dao.project.ExpatProjectMember;
import io.hops.hopsworks.expat.db.dao.project.ExpatProjectMemberFacade;
import io.hops.hopsworks.expat.migrations.projects.util.HopsClient;
import io.hops.hopsworks.persistence.entity.dataset.DatasetAccessPermission;
import io.hops.hopsworks.persistence.entity.project.team.ProjectRoleTypes;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class FixDatasetPermissionHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixDatasetPermission.class);
  
  private Connection connection;
  private ExpatProjectFacade projectFacade;
  private ExpatDatasetFacade datasetFacade;
  private ExpatDatasetSharedWithFacade datasetSharedWithFacade;
  private ExpatProjectMemberFacade projectMemberFacade;
  private ExpatHdfsGroupFacade hdfsGroupFacade;
  private ExpatHdfsUserFacade hdfsUserFacade;
  private ExpatHdfsInodeFacade inodeFacade;
  private String hopsUser;
  
  private boolean dryrun;
  
  public FixDatasetPermissionHelper() {
  }
  
  public void setup() throws SQLException, ConfigurationException {
    Configuration conf = ConfigurationBuilder.getConfiguration();
    this.hopsUser = conf.getString(ExpatConf.HOPS_CLIENT_USER);
    this.connection = DbConnectionFactory.getConnection();
    this.projectFacade = new ExpatProjectFacade(ExpatProject.class, this.connection);
    this.datasetFacade = new ExpatDatasetFacade(ExpatDataset.class, this.connection);
    this.datasetSharedWithFacade = new ExpatDatasetSharedWithFacade(ExpatDatasetSharedWith.class, this.connection);
    this.projectMemberFacade = new ExpatProjectMemberFacade(ExpatProjectMember.class, this.connection);
    this.hdfsGroupFacade = new ExpatHdfsGroupFacade(ExpatHdfsGroup.class, this.connection);
    this.hdfsUserFacade = new ExpatHdfsUserFacade(ExpatHdfsUser.class, this.connection);
    this.inodeFacade = new ExpatHdfsInodeFacade(ExpatHdfsInode.class, this.connection);
    this.dryrun = conf.getBoolean(ExpatConf.DRY_RUN);
  }
  
  public void fixAllProjects() throws SQLException, InstantiationException, IOException, IllegalAccessException {
    DistributedFileSystemOps dfso = null;
    try {
      dfso = HopsClient.getDFSO(this.hopsUser);
      fixPermission(dfso);
    } finally {
      if (dfso != null) {
        dfso.close();
      }
    }
  }
  
  public void rollbackAllProject() throws SQLException, InstantiationException, IOException, IllegalAccessException {
    DistributedFileSystemOps dfso = null;
    try {
      dfso = HopsClient.getDFSO(this.hopsUser);
      rollbackPermission(dfso);
    } finally {
      if (dfso != null) {
        dfso.close();
      }
    }
  }
  
  public void close() {
    if (this.connection != null) {
      try {
        this.connection.close();
      } catch (SQLException e) {
        LOGGER.warn("Failed to close database connection. {}", e.getMessage());
      }
    }
  }
  
  private void fixPermission(DistributedFileSystemOps dfso) throws IllegalAccessException, SQLException,
    InstantiationException, IOException {
    List<ExpatProject> projects = this.projectFacade.findAll();
    for (int i = 0; i < projects.size(); i++) {
      LOGGER.info("====================== Fixing project={} ===========================", projects.get(i).getName());
      fixPermission(projects.get(i), dfso);
      LOGGER.info("====================== Done Fixing project={} ======================", projects.get(i).getName());
    }
    LOGGER.info("Fixed {} projects.", projects.size());
  }
  
  private void rollbackPermission(DistributedFileSystemOps dfso) throws IllegalAccessException, SQLException,
    InstantiationException, IOException {
    List<ExpatProject> projects = this.projectFacade.findAll();
    for (int i = 0; i < projects.size(); i++) {
      LOGGER.info("====================== Rollback project={} ===========================", projects.get(i).getName());
      rollbackPermission(projects.get(i), dfso);
      LOGGER.info("====================== Done Rolling back project={} ==================", projects.get(i).getName());
    }
    LOGGER.info("Rolledback {} projects.", projects.size());
  }
  
  private void rollbackPermission(ExpatProject project, DistributedFileSystemOps dfso) throws IllegalAccessException,
    SQLException, InstantiationException, IOException {
    if (isUnderRemoval(project)) {
      LOGGER.info("Skipped rollback permission for project={} because it is under removal.", project.getName());
      return;
    }
    LOGGER.info("Rolling back datasets in project={}...", project.getName());
    List<ExpatDataset> datasetList = this.datasetFacade.findByProjectId(project.getId());
    for (ExpatDataset dataset : datasetList) {
      rollbackDataset(dataset, project, dfso);
    }
  }
  
  private void rollbackDataset(ExpatDataset dataset, ExpatProject project, DistributedFileSystemOps dfso)
    throws IllegalAccessException, SQLException, InstantiationException, IOException {
    String datasetGroup = getHdfsGroupName(project.getName(), dataset);
    String datasetAclGroup = getHdfsAclGroupName(project.getName(), dataset);
    ExpatHdfsGroup hdfsDatasetGroup = this.hdfsGroupFacade.findByName(datasetGroup);
    ExpatHdfsGroup hdfsDatasetAclGroup = this.hdfsGroupFacade.findByName(datasetAclGroup);
    ExpatHdfsInode inode = this.inodeFacade.find(dataset.getInodeId());
    ExpatHdfsUser hdfsUser = this.hdfsUserFacade.find(inode.getHdfsUser());
    Path path = new Path(getPath(inode));
    if (hdfsDatasetGroup == null) {
      LOGGER.info("Failed to get group={} for dataset in path={}", datasetGroup, path.toString());
      throw new IllegalStateException("Failed to get group=" + datasetGroup);
    }
    
    if (hdfsDatasetAclGroup != null) {
      removeGroup(hdfsDatasetAclGroup, dfso);
    }
    
    rollbackPermission(dataset, path, inode, dfso);
    
    List<ExpatProjectMember> datasetTeamCollection = new ArrayList<>();
    List<ExpatProjectMember> projectMembers = this.projectMemberFacade.findByProjectId(project.getId());
    datasetTeamCollection.addAll(projectMembers);
    
    List<ExpatDatasetSharedWith> datasetSharedWithList = this.datasetSharedWithFacade.findByDatasetId(dataset.getId());
    for (ExpatDatasetSharedWith datasetSharedWith : datasetSharedWithList) {
      if (datasetSharedWith.isAccepted()) {
        projectMembers = this.projectMemberFacade.findByProjectId(datasetSharedWith.getProject());
        datasetTeamCollection.addAll(projectMembers);
      }
    }
    addBackToGroup(datasetTeamCollection, hdfsDatasetGroup, hdfsUser, dfso);
  }
  
  private void addBackToGroup(List<ExpatProjectMember> datasetTeamCollection, ExpatHdfsGroup hdfsDatasetGroup,
    ExpatHdfsUser hdfsUser, DistributedFileSystemOps dfso) throws SQLException, InstantiationException,
    IllegalAccessException, IOException {
    List<ExpatHdfsUser> hdfsDatasetGroupMembers = getMembers(hdfsDatasetGroup);
    for (ExpatProjectMember projectTeam : datasetTeamCollection) {
      addBackToGroup(projectTeam, hdfsDatasetGroupMembers, hdfsDatasetGroup, hdfsUser, dfso);
    }
  }
  
  private void addBackToGroup(ExpatProjectMember projectTeam, List<ExpatHdfsUser> hdfsDatasetGroupMembers,
    ExpatHdfsGroup hdfsDatasetGroup, ExpatHdfsUser owner, DistributedFileSystemOps dfso) throws SQLException,
    InstantiationException, IOException, IllegalAccessException {
    if (projectTeam.getUsername().equals("srvmanager")) {
      return;//Does this user need to be in groups?
    }
    String hdfsUsername = getHdfsUserName(projectTeam.getProjectName(), projectTeam.getUsername());
    ExpatHdfsUser hdfsUser = getOrCreateUser(hdfsUsername, dfso);
    if (owner != null && owner.equals(hdfsUser)) {
      return;
    }
    if (!hdfsDatasetGroupMembers.contains(hdfsUser)) {
      addToGroup(hdfsUser, hdfsDatasetGroup, dfso);
    }
  }
  
  private void rollbackPermission(ExpatDataset dataset, Path path, ExpatHdfsInode inode,
    DistributedFileSystemOps dfso) throws IOException {
    FsPermission fsPermission = FsPermission.createImmutable(inode.getPermission());
    FsPermission fsPermissionDefault = FsPermissions.rwxr_x___;
    FsPermission fsPermissionServiceDatasetDefault = FsPermissions.rwxrwx___;
    FsPermission fsPermissionServiceDatasetDefaultT = FsPermissions.rwxrwx___T;
    if (isDefaultDataset(dataset.getName())) {
      if (dataset.getName().endsWith(".db") || dataset.getName().equals("TourData") ||
        dataset.getName().equals("TestJob") || dataset.getName().equals(Settings.BaseDataset.LOGS.getName())) {
        setPermission(fsPermission, fsPermissionServiceDatasetDefaultT, path, dfso);
      } else {
        setPermission(fsPermission, fsPermissionServiceDatasetDefault, path, dfso);
      }
    } else {
      setPermission(fsPermission, fsPermissionDefault, path, dfso);
    }
    
  }
  
  private void setPermission(FsPermission currentPermission, FsPermission fsPermission, Path path,
    DistributedFileSystemOps dfso) throws IOException {
    if (!dryrun && !currentPermission.equals(fsPermission)) {
      dfso.setPermission(path, fsPermission);
    }
    if (!currentPermission.equals(fsPermission)) {
      LOGGER.info("Rolling back permission from={} to={} for dataset in path={}.", currentPermission, fsPermission,
        path);
    }
  }
  
  private void fixPermission(ExpatProject expatProject, DistributedFileSystemOps dfso) throws IllegalAccessException,
    SQLException, InstantiationException, IOException {
    if (isUnderRemoval(expatProject)) {
      LOGGER.info("Skipped fix permission for project={} because it is under removal.", expatProject.getName());
      return;
    }
    LOGGER.info("Fixing datasets in project={}...", expatProject.getName());
    List<ExpatDataset> datasetList = this.datasetFacade.findByProjectId(expatProject.getId());
    for (ExpatDataset dataset : datasetList) {
      fixDataset(dataset, expatProject, dfso);
    }
  }
  
  private void fixDataset(ExpatDataset dataset, ExpatProject expatProject, DistributedFileSystemOps dfso )
    throws IllegalAccessException, SQLException, InstantiationException, IOException {
    LOGGER.info("Fixing Dataset={} in project={}", dataset.getName(), expatProject.getName());
    fixPermission(expatProject, dataset, dfso);
  }
  
  private void fixPermission(ExpatProject expatProject, ExpatDataset dataset, DistributedFileSystemOps dfso)
    throws IllegalAccessException, SQLException, InstantiationException, IOException {
    String datasetGroup = getHdfsGroupName(expatProject.getName(), dataset);
    String datasetAclGroup = getHdfsAclGroupName(expatProject.getName(), dataset);
    ExpatHdfsGroup hdfsDatasetGroup = getOrCreateGroup(datasetGroup, dfso);
    ExpatHdfsGroup hdfsDatasetAclGroup = getOrCreateGroup(datasetAclGroup, dfso);
    ExpatHdfsInode inode = this.inodeFacade.find(dataset.getInodeId());
    ExpatHdfsUser hdfsUser = this.hdfsUserFacade.find(inode.getHdfsUser());
    Path path = new Path(getPath(inode));
    if (!dryrun && hdfsDatasetGroup == null) {
      LOGGER.info("Failed to add group={} for dataset in path={}", datasetGroup, path.toString());
      throw new IllegalStateException("Failed to get group=" + datasetGroup);
    } else if (hdfsDatasetGroup == null) {
      hdfsDatasetGroup = new ExpatHdfsGroup();
      hdfsDatasetGroup.setName(datasetGroup);
    }
    if (!dryrun && hdfsDatasetAclGroup == null) {
      LOGGER.info("Failed to add group={} for dataset in path={}", datasetAclGroup, path.toString());
      throw new IllegalStateException("Failed to get group=" + datasetAclGroup);
    } else if (hdfsDatasetAclGroup == null) {
      hdfsDatasetAclGroup = new ExpatHdfsGroup();
      hdfsDatasetAclGroup.setName(datasetAclGroup);
    }
    
    setDatasetAcl(datasetAclGroup, path, dfso);
  
    dataset = setPermission(dataset);
    
    List<ExpatProjectMember> datasetTeamCollection = new ArrayList<>();
    List<ExpatProjectMember> projectMembers = this.projectMemberFacade.findByProjectId(expatProject.getId());
    datasetTeamCollection.addAll(projectMembers);
    testFsPermission(dataset, path, inode, dfso);
    testAndFixPermissionForAllMembers(projectMembers, dfso, hdfsDatasetGroup, hdfsDatasetAclGroup, hdfsUser,
      DatasetAccessPermission.valueOf(dataset.getPermission()));
    List<ExpatDatasetSharedWith> datasetSharedWithList = this.datasetSharedWithFacade.findByDatasetId(dataset.getId());
    for (ExpatDatasetSharedWith datasetSharedWith : datasetSharedWithList) {
      setPermission(dataset,datasetSharedWith );
      if (datasetSharedWith.isAccepted()) {
        projectMembers = this.projectMemberFacade.findByProjectId(datasetSharedWith.getProject());
        datasetTeamCollection.addAll(projectMembers);
        testAndFixPermissionForAllMembers(projectMembers, dfso, hdfsDatasetGroup, hdfsDatasetAclGroup, null,
          DatasetAccessPermission.valueOf(datasetSharedWith.getPermission()));
      }
    }
    testAndRemoveUsersFromGroup(datasetTeamCollection, hdfsDatasetGroup, hdfsDatasetAclGroup, hdfsUser, dfso);
  }
  
  private ExpatDataset setPermission(ExpatDataset dataset) throws SQLException, InstantiationException,
    IllegalAccessException {
    if (dataset.getPublicDs() > 0 && !DatasetAccessPermission.READ_ONLY.getValue().equals(dataset.getPermission())) {
      if (!dryrun) {
        datasetFacade.updatePermission(dataset.getId(), DatasetAccessPermission.READ_ONLY.getValue());
        dataset = datasetFacade.find(dataset.getId());
      } else {
        dataset.setPermission(DatasetAccessPermission.READ_ONLY.getValue());
      }
      LOGGER.info("Updated dataset permission for public dataset id={} to read only.", dataset.getId());
    } else if (isDefaultDataset(dataset.getName()) &&
      !DatasetAccessPermission.EDITABLE.getValue().equals(dataset.getPermission())) {
      if (!dryrun) {
        datasetFacade.updatePermission(dataset.getId(), DatasetAccessPermission.EDITABLE.getValue());
        dataset = datasetFacade.find(dataset.getId());
      } else {
        dataset.setPermission(DatasetAccessPermission.EDITABLE.getValue());
      }
      LOGGER.info("Updated dataset permission for default dataset id={} to editable.", dataset.getId());
    }
    return dataset;
  }
  
  private boolean isDefaultDataset(String datasetName) {
    for (Settings.BaseDataset baseDataset : Settings.BaseDataset.values()) {
      if (baseDataset.getName().equals(datasetName)) {
        return true;
      }
    }
    for (Settings.ServiceDataset serviceDataset : Settings.ServiceDataset.values()) {
      if (serviceDataset.getName().equals(datasetName)) {
        return true;
      }
    }
    return datasetName.contains(Settings.ServiceDataset.TRAININGDATASETS.getName()) || datasetName.endsWith(".db") ||
      datasetName.equals("TestJob") || datasetName.equals("TourData");
  }
  
  private void setPermission(ExpatDataset dataset, ExpatDatasetSharedWith datasetSharedWith) throws SQLException,
    InstantiationException, IllegalAccessException {
    if (dataset.getPublicDs() > 0 &&
      !DatasetAccessPermission.READ_ONLY.getValue().equals(datasetSharedWith.getPermission())) {
      if (!dryrun) {
        datasetSharedWithFacade
          .updatePermission(datasetSharedWith.getId(), DatasetAccessPermission.READ_ONLY.getValue());
        datasetSharedWith = datasetSharedWithFacade.find(datasetSharedWith.getId());
      }
      LOGGER.info("Updated datasetSharedWith permission for shared public dataset id={}", datasetSharedWith.getId());
    }
  }
  
  private void testAndRemoveUsersFromGroup(List<ExpatProjectMember> datasetTeamCollection,
    ExpatHdfsGroup hdfsDatasetGroup, ExpatHdfsGroup hdfsDatasetAclGroup, ExpatHdfsUser hdfsUser,
    DistributedFileSystemOps dfso) throws IllegalAccessException, SQLException, InstantiationException, IOException {
    //Remove if member is not in team collection
    List<ExpatHdfsUser> members =  !dryrun? this.hdfsUserFacade.getUsersInGroup(hdfsDatasetGroup) : new ArrayList<>();
    for (ExpatHdfsUser hdfsUsers : members) {
      testAndRemoveMember(datasetTeamCollection, hdfsDatasetGroup, hdfsUsers, hdfsUser, dfso);
    }
    List<ExpatHdfsUser> aclGroupMembers =  !dryrun? this.hdfsUserFacade.getUsersInGroup(hdfsDatasetAclGroup) :
      new ArrayList<>();
    for (ExpatHdfsUser hdfsUsers : aclGroupMembers) {
      testAndRemoveMember(datasetTeamCollection, hdfsDatasetAclGroup, hdfsUsers, hdfsUser, dfso);
    }
  }
  
  private void testAndRemoveMember(List<ExpatProjectMember> datasetTeamCollection, ExpatHdfsGroup hdfsDatasetGroup,
    ExpatHdfsUser hdfsUser, ExpatHdfsUser owner, DistributedFileSystemOps dfso) throws IOException {
    if (hdfsUser == null || hdfsUser.equals(owner)) {
      return;
    }
    boolean found = false;
    for (ExpatProjectMember projectTeam : datasetTeamCollection) {
      String hdfsUsername = getHdfsUserName(projectTeam.getProjectName(), projectTeam.getUsername());
      if (hdfsUser.getName().equals(hdfsUsername)) {
        found = true;
      }
    }
    if (!found) {
      removeFromGroup(hdfsUser, hdfsDatasetGroup, dfso);
    }
  }
  
  private void removeFromGroup(ExpatHdfsUser hdfsUser, ExpatHdfsGroup hdfsDatasetGroup, DistributedFileSystemOps dfso)
    throws IOException {
    if (!dryrun) {
      dfso.removeUserFromGroup(hdfsUser.getName(), hdfsDatasetGroup.getName());
    }
    LOGGER.info("Removed user={} from group={}", hdfsUser.getName(), hdfsDatasetGroup.getName());
  }
  
  private void addToGroup(ExpatHdfsUser hdfsUser, ExpatHdfsGroup hdfsDatasetGroup, DistributedFileSystemOps dfso)
    throws IOException {
    if (!dryrun) {
      dfso.addUserToGroup(hdfsUser.getName(), hdfsDatasetGroup.getName());
    }
    LOGGER.info("Added user={} to group={}", hdfsUser.getName(), hdfsDatasetGroup.getName());
  }
  
  private void removeGroup(ExpatHdfsGroup group, DistributedFileSystemOps dfso) throws IOException {
    if (!dryrun) {
      dfso.removeGroup(group.getName());
    }
    LOGGER.info("Remove group={}", group.getName());
  }
  
  private String getHdfsUserName(String projectName, String username) {
    return projectName + HdfsUsersController.USER_NAME_DELIMITER + username;
  }
  
  private void testAndFixPermissionForAllMembers(List<ExpatProjectMember> projectMembers, DistributedFileSystemOps dfso,
    ExpatHdfsGroup hdfsDatasetGroup, ExpatHdfsGroup hdfsDatasetAclGroup, ExpatHdfsUser hdfsUser,
    DatasetAccessPermission permission) throws SQLException, InstantiationException, IllegalAccessException,
    IOException {
    List<ExpatHdfsUser> hdfsDatasetGroupMembers = getMembers(hdfsDatasetGroup);
    List<ExpatHdfsUser> hdfsDatasetAclGroupMembers = getMembers(hdfsDatasetAclGroup);
    for (ExpatProjectMember projectTeam : projectMembers) {
      testAndFixPermission(projectTeam, dfso, hdfsDatasetGroupMembers, hdfsDatasetAclGroupMembers, hdfsDatasetGroup,
        hdfsDatasetAclGroup, hdfsUser, permission);
    }
  }
  
  private void testAndFixPermission(ExpatProjectMember projectTeam, DistributedFileSystemOps dfso,
    List<ExpatHdfsUser> hdfsDatasetGroupMembers, List<ExpatHdfsUser> hdfsDatasetAclGroupMembers,
    ExpatHdfsGroup hdfsDatasetGroup, ExpatHdfsGroup hdfsDatasetAclGroup, ExpatHdfsUser owner,
    DatasetAccessPermission permission) throws SQLException, InstantiationException, IOException,
    IllegalAccessException {
    if (projectTeam.getUsername().equals("srvmanager")) {
      return;//Does this user need to be in groups?
    }
    String hdfsUsername = getHdfsUserName(projectTeam.getProjectName(), projectTeam.getUsername());
    ExpatHdfsUser hdfsUser = getOrCreateUser(hdfsUsername, dfso);
    if (owner != null && owner.equals(hdfsUser)) {
      return;
    }
    switch (permission) {
      case EDITABLE:
        if (!hdfsDatasetGroupMembers.contains(hdfsUser)) {
          addToGroup(hdfsUser, hdfsDatasetGroup, dfso);
        }
        if (hdfsDatasetAclGroupMembers.contains(hdfsUser)) {
          removeFromGroup(hdfsUser, hdfsDatasetAclGroup, dfso);
        }
        break;
      case READ_ONLY:
        if (hdfsDatasetGroupMembers.contains(hdfsUser)) {
          removeFromGroup(hdfsUser, hdfsDatasetGroup, dfso);
        }
        if (!hdfsDatasetAclGroupMembers.contains(hdfsUser)) {
          addToGroup(hdfsUser, hdfsDatasetAclGroup, dfso);
        }
        break;
      case EDITABLE_BY_OWNERS:
        if (AllowedRoles.DATA_OWNER.equals(projectTeam.getTeamRole())) {
          if (!hdfsDatasetGroupMembers.contains(hdfsUser)) {
            addToGroup(hdfsUser, hdfsDatasetGroup, dfso);
          }
          if (hdfsDatasetAclGroupMembers.contains(hdfsUser)) {
            removeFromGroup(hdfsUser, hdfsDatasetAclGroup, dfso);
          }
        } else {
          if (hdfsDatasetGroupMembers.contains(hdfsUser)) {
            removeFromGroup(hdfsUser, hdfsDatasetGroup, dfso);
          }
          if (!hdfsDatasetAclGroupMembers.contains(hdfsUser)) {
            addToGroup(hdfsUser, hdfsDatasetAclGroup, dfso);
          }
        }
        break;
      default:
        LOGGER.warn("Found a dataset with an unknown permission: group={}, project={}", hdfsDatasetGroup,
          projectTeam.getProjectName());
    }
  }
  
  private List<ExpatHdfsUser> getMembers(ExpatHdfsGroup group) throws IllegalAccessException, SQLException,
    InstantiationException {
    return this.hdfsUserFacade.getUsersInGroup(group);
  }
  
  private void testFsPermission(ExpatDataset dataset, Path path, ExpatHdfsInode inode, DistributedFileSystemOps dfso)
    throws IOException {
    FsPermission fsPermission = FsPermission.createImmutable(inode.getPermission());
    FsPermission fsPermissionReadOnly = FsPermission.createImmutable((short) 00550);
    FsPermission fsPermissionReadOnlyT = FsPermission.createImmutable((short) 01550);
    FsPermission fsPermissionDefault = FsPermissions.rwxrwx___;
    FsPermission fsPermissionDefaultT = FsPermissions.rwxrwx___T;
    if (dataset.getPublicDs() > 0 && !fsPermissionReadOnly.equals(fsPermission) &&
      !fsPermissionReadOnlyT.equals(fsPermission)) {
      makeImmutable(path, dfso);
      LOGGER.info("Make public Dataset at path={} immutable.", path.toString());
    }
    if (dataset.getPublicDs() == 0 && !fsPermissionDefault.equals(fsPermission) &&
      !fsPermissionDefaultT.equals(fsPermission)) {
      undoImmutable(path, dfso);
      LOGGER.info("Set default permission={} for Dataset at path={}.", fsPermissionDefault, path.toString());
    }
  }
  
  private void makeImmutable(Path path, DistributedFileSystemOps dfso) throws IOException {
    List<AclEntry> aclEntries = new ArrayList<>();
    AclEntry aclEntryUser = new AclEntry.Builder()
      .setType(AclEntryType.USER)
      .setScope(AclEntryScope.ACCESS)
      .setPermission(FsAction.READ_EXECUTE)
      .build();
    aclEntries.add(aclEntryUser);
    AclEntry aclEntryGroup = new AclEntry.Builder()
      .setType(AclEntryType.GROUP)
      .setScope(AclEntryScope.ACCESS)
      .setPermission(FsAction.READ_EXECUTE)
      .build();
    aclEntries.add(aclEntryGroup);
    AclEntry aclEntryOther = new AclEntry.Builder()
      .setType(AclEntryType.OTHER)
      .setScope(AclEntryScope.ACCESS)
      .setPermission(FsAction.NONE)
      .build();
    aclEntries.add(aclEntryOther);
    addAcl(aclEntries, path, dfso);
  }
  
  private void undoImmutable(Path path, DistributedFileSystemOps dfso) throws IOException {
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
    AclEntry aclEntryOther = new AclEntry.Builder()
      .setType(AclEntryType.OTHER)
      .setScope(AclEntryScope.ACCESS)
      .setPermission(FsAction.NONE)
      .build();
    aclEntries.add(aclEntryOther);
    addAcl(aclEntries, path, dfso);
  }
  
  private void setDatasetAcl(String aclGroup, Path path, DistributedFileSystemOps dfso) throws IOException {
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
    addAcl(aclEntries, path, dfso);
  }
  
  private void addAcl(List<AclEntry> aclEntries, Path path, DistributedFileSystemOps dfso) throws IOException {
    if (!dryrun) {
      dfso.getFilesystem().setAcl(path, aclEntries);
    }
    LOGGER.info("Adding acl={} for Dataset at path={}", aclEntries, path.toString());
  }
  
  private String getPath(ExpatHdfsInode inode) throws IllegalAccessException, SQLException, InstantiationException {
    StringBuilder path = new StringBuilder(inode.getName());
    ExpatHdfsInode parentInode = this.inodeFacade.find(inode.getParentId());
    while (parentInode != null) {
      path.insert(0, parentInode.getName() + File.separator);
      parentInode = this.inodeFacade.find(parentInode.getParentId());
    }
    return path.toString();
  }
  
  private ExpatHdfsGroup getOrCreateGroup(String group, DistributedFileSystemOps dfso) throws IllegalAccessException,
    SQLException, InstantiationException, IOException {
    ExpatHdfsGroup hdfsGroup = this.hdfsGroupFacade.findByName(group);
    if (!dryrun && hdfsGroup == null) {
      addGroup(group, dfso);
      hdfsGroup = this.hdfsGroupFacade.findByName(group);
    }
    return hdfsGroup;
  }
  
  private ExpatHdfsUser getOrCreateUser(String username, DistributedFileSystemOps dfso) throws IllegalAccessException,
    SQLException, InstantiationException, IOException {
    ExpatHdfsUser hdfsUser = this.hdfsUserFacade.findByName(username);
    if (!dryrun && hdfsUser == null) {
      addUser(username, dfso);
      hdfsUser = this.hdfsUserFacade.findByName(username);
    }
    return hdfsUser;
  }
  
  private ExpatHdfsGroup addGroup(String group, DistributedFileSystemOps dfso)
    throws IOException, IllegalAccessException, SQLException, InstantiationException {
    ExpatHdfsGroup hdfsGroup = null;
    if (!dryrun) {
      dfso.addGroup(group);
      hdfsGroup = this.hdfsGroupFacade.findByName(group);
    }
    LOGGER.info("Found and fixed a missing group: group={}", group);
    return hdfsGroup;
  }
  
  private ExpatHdfsUser addUser(String username, DistributedFileSystemOps dfso)
    throws IOException, IllegalAccessException, SQLException, InstantiationException {
    ExpatHdfsUser hdfsUser = null;
    if (!dryrun) {
      dfso.addGroup(username);
      hdfsUser = this.hdfsUserFacade.findByName(username);
    }
    LOGGER.info("Found and fixed a missing user: username={}", username);
    return hdfsUser;
  }
  
  private String getHdfsAclGroupName(String projectName, ExpatDataset dataset) {
    return getHdfsGroupName(projectName, dataset) + HdfsUsersController.USER_NAME_DELIMITER + "read";
  }
  
  private String getHdfsGroupName(String projectName, ExpatDataset dataset) {
    return projectName + HdfsUsersController.USER_NAME_DELIMITER + dataset.getName();
  }
  
  private boolean isUnderRemoval(ExpatProject expatProject) throws IllegalAccessException, SQLException,
    InstantiationException {
    List<ExpatProjectMember> members = this.projectMemberFacade.findByProjectId(expatProject.getId());
    for (ExpatProjectMember member : members) {
      if (ProjectRoleTypes.UNDER_REMOVAL.equals(member.getTeamRole())) {
        return true;
      }
    }
    return false;
  }
}
