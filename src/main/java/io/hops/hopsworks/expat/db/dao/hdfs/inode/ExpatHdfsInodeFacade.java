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
package io.hops.hopsworks.expat.db.dao.hdfs.inode;

import io.hops.hopsworks.common.util.HopsUtils;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.ExpatAbstractFacade;
import io.hops.hopsworks.expat.migrations.MigrationException;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ExpatHdfsInodeFacade extends ExpatAbstractFacade<ExpatHdfsInode> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExpatHdfsInodeFacade.class);
  
  private final static String FIND_ROOT_BY_NAME = "SELECT * FROM hops.hdfs_inodes i WHERE parent_id = ? "
    + "AND name = ? AND partition_id = ?";

  private final static String FIND_INODE_BY_ID = "SELECT * FROM hops.hdfs_inodes i WHERE id = ? ";
  
  private Connection connection;
  private PreparedStatement findRootByName;
  private PreparedStatement findInodeById;
  
  protected ExpatHdfsInodeFacade(Class<ExpatHdfsInode> entityClass) throws SQLException, ConfigurationException {
    super(entityClass);
    this.connection = DbConnectionFactory.getConnection();
  }
  
  public ExpatHdfsInodeFacade(Class<ExpatHdfsInode> entityClass, Connection connection) {
    super(entityClass);
    this.connection = connection;
  }
  
  @Override
  public Connection getConnection() {
    return this.connection;
  }
  
  @Override
  public String findAllQuery() {
    return "SELECT * FROM hops.hdfs_inodes";
  }
  
  @Override
  public String findByIdQuery() {
    return "SELECT * FROM hops.hdfs_inodes WHERE id = ?";
  }
  
  public ExpatHdfsInode find(Long id) throws IllegalAccessException, SQLException, InstantiationException {
    return this.findById(id, JDBCType.BIGINT);
  }
  
  public ExpatHdfsInode getRootNode(String name) throws SQLException, MigrationException {
    // LOGGER.info("getRootNode: " + name);
    long partitionId = HopsUtils.calculatePartitionId(HopsUtils.ROOT_INODE_ID, name, HopsUtils.ROOT_DIR_DEPTH + 1);
    return findByInodePK(HopsUtils.ROOT_INODE_ID, name, partitionId);
  }

  public ExpatHdfsInode findInodeById(long inodeId)
    throws SQLException, MigrationException {
    findInodeById = connection.prepareStatement(FIND_INODE_BY_ID);
    findInodeById.setLong(1, inodeId);

    List<ExpatHdfsInode> resultList = new ArrayList<>();
    ResultSet result = findInodeById.executeQuery();
    while (result.next()) {
      resultList.add(new ExpatHdfsInode().getEntity(result));
    }

    findInodeById.close();

    if (resultList.size() == 1) {
      return resultList.get(0);
    } else if (resultList.size() > 1) {
      LOGGER.warn("Found more than one inode with id: " + inodeId);
      throw new MigrationException("Found more than one root inode with name: " + inodeId);
    } else {
      LOGGER.warn("Found no inode with id: " + inodeId);
      return null;
    }
  }
  
  public ExpatHdfsInode findByInodePK(long parentId, String name, long partitionId)
    throws SQLException, MigrationException {
    // LOGGER.info("findByInodePK: parentId: " + parentId + " name: " + name + " partitionId: " + partitionId);
    findRootByName = connection.prepareStatement(FIND_ROOT_BY_NAME);
    findRootByName.setLong(1, parentId);
    findRootByName.setString(2, name);
    findRootByName.setLong(3, partitionId);
  
    List<ExpatHdfsInode> resultList = new ArrayList<>();
    ResultSet result = findRootByName.executeQuery();
    while (result.next()) {
      resultList.add(new ExpatHdfsInode().getEntity(result));
    }
  
    findRootByName.close();
  
    if (resultList.size() == 1) {
      return resultList.get(0);
    } else if (resultList.size() > 1) {
      LOGGER.warn("Found more than one root inode with name: " + name);
      throw new MigrationException("Found more than one root inode with name: " + name);
    } else {
      LOGGER.warn("Found no root inode with name: " + name);
      return null;
    }
  }
}
