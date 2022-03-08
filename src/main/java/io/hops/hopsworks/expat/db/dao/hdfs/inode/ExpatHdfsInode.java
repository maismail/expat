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

import io.hops.hopsworks.expat.db.dao.ExpatAbstractEntity;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ExpatHdfsInode extends ExpatAbstractEntity<ExpatHdfsInode> {
  private Long id;
  private Long parentId;
  private String name;
  private BigDecimal modificationTime;
  private BigDecimal accessTime;
  private Integer hdfsUser;
  private Integer hdfsGroup;
  private short permission;
  private String symlink;
  private boolean quotaEnabled;
  private boolean underConstruction;
  private String metaStatus;
  private boolean dir;
  private int childrenNum;
  private long size;
  private long partitionId;
  
  public ExpatHdfsInode() {
  }
  
  public Long getId() {
    return id;
  }
  
  public void setId(Long id) {
    this.id = id;
  }
  
  public Long getParentId() {
    return parentId;
  }
  
  public void setParentId(Long parentId) {
    this.parentId = parentId;
  }
  
  public String getName() {
    return name;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public BigDecimal getModificationTime() {
    return modificationTime;
  }
  
  public void setModificationTime(BigDecimal modificationTime) {
    this.modificationTime = modificationTime;
  }
  
  public BigDecimal getAccessTime() {
    return accessTime;
  }
  
  public void setAccessTime(BigDecimal accessTime) {
    this.accessTime = accessTime;
  }
  
  public Integer getHdfsUser() {
    return hdfsUser;
  }
  
  public void setHdfsUser(Integer hdfsUser) {
    this.hdfsUser = hdfsUser;
  }
  
  public Integer getHdfsGroup() {
    return hdfsGroup;
  }
  
  public void setHdfsGroup(Integer hdfsGroup) {
    this.hdfsGroup = hdfsGroup;
  }
  
  public short getPermission() {
    return permission;
  }
  
  public void setPermission(short permission) {
    this.permission = permission;
  }
  
  public String getSymlink() {
    return symlink;
  }
  
  public void setSymlink(String symlink) {
    this.symlink = symlink;
  }
  
  public boolean isQuotaEnabled() {
    return quotaEnabled;
  }
  
  public void setQuotaEnabled(boolean quotaEnabled) {
    this.quotaEnabled = quotaEnabled;
  }
  
  public boolean isUnderConstruction() {
    return underConstruction;
  }
  
  public void setUnderConstruction(boolean underConstruction) {
    this.underConstruction = underConstruction;
  }
  
  public String getMetaStatus() {
    return metaStatus;
  }
  
  public void setMetaStatus(String metaStatus) {
    this.metaStatus = metaStatus;
  }
  
  public boolean isDir() {
    return dir;
  }
  
  public void setDir(boolean dir) {
    this.dir = dir;
  }
  
  public int getChildrenNum() {
    return childrenNum;
  }
  
  public void setChildrenNum(int childrenNum) {
    this.childrenNum = childrenNum;
  }
  
  public long getSize() {
    return size;
  }
  
  public void setSize(long size) {
    this.size = size;
  }
  
  public long getPartitionId() {
    return partitionId;
  }
  
  public void setPartitionId(long partitionId) {
    this.partitionId = partitionId;
  }
  
  @Override
  public ExpatHdfsInode getEntity(ResultSet resultSet) throws SQLException {
    this.id = resultSet.getLong("id");
    this.parentId = resultSet.getLong("parent_id");
    this.name = resultSet.getString("name");
    this.modificationTime = resultSet.getBigDecimal("modification_time");
    this.accessTime = resultSet.getBigDecimal("access_time");
    this.hdfsUser = resultSet.getInt("user_id");
    this.hdfsGroup = resultSet.getInt("group_id");
    this.permission = resultSet.getShort("permission");
    this.symlink = resultSet.getString("symlink");
    this.quotaEnabled = resultSet.getBoolean("quota_enabled");
    this.underConstruction = resultSet.getBoolean("under_construction");
    this.metaStatus = resultSet.getString("meta_enabled");
    this.dir = resultSet.getBoolean("is_dir");
    this.childrenNum = resultSet.getInt("children_num");
    this.size = resultSet.getLong("size");
    this.partitionId = resultSet.getLong("partition_id");
    return this;
  }
}
