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
package io.hops.hopsworks.expat.db.dao.project;

import io.hops.hopsworks.expat.db.dao.ExpatAbstractEntity;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public class ExpatProject extends ExpatAbstractEntity<ExpatProject> {
  private Integer id;
  private String name;
  private String owner;
  private Date created;
  private String paymentType;
  private Integer pythonEnvId;
  private String description;
  private Integer kafkaMaxNumTopics;
  private Date lastQuotaUpdate;
  private String dockerImage;
  
  public ExpatProject() {
  }
  
  public Integer getId() {
    return id;
  }
  
  public void setId(Integer id) {
    this.id = id;
  }
  
  public String getName() {
    return name;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public String getOwner() {
    return owner;
  }
  
  public void setOwner(String owner) {
    this.owner = owner;
  }
  
  public Date getCreated() {
    return created;
  }
  
  public void setCreated(Date created) {
    this.created = created;
  }
  
  public String getPaymentType() {
    return paymentType;
  }
  
  public void setPaymentType(String paymentType) {
    this.paymentType = paymentType;
  }
  
  public Integer getPythonEnvId() {
    return pythonEnvId;
  }
  
  public void setPythonEnvId(Integer pythonEnvId) {
    this.pythonEnvId = pythonEnvId;
  }
  
  public String getDescription() {
    return description;
  }
  
  public void setDescription(String description) {
    this.description = description;
  }
  
  public Integer getKafkaMaxNumTopics() {
    return kafkaMaxNumTopics;
  }
  
  public void setKafkaMaxNumTopics(Integer kafkaMaxNumTopics) {
    this.kafkaMaxNumTopics = kafkaMaxNumTopics;
  }
  
  public Date getLastQuotaUpdate() {
    return lastQuotaUpdate;
  }
  
  public void setLastQuotaUpdate(Date lastQuotaUpdate) {
    this.lastQuotaUpdate = lastQuotaUpdate;
  }
  
  public String getDockerImage() {
    return dockerImage;
  }
  
  public void setDockerImage(String dockerImage) {
    this.dockerImage = dockerImage;
  }
  
  @Override
  public ExpatProject getEntity(ResultSet resultSet) throws SQLException {
    this.id = resultSet.getInt("id");
    this.name = resultSet.getString("projectname");
    this.owner = resultSet.getString("username");
    this.created = resultSet.getDate("created");
    this.paymentType = resultSet.getString("payment_type");
    this.pythonEnvId = resultSet.getInt("python_env_id");
    this.description = resultSet.getString("description");
    this.kafkaMaxNumTopics = resultSet.getInt("kafka_max_num_topics");
    this.lastQuotaUpdate = resultSet.getDate("last_quota_update");
    this.dockerImage = resultSet.getString("docker_image");
    return this;
  }
  
  @Override
  public String toString() {
    return "ExpatProject{" +
      "id=" + id +
      ", name='" + name + '\'' +
      ", owner='" + owner + '\'' +
      ", created=" + created +
      ", paymentType='" + paymentType + '\'' +
      ", pythonEnvId='" + pythonEnvId + '\'' +
      ", description='" + description + '\'' +
      ", kafkaMaxNumTopics=" + kafkaMaxNumTopics +
      ", lastQuotaUpdate=" + lastQuotaUpdate +
      ", dockerImage='" + dockerImage + '\'' +
      '}';
  }
}
