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
package io.hops.hopsworks.expat.db.dao.alert;

import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.ExpatAbstractFacade;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;

public class ExpatFeatureGroupAlertFacade extends ExpatAbstractFacade<ExpatFeatureGroupAlert> {
  private static final String GET_ALL_ALERTS = "SELECT * FROM feature_group_alert";
  private final static String UPDATE_ALERTS = "UPDATE feature_group_alert SET receiver = ? WHERE id = ?";
  private Connection connection;
  
  protected ExpatFeatureGroupAlertFacade(Class<ExpatFeatureGroupAlert> entityClass)
      throws ConfigurationException, SQLException {
    super(entityClass);
    this.connection = DbConnectionFactory.getConnection();
  }
  
  public ExpatFeatureGroupAlertFacade(Class<ExpatFeatureGroupAlert> entityClass, Connection connection) {
    super(entityClass);
    this.connection = connection;
  }
  
  @Override
  public Connection getConnection() {
    return this.connection;
  }
  
  @Override
  public String findAllQuery() {
    return GET_ALL_ALERTS;
  }
  
  @Override
  public String findByIdQuery() {
    return GET_ALL_ALERTS + " WHERE id = ?";
  }
  
  public ExpatFeatureGroupAlert find(Integer id) throws IllegalAccessException, SQLException, InstantiationException {
    return this.findById(id, JDBCType.INTEGER);
  }
  
  public void update(Integer id, Integer receiver) throws SQLException {
    this.update(UPDATE_ALERTS, new Object[]{receiver, id}, new JDBCType[]{JDBCType.INTEGER, JDBCType.INTEGER});
  }
  
  public void updateColumnNotNull() throws SQLException {
    this.updateBatch(new String[] {"ALTER TABLE feature_group_alert DROP FOREIGN KEY fk_feature_group_alert_1",
        "ALTER TABLE feature_group_alert CHANGE receiver receiver INT(11) NOT NULL",
        "ALTER TABLE feature_group_alert ADD CONSTRAINT fk_feature_group_alert_1 FOREIGN KEY (receiver) " +
            "REFERENCES alert_receiver (id) ON DELETE CASCADE ON UPDATE NO ACTION"});
  }
  
  public void updateColumnNullable() throws SQLException {
    this.updateBatch(new String[] {"ALTER TABLE feature_group_alert DROP FOREIGN KEY fk_feature_group_alert_1",
        "ALTER TABLE feature_group_alert CHANGE receiver receiver INT(11) DEFAULT NULL",
        "ALTER TABLE feature_group_alert ADD CONSTRAINT fk_feature_group_alert_1 FOREIGN KEY (receiver) " +
            "REFERENCES alert_receiver (id) ON DELETE CASCADE ON UPDATE NO ACTION"});
  }
}
