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
package io.hops.hopsworks.expat.db.dao;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public abstract class ExpatAbstractFacade<E extends ExpatAbstractEntity> {
  private final Class<E> entityClass;
  
  protected ExpatAbstractFacade(Class<E> entityClass) {
    this.entityClass = entityClass;
  }
  
  public abstract Connection getConnection();
  
  public List<E> findAll() throws SQLException, IllegalAccessException, InstantiationException {
    Statement statement = getConnection().createStatement();
    ResultSet resultSet = null;
    List<E> resultList = new ArrayList<>();
    try {
      resultSet = statement.executeQuery(this.findAllQuery());
      while (resultSet.next()) {
        E entity = this.entityClass.newInstance();
        resultList.add((E) entity.getEntity(resultSet));
      }
    } finally {
      if (resultSet != null) {
        resultSet.close();
      }
      if (statement != null) {
        statement.close();
      }
    }
    return resultList;
  }
  
  public E findById(Object id, JDBCType sqlType) throws SQLException, IllegalAccessException, InstantiationException {
    return findByCompositeId(new Object[]{id}, new JDBCType[]{sqlType});
  }
  
  public E findByCompositeId(Object[] ids, JDBCType[] sqlType) throws SQLException, IllegalAccessException,
    InstantiationException {
    List<E> resultList = findByQuery(this.findByIdQuery(), ids, sqlType);
    if (resultList.isEmpty()) {
      return null;
    }
    if (resultList.size() > 1) {
      throw new IllegalStateException("More than one results found");
    }
    return resultList.get(0);
  }
  
  public List<E> findByQuery(String query, Object param, JDBCType sqlType) throws SQLException, IllegalAccessException,
    InstantiationException {
    return findByQuery(query, new Object[]{param}, new JDBCType[]{sqlType});
  }
  
  public List<E> findByQuery(String query, Object[] params, JDBCType[] sqlType)
    throws SQLException, IllegalAccessException, InstantiationException {
    ResultSet resultSet = null;
    List<E> resultList = new ArrayList<>();
    PreparedStatement preparedStatement = null;
    try {
      preparedStatement = getConnection().prepareStatement(query);
      for (int i = 0; i < params.length; i++) {
        setObject(preparedStatement, i + 1, params[i], sqlType[i]);
      }
      resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        E entity = this.entityClass.newInstance();
        resultList.add((E) entity.getEntity(resultSet));
      }
    } finally {
      if (resultSet != null) {
        resultSet.close();
      }
      if (preparedStatement != null) {
        preparedStatement.close();
      }
    }
    return resultList;
  }
  
  public void update(String query, Object param, JDBCType sqlType) throws SQLException {
    update(query, new Object[]{param}, new JDBCType[]{sqlType});
  }
  
  public void update(String query, Object[] params, JDBCType[] sqlType) throws SQLException {
    PreparedStatement preparedStatement = null;
    try {
      preparedStatement = getConnection().prepareStatement(query);
      for (int i = 0; i < params.length; i++) {
        setObject(preparedStatement, i + 1, params[i], sqlType[i]);
      }
      preparedStatement.execute();
    } finally {
      if (preparedStatement != null) {
        preparedStatement.close();
      }
    }
  }
  
  public void updateBatch(String[] statements) throws SQLException {
    Statement statement = getConnection().createStatement();
    try {
      for (String s : statements) {
        statement.addBatch(s);
      }
      statement.executeBatch();
    } finally {
      if (statement != null) {
        statement.close();
      }
    }
  }
  
  public void closeConnection() throws SQLException {
    if (this.getConnection() != null) {
      this.getConnection().close();
    }
  }
  
  public abstract String findAllQuery();
  
  public abstract String findByIdQuery();
  
  private void setObject(PreparedStatement preparedStatement, int i, Object parameterObj, JDBCType sqlType)
    throws SQLException {
    try {
      preparedStatement.setObject(i, parameterObj, sqlType);
      return;
    } catch (SQLFeatureNotSupportedException e) {
      //setObject not implemented
    }
    if (parameterObj == null) {
      preparedStatement.setNull(i, 1111);
    } else if (parameterObj instanceof Byte) {
      preparedStatement.setInt(i, ((Byte) parameterObj).intValue());
    } else if (parameterObj instanceof String) {
      preparedStatement.setString(i, (String) parameterObj);
    } else if (parameterObj instanceof BigDecimal) {
      preparedStatement.setBigDecimal(i, (BigDecimal) parameterObj);
    } else if (parameterObj instanceof Short) {
      preparedStatement.setShort(i, (Short) parameterObj);
    } else if (parameterObj instanceof Integer) {
      preparedStatement.setInt(i, (Integer) parameterObj);
    } else if (parameterObj instanceof Long) {
      preparedStatement.setLong(i, (Long) parameterObj);
    } else if (parameterObj instanceof Float) {
      preparedStatement.setFloat(i, (Float) parameterObj);
    } else if (parameterObj instanceof Double) {
      preparedStatement.setDouble(i, (Double) parameterObj);
    } else if (parameterObj instanceof byte[]) {
      preparedStatement.setBytes(i, (byte[]) parameterObj);
    } else if (parameterObj instanceof Date) {
      preparedStatement.setDate(i, (Date) parameterObj);
    } else if (parameterObj instanceof Time) {
      preparedStatement.setTime(i, (Time) parameterObj);
    } else if (parameterObj instanceof Timestamp) {
      preparedStatement.setTimestamp(i, (Timestamp) parameterObj);
    } else if (parameterObj instanceof Boolean) {
      preparedStatement.setBoolean(i, (Boolean) parameterObj);
    } else if (parameterObj instanceof InputStream) {
      preparedStatement.setBinaryStream(i, (InputStream) parameterObj, -1);
    } else if (parameterObj instanceof Blob) {
      preparedStatement.setBlob(i, (Blob) parameterObj);
    } else if (parameterObj instanceof Clob) {
      preparedStatement.setClob(i, (Clob) parameterObj);
    } else if (parameterObj instanceof java.util.Date) {
      preparedStatement.setTimestamp(i, new Timestamp(((java.util.Date) parameterObj).getTime()));
    } else if (parameterObj instanceof BigInteger) {
      preparedStatement.setString(i, parameterObj.toString());
    } else {
      throw new SQLException("Not implemented");
    }
  }
}
