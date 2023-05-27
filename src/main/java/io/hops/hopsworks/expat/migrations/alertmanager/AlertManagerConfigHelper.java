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
package io.hops.hopsworks.expat.migrations.alertmanager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Strings;
import com.logicalclocks.servicediscoverclient.exceptions.ServiceDiscoveryException;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.alert.ExpatFeatureGroupAlert;
import io.hops.hopsworks.expat.db.dao.alert.ExpatFeatureGroupAlertFacade;
import io.hops.hopsworks.expat.db.dao.alert.ExpatJobAlert;
import io.hops.hopsworks.expat.db.dao.alert.ExpatJobAlertFacade;
import io.hops.hopsworks.expat.db.dao.alert.ExpatProjectAlert;
import io.hops.hopsworks.expat.db.dao.alert.ExpatProjectAlertFacade;
import io.hops.hopsworks.expat.db.dao.alert.ExpatReceiver;
import io.hops.hopsworks.expat.db.dao.alert.ExpatReceiverFacade;
import io.hops.hopsworks.expat.migrations.alertmanager.dto.AlertManagerConfig;
import io.hops.hopsworks.expat.migrations.alertmanager.dto.Receiver;
import io.hops.hopsworks.persistence.entity.alertmanager.AlertType;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.List;
import java.util.Optional;

public class AlertManagerConfigHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlertManagerConfigHelper.class);

  public static final String ALERT_MANAGER_DIR = "/srv/hops/alertmanager/alertmanager";
  public static final String ALERT_MANAGER_CONFIG_FILE = "alertmanager.yml";
  public static final String ALERT_MANAGER_CONFIG = ALERT_MANAGER_DIR + "/" + ALERT_MANAGER_CONFIG_FILE;
  
  private Connection connection;
  private ExpatReceiverFacade receiverFacade;
  private ExpatProjectAlertFacade projectAlertFacade;
  private ExpatJobAlertFacade jobAlertFacade;
  private ExpatFeatureGroupAlertFacade featureGroupAlert;
  private boolean dryrun;
  
  public AlertManagerConfigHelper() {
  }
  
  public void setup() throws ConfigurationException, SQLException {
    Configuration conf = ConfigurationBuilder.getConfiguration();
    this.connection = DbConnectionFactory.getConnection();
    this.receiverFacade = new ExpatReceiverFacade(ExpatReceiver.class, this.connection);
    this.projectAlertFacade = new ExpatProjectAlertFacade(ExpatProjectAlert.class, this.connection);
    this.jobAlertFacade = new ExpatJobAlertFacade(ExpatJobAlert.class, this.connection);
    this.featureGroupAlert = new ExpatFeatureGroupAlertFacade(ExpatFeatureGroupAlert.class, this.connection);
    this.dryrun = conf.getBoolean(ExpatConf.DRY_RUN);
  }
  
  public AlertManagerConfig read() throws IOException {
    File configFile = new File(ALERT_MANAGER_CONFIG);
    ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
    return objectMapper.readValue(configFile, AlertManagerConfig.class);
  }
  
  public void migrate() throws IOException, SQLException, IllegalAccessException, InstantiationException {
    AlertManagerConfig alertManagerConfig = read();
    if (alertManagerConfig != null && alertManagerConfig.getReceivers() != null &&
        !alertManagerConfig.getReceivers().isEmpty()) {
      for (Receiver receiver : alertManagerConfig.getReceivers()) {
        save(receiver);
      }
    }
    fixColumnNotNUll(alertManagerConfig);
  }
  
  private ExpatReceiver getGlobalReceiverOrDefault(AlertType alertType, ExpatReceiver defaultReceiver)
      throws SQLException, IllegalAccessException, InstantiationException, IOException {
    if (dryrun) {
      AlertManagerConfig alertManagerConfig = read();
      Receiver receiver = new Receiver(AlertType.DEFAULT.getReceiverName());
      if (alertType.isGlobal() && alertManagerConfig.getReceivers() != null) {
        Optional<Receiver> optionalReceiver =
            alertManagerConfig.getReceivers().stream().filter(r -> r.getName().equals(alertType.getReceiverName()))
                .findFirst();
        if (optionalReceiver.isPresent()) {
          receiver = optionalReceiver.get();
        }
      }
      return new ExpatReceiver(receiver.getName());
    }
    if (alertType.isGlobal()) {
      ExpatReceiver receiver = this.receiverFacade.findByName(alertType.getReceiverName());
      if (receiver != null) {
        return receiver;
      }
    }
    return defaultReceiver;
  }
  
  private void fixProjectAlerts(ExpatReceiver defaultReceiver)
      throws SQLException, IllegalAccessException, InstantiationException, IOException {
    List<ExpatProjectAlert> projectAlerts = this.projectAlertFacade.findAll();
    for (ExpatProjectAlert projectAlert : projectAlerts) {
      ExpatReceiver receiver = getGlobalReceiverOrDefault(projectAlert.getAlertType(), defaultReceiver);
      if (projectAlert.getReceiver() == null) {
        if (dryrun) {
          LOGGER
              .info("Set Project alert table receiver id={}, type={}, receiverName={}.", projectAlert.getId(),
                  projectAlert.getAlertType(), receiver.getName());
        } else {
          LOGGER
              .info("Set Project alert table receiver id={}, type={}, receiverName={}.", projectAlert.getId(),
                  projectAlert.getAlertType(), receiver.getName());
          this.projectAlertFacade.update(projectAlert.getId(), receiver.getId());
        }
      }
    }
    if (dryrun) {
      LOGGER.info("Set Project alert table receiver column to not nullable.");
    } else {
      LOGGER.info("Set Project alert table receiver column to not nullable.");
      this.projectAlertFacade.updateColumnNotNull();
    }
  }
  
  private void fixJobAlerts(ExpatReceiver defaultReceiver)
      throws SQLException, IllegalAccessException, InstantiationException, IOException {
    List<ExpatJobAlert> jobAlerts = this.jobAlertFacade.findAll();
    for (ExpatJobAlert jobAlert : jobAlerts) {
      ExpatReceiver receiver = getGlobalReceiverOrDefault(jobAlert.getAlertType(), defaultReceiver);
      if (jobAlert.getReceiver() == null) {
        if (dryrun) {
          LOGGER.info("Set Job alert table receiver id={}, type={}, receiverName={}.", jobAlert.getId(),
              jobAlert.getAlertType(), receiver.getName());
        } else {
          LOGGER.info("Set Job alert table receiver id={}, type={}, receiverName={}.", jobAlert.getId(),
              jobAlert.getAlertType(), receiver.getName());
          this.jobAlertFacade.update(jobAlert.getId(), receiver.getId());
        }
      }
    }
    if (dryrun) {
      LOGGER.info("Set Job alert table receiver column to not nullable.");
    } else {
      LOGGER.info("Set Job alert table receiver column to not nullable.");
      this.jobAlertFacade.updateColumnNotNull();
    }
  }
  
  private void fixFeatureGroupAlerts(ExpatReceiver defaultReceiver)
      throws SQLException, IllegalAccessException, InstantiationException, IOException {
    List<ExpatFeatureGroupAlert> featureGroupAlerts = this.featureGroupAlert.findAll();
    for (ExpatFeatureGroupAlert featureGroupAlert : featureGroupAlerts) {
      ExpatReceiver receiver = getGlobalReceiverOrDefault(featureGroupAlert.getAlertType(), defaultReceiver);
      if (featureGroupAlert.getReceiver() == null) {
        if (dryrun) {
          LOGGER.info("Set FeatureGroup alert table receiver id={}, type={}, receiverName={}.",
              featureGroupAlert.getId(),
              featureGroupAlert.getAlertType(), receiver.getName());
        } else {
          LOGGER.info("Set FeatureGroup alert table receiver id={}, type={}, receiverName={}.",
              featureGroupAlert.getId(),
              featureGroupAlert.getAlertType(), receiver.getName());
          this.featureGroupAlert.update(featureGroupAlert.getId(), receiver.getId());
        }
      }
    }
    if (dryrun) {
      LOGGER.info("Set FeatureGroup alert table receiver column to not nullable.");
    } else {
      LOGGER.info("Set FeatureGroup alert table receiver column to not nullable.");
      this.featureGroupAlert.updateColumnNotNull();
    }
  }
  
  private void rollbackProjectAlerts()
      throws SQLException, IllegalAccessException, InstantiationException {
    List<ExpatProjectAlert> projectAlerts = this.projectAlertFacade.findAll();
    if (dryrun) {
      LOGGER.info("Set Project alert table receiver column to nullable.");
    } else {
      LOGGER.info("Set Project alert table receiver column to nullable.");
      this.projectAlertFacade.updateColumnNullable();
    }
    for (ExpatProjectAlert projectAlert : projectAlerts) {
      if (dryrun) {
        LOGGER.info("Reset Project alert table receiver id={}.", projectAlert.getId());
      } else {
        LOGGER.info("Reset Project alert table receiver id={}.", projectAlert.getId());
        this.projectAlertFacade.update(projectAlert.getId(), null);
      }
    }
  }
  
  private void rollbackJobAlerts()
      throws SQLException, IllegalAccessException, InstantiationException {
    List<ExpatJobAlert> jobAlerts = this.jobAlertFacade.findAll();
    if (dryrun) {
      LOGGER.info("Set Job alert table receiver column to nullable.");
    } else {
      LOGGER.info("Set Job alert table receiver column to nullable.");
      this.jobAlertFacade.updateColumnNullable();
    }
    for (ExpatJobAlert jobAlert : jobAlerts) {
      if (dryrun) {
        LOGGER.info("Reset Job alert table receiver id={}.", jobAlert.getId());
      } else {
        LOGGER.info("Reset Job alert table receiver id={}.", jobAlert.getId());
        this.jobAlertFacade.update(jobAlert.getId(), null);
      }
    }
  }
  
  private void rollbackFeatureGroupAlerts()
      throws SQLException, IllegalAccessException, InstantiationException {
    List<ExpatFeatureGroupAlert> featureGroupAlerts = this.featureGroupAlert.findAll();
    if (dryrun) {
      LOGGER.info("Set FeatureGroup alert table receiver column to nullable.");
    } else {
      LOGGER.info("Set FeatureGroup alert table receiver column to nullable.");
      this.featureGroupAlert.updateColumnNullable();
    }
    for (ExpatFeatureGroupAlert featureGroupAlert : featureGroupAlerts) {
      if (dryrun) {
        LOGGER.info("Reset FeatureGroup alert table receiver id={}.", featureGroupAlert.getId());
      } else {
        LOGGER.info("Reset FeatureGroup alert table receiver id={}.", featureGroupAlert.getId());
        this.featureGroupAlert.update(featureGroupAlert.getId(), null);
      }
    }
  }
  
  private void fixColumnNotNUll(AlertManagerConfig alertManagerConfig)
      throws SQLException, IOException, IllegalAccessException, InstantiationException {
    String receiverName = alertManagerConfig != null &&
        alertManagerConfig.getRoute() != null && !Strings.isNullOrEmpty(alertManagerConfig.getRoute().getReceiver()) ?
        alertManagerConfig.getRoute().getReceiver() : AlertType.DEFAULT.getReceiverName();
    ExpatReceiver receiver = this.receiverFacade.findByName(receiverName);
    if (!dryrun && receiver == null) {
      save(new Receiver(receiverName));
      receiver = this.receiverFacade.findByName(receiverName);
    }
    fixProjectAlerts(receiver);
    fixJobAlerts(receiver);
    fixFeatureGroupAlerts(receiver);
  }
  
  private JSONObject toJson(Receiver receiver) throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    return new JSONObject(objectMapper.writeValueAsString(receiver));
  }
  
  private void save(Receiver receiver) throws SQLException, JsonProcessingException {
    try {
      JSONObject jsonObject = toJson(receiver);
      if (dryrun) {
        LOGGER.info("Add receiver: {}.", receiver.getName());
      } else {
        LOGGER.info("Add receiver: {}.", receiver.getName());
        this.receiverFacade.addReceiver(receiver.getName(), jsonObject.toString().getBytes(StandardCharsets.UTF_8));
      }
    } catch (SQLIntegrityConstraintViolationException e) {
      LOGGER.info("Integrity Constraint Violation: receiver name={}. {}", receiver.getName(), e.getMessage());
    }
  }
  
  public void rollback() throws IOException, ServiceDiscoveryException, SQLException, InstantiationException,
      ConfigurationException, IllegalAccessException {
    rollbackProjectAlerts();
    rollbackJobAlerts();
    rollbackFeatureGroupAlerts();
  }
  
  public void close() {
    if (this.connection != null) {
      try {
        this.connection.close();
      } catch (SQLException e) {
        LOGGER.info("Failed to close db connection {}", e.getMessage());
      }
    }
  }
}
