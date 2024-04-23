/*
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

package io.hops.hopsworks.expat.kubernetes;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;

public class KubernetesClientFactory {

  private static Config kubeConfig = null;

  public static KubernetesClient getClient() throws ConfigurationException {
    Configuration config = ConfigurationBuilder.getConfiguration();

    if (kubeConfig == null) {
      kubeConfig = new ConfigBuilder()
          .withUsername(config.getString(ExpatConf.KUBE_USER_KEY))
          .withMasterUrl(config.getString(ExpatConf.KUBE_MASTER_URL_KEY))
          .withCaCertFile(config.getString(ExpatConf.KUBE_CA_CERTFILE_KEY))
          .withTrustStoreFile(config.getString(ExpatConf.KUBE_TSTORE_PATH_KEY))
          .withTrustStorePassphrase(config.getString(ExpatConf.KUBE_TSTORE_PWD_KEY))
          .withKeyStoreFile(config.getString(ExpatConf.KUBE_KSTORE_PATH_KEY))
          .withKeyStorePassphrase(config.getString(ExpatConf.KUBE_KSTORE_PWD_KEY))
          .withClientCertFile(config.getString(ExpatConf.KUBE_CERTFILE_KEY))
          .withClientKeyFile(config.getString(ExpatConf.KUBE_KEYFILE_KEY))
          .withClientKeyPassphrase(config.getString(ExpatConf.KUBE_KEYPWD_KEY))
          .withHttp2Disable(true)
          .build();
    }

    return new DefaultKubernetesClient();
  }
}
