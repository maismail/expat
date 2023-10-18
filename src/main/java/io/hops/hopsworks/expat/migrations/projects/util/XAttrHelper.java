/**
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
package io.hops.hopsworks.expat.migrations.projects.util;

import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;

public class XAttrHelper {
  private final static String XATTR_PROV_NAMESPACE = "provenance";
  
  public static void insertXAttr(DistributedFileSystemOps udfso, String path, String namespace, String name,
                                    byte[] value) throws XAttrException {
    if (name == null || name.isEmpty()) {
      throw new XAttrException("missing xattr name");
    }
    addXAttrInt(udfso, path, namespace, name, value);
  }
    
  public static boolean upsertProvXAttr(DistributedFileSystemOps udfso, String path, String name, byte[] value)
    throws XAttrException {
    if (name == null || name.isEmpty()) {
      throw new XAttrException("missing xattr name");
    }
    boolean hasPrevious = (getXAttr(udfso, path, XATTR_PROV_NAMESPACE, name) != null);
    addXAttrInt(udfso, path, XATTR_PROV_NAMESPACE, name, value);
    return hasPrevious;
  }
  
  private static void addXAttrInt(DistributedFileSystemOps udfso, String path, String namespace, String name,
    byte[] value) throws XAttrException {
    try {
      udfso.setXAttr(new Path(path), getXAttrName(namespace, name), value);
    } catch(RemoteException e) {
      if(e.getClassName().equals("org.apache.hadoop.HadoopIllegalArgumentException")
        && e.getMessage().startsWith("The XAttr value is too big.")) {
        throw new XAttrException("metadata max size exceeded", e);
      } else if(e.getClassName().equals("java.io.IOException")
        && e.getMessage().startsWith("Cannot add additional XAttr to inode, would exceed limit")) {
        throw new XAttrException("metadata max size exceeded", e);
      } else {
        throw new XAttrException("metadata error", e);
      }
    } catch (IOException e) {
      throw new XAttrException("metadata error", e);
    }
  }
  
  public static byte[] getXAttr(DistributedFileSystemOps udfso, String path, String namespace, String name)
    throws XAttrException {
    try {
      return udfso.getXAttr(new Path(path), getXAttrName(namespace, name));
    } catch (RemoteException e) {
      if(e.getClassName().equals("io.hops.exception.StorageException")
        && e.getMessage().startsWith("com.mysql.clusterj.ClusterJUserException: Data length")) {
        throw new XAttrException("xattr max size exceeded", e);
      }
      if(e.getClassName().equals("java.io.IOException")
        && e.getMessage().startsWith("At least one of the attributes provided was not found.")) {
        return null;
      }
      if(e.getClassName().equals("java.io.FileNotFoundException")){
        return null;
      }
      throw new XAttrException("metadata error", e);
    } catch (java.io.FileNotFoundException e){
      return null;
    } catch (IOException e) {
      throw new XAttrException("metadata error", e);
    }
  }
  
  private static String getXAttrName(String namespace, String name) {
    return namespace + "." + name;
  }
  
  public static void deleteXAttr(DistributedFileSystemOps udfso, String path, String namespace, String name)
    throws XAttrException {
    try {
      udfso.removeXAttr(new Path(path), getXAttrName(namespace, name));
    } catch (IOException e) {
      throw new XAttrException("metadata error", e);
    }
  }
}
