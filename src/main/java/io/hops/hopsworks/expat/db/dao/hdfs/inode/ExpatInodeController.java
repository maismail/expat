package io.hops.hopsworks.expat.db.dao.hdfs.inode;

import io.hops.hopsworks.common.util.HopsUtils;
import io.hops.hopsworks.expat.migrations.MigrationException;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

public class ExpatInodeController {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExpatInodeController.class);
  
  private ExpatHdfsInodeFacade inodeFacade;
  
  public ExpatInodeController(Connection connection) {
    this.inodeFacade = new ExpatHdfsInodeFacade(ExpatHdfsInode.class, connection);
  }
  
  /**
   * Get the Inode at the specified path.
   * <p/>
   * @param path
   * @return Null if path does not exist.
   */
  public ExpatHdfsInode getInodeAtPath(String path) throws MigrationException, SQLException {
    // LOGGER.info("getInodeAtPath: " + path);
    return getInode(path);
  }
  
  private ExpatHdfsInode getInode(String path) throws MigrationException, SQLException {
    // LOGGER.info("getInode: " + path);
    // Get the path components
    String[] p;
    if (path.charAt(0) == '/') {
      p = path.substring(1).split("/");
    } else if(path.startsWith("hopsfs")){
      //In case path looks like "hopsfs://namenode.service.consul:8020/apps/hive/warehouse/test_proj_fs.db/fg1_1"
      p = path.split("/");
      p = ArrayUtils.subarray(p,3,p.length);
    } else {
      p = path.split("/");
    }
    
    if (p.length < 1) {
      return null;
    }
    
    return getInode(inodeFacade.getRootNode(p[0]), 1, Arrays.copyOfRange(p, 1, p.length));
  }
  
  private ExpatHdfsInode getInode(ExpatHdfsInode inode, int depth, String[] p) throws MigrationException, SQLException {
    // LOGGER.info("getInode: " + inode + " depth: " + depth + " p: " + p);
    //Get the right root node
    ExpatHdfsInode curr = inode;
    if (curr == null) {
      return null;
    }
    //Move down the path
    for (int i = 0; i < p.length; i++) {
      long partitionId = HopsUtils.calculatePartitionId(curr.getId(), p[i], i + depth + 1);
      ExpatHdfsInode next = inodeFacade.findByInodePK(curr.getId(), p[i], partitionId);
      if (next == null) {
        return null;
      } else {
        curr = next;
      }
    }
    return curr;
  }
}
