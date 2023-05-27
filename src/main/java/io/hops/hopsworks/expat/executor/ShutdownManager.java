/**
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
package io.hops.hopsworks.expat.executor;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ShutdownManager {
  private static final Logger LOG = LoggerFactory.getLogger(ShutdownManager.class);
  private static volatile ShutdownManager instance;
  private final SortedSet<ShutdownHook> hooks;
  private final AtomicBoolean shuttingDown;
  private final ExecutorService exec;
  
  private ShutdownManager() {
    shuttingDown = new AtomicBoolean(false);
    exec = Executors.newSingleThreadExecutor();
    hooks = Collections.synchronizedSortedSet(new TreeSet<>(new Comparator<ShutdownHook>() {
      @Override
      public int compare(ShutdownHook t0, ShutdownHook t1) {
        return t1.priority - t0.priority;
      }
    }));
    registerMasterShutdownHook();
  }
  
  public static ShutdownManager getManager() {
    if (instance == null) {
      synchronized (ShutdownManager.class) {
        if (instance == null) {
          instance = new ShutdownManager();
        }
      }
    }
    return instance;
  }
  
  public void addShutdownHook(Runnable hook, int priority) {
    if (!shuttingDown.get()) {
      hooks.add(new ShutdownHook(hook, priority));
    }
  }
  
  private void registerMasterShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new MasterHook());
  }
  
  private final class MasterHook extends Thread {
    @Override
    public void run() {
      shuttingDown.set(true);
      for (ShutdownHook hook : hooks) {
        LOG.info("Calling shutdown hook " + hook.hook.getClass().getCanonicalName());
        Future future = exec.submit(hook.hook);
        try {
          future.get(30L, TimeUnit.SECONDS);
        } catch (TimeoutException ex) {
          future.cancel(true);
          LOG.warn("Shutdown hook timeout, cancelling " + hook.hook.getClass().getCanonicalName());
        } catch (InterruptedException | ExecutionException ex) {
          LOG.warn("Exception while executing shutdown hook " + hook.hook.getClass().getCanonicalName());
        }
      }
      
      try {
        exec.shutdown();
        if (!exec.awaitTermination(3L, TimeUnit.MINUTES)) {
          exec.shutdownNow();
        }
      } catch (InterruptedException ex) {
        exec.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }
  
  private final class ShutdownHook {
    private final Runnable hook;
    private final int priority;
    
    private ShutdownHook(Runnable hook, int priority) {
      this.hook = hook;
      this.priority = priority;
    }
  
    @Override
    public int hashCode() {
      return hook.hashCode();
    }
    
    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      
      if (other instanceof ShutdownHook) {
        return this.hook.equals(((ShutdownHook) other).hook);
      }
      
      return false;
    }
  }
}
