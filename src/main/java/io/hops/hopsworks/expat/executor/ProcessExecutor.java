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

import io.hops.hopsworks.common.util.ProcessDescriptor;
import io.hops.hopsworks.common.util.ProcessResult;
import io.hops.hopsworks.common.util.StreamGobbler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class ProcessExecutor {
  private final static Logger LOG = LogManager.getLogger(ProcessExecutor.class);
  private final static int EXECUTOR_THREADS = 20;
  
  private final ExecutorService executorService;
  
  private static ProcessExecutor instance;
  
  private ProcessExecutor() {
    executorService = Executors.newFixedThreadPool(EXECUTOR_THREADS);
  }
  
  public static ProcessExecutor getExecutor() {
    if (instance == null) {
      instance = new ProcessExecutor();
    }
    
    return instance;
  }
  
  public void stop() {
    if (executorService != null) {
      // Messages here may not be printed.
      // It might happen log4j shutdown hooks are called before
      // Expat shutdown hooks
      LOG.debug("Shutting down ProcessExecutor executor service");
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException ex) {
        LOG.warn("Waited enough to gracefully shutdown. Bye...");
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }
  
  public ProcessResult execute(ProcessDescriptor processDescriptor) throws IOException {
    try {
      return runProcess(processDescriptor);
    } catch (InterruptedException | ExecutionException | TimeoutException ex) {
      throw new IOException(ex);
    }
  }
  
  public Future<ProcessResult> submit(ProcessDescriptor processDescriptor) throws IOException {
    ExecutorWorker worker = new ExecutorWorker(processDescriptor);
    return executorService.submit(worker);
  }
  
  private ProcessResult runProcess(ProcessDescriptor processDescriptor) throws IOException, InterruptedException,
      ExecutionException, TimeoutException {
    ProcessBuilder processBuilder = new ProcessBuilder(processDescriptor.getSubcommands());
    processBuilder.directory(processDescriptor.getCwd());
    Map<String, String> env = processBuilder.environment();
    for (Map.Entry<String, String> entry : processDescriptor.getEnvironmentVariables().entrySet()) {
      env.put(entry.getKey(), entry.getValue());
    }
    processBuilder.redirectErrorStream(processDescriptor.redirectErrorStream());
  
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    ByteArrayOutputStream errStream = new ByteArrayOutputStream();
    boolean ignoreStreams = processDescriptor.ignoreOutErrStreams();

    StreamGobbler stderrGobbler;
    Future stderrGobblerFuture = null;

    try {
      Process process = processBuilder.start();

      if (!processDescriptor.redirectErrorStream()) {
        stderrGobbler = new StreamGobbler(process.getErrorStream(), errStream, ignoreStreams);
        stderrGobblerFuture = executorService.submit(stderrGobbler);
      }

      StreamGobbler stdoutGobbler = new StreamGobbler(process.getInputStream(), outStream, ignoreStreams);
      Future stdoutGobblerFuture = executorService.submit(stdoutGobbler);

      boolean exited = process.waitFor(processDescriptor.getWaitTimeout(), processDescriptor.getTimeoutUnit());

      if (exited) {
        waitForGobbler(stdoutGobblerFuture);
        if (stderrGobblerFuture != null) {
          waitForGobbler(stderrGobblerFuture);
        }
        return new ProcessResult(process.exitValue(), true, stringifyStream(outStream, ignoreStreams),
            stringifyStream(errStream, ignoreStreams));
      } else {
        process.destroyForcibly();
        stdoutGobblerFuture.cancel(true);
        if (stderrGobblerFuture != null) {
          stderrGobblerFuture.cancel(true);
        }
        return new ProcessResult(process.exitValue(), false, stringifyStream(outStream, ignoreStreams),
            "Process timed-out");
      }
    } catch (IOException e) {
      throw new IOException(stringifyStream(outStream, ignoreStreams));
    }
  }
  
  private void waitForGobbler(Future gobbler) throws InterruptedException, ExecutionException {
    try {
      gobbler.get(500L, TimeUnit.MILLISECONDS);
    } catch (TimeoutException ex) {
      LOG.warn("Waited enough for StreamGobbler to finish, killing it...");
      gobbler.cancel(true);
    }
  }
  
  private String stringifyStream(OutputStream outputStream, boolean ignoreStream) {
    if (ignoreStream) {
      return "";
    }
    return outputStream.toString();
  }
  
  public static class ShutdownHook implements Runnable {
  
    @Override
    public void run() {
      ProcessExecutor.getExecutor().stop();
    }
  }
  
  private class ExecutorWorker implements Callable<ProcessResult> {
  
    private final ProcessDescriptor processDescriptor;
    
    private ExecutorWorker(ProcessDescriptor processDescriptor) {
      this.processDescriptor = processDescriptor;
    }
    
    @Override
    public ProcessResult call() throws IOException {
      try {
        return runProcess(processDescriptor);
      } catch (InterruptedException | ExecutionException | TimeoutException ex) {
        throw new IOException(ex);
      }
    }
  }
}
