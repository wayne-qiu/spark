package com.societegenerale.volcker.ulti;

import jakarta.annotation.PreDestroy;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

public abstract class SparkTemplate {

  private static final Logger logger = LoggerFactory.getLogger(SparkTemplate.class);
  
  private SparkSession sparkSession;
  protected SparkSession spark;
  
  @Value("${spark.tmp.dir:C:\\Users\\wei_q\\Projects\\spark-tmp}")
  private String sparkTempDir;
  
  @Value("${spark.app.name:Spring-Boot-Spark-Application}")
  private String appName;
  
  @Value("${spark.master:local[*]}")
  private String master;
  
  @Value("${spark.ui.enabled:false}")
  private boolean uiEnabled;
  
  @Value("${spark.ui.port:4040}")
  private String uiPort;

  public final void run(String... args) throws Exception {
    logger.info("Starting Spark Batch Job...");
    
    spark = getSession();
    try {
      job();
      logger.info("Spark Batch Job completed successfully");
    } catch (Exception e) {
      logger.error("Spark Batch Job failed", e);
      throw e;
    }
  }

  protected abstract void job() throws Exception;

  private SparkSession getSession() {
    if (sparkSession == null) {
      synchronized (this) {
        if (sparkSession == null) {
          logger.info("Initializing SparkSession...");
          sparkSession = createSession();
          logger.info("SparkSession initialized successfully");
          if (uiEnabled) {
            logger.info("Access SparkUI @ localhost:{}", uiPort);
          }
        }
      }
    }
    return sparkSession;
  }
  
  private SparkSession createSession() {
    String checkpointLocation = sparkTempDir + "\\spark-checkpoints";
    
    return SparkSession.builder()
        .appName(appName)
        .master(master)
        .config("spark.ui.enabled", String.valueOf(uiEnabled))
        .config("spark.ui.port", uiPort)
        .config("spark.shutdown.hook.enabled", "false")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.streaming.checkpointLocation", checkpointLocation)
        .config("spark.local.dir", sparkTempDir)
        .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=" + sparkTempDir)
        .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=" + sparkTempDir)
        // Windows-specific fixes - more aggressive
        .config("spark.cleaner.referenceTracking", "false")
        .config("spark.cleaner.periodicGC.interval", "0min")
        .config("spark.worker.cleanup.enabled", "false")
        .config("spark.storage.blockManagerSlaveTimeoutMs", "300000")
        .config("spark.storage.removeBlocks", "false")
        .config("spark.cleaner.delayedCleanupPeriod", "0min")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate();
  }

  @PreDestroy
  public void closeSession() {
    if (sparkSession != null) {
      logger.info("Closing SparkSession...");
      try {
        // Stop Spark context before closing session
        if (sparkSession.sparkContext() != null && !sparkSession.sparkContext().isStopped()) {
          sparkSession.sparkContext().stop();
        }
        sparkSession.close();
        logger.info("SparkSession closed successfully");
      } catch (Exception e) {
        logger.warn("Error during SparkSession closure (this is normal on Windows): {}", e.getMessage());
      } finally {
        sparkSession = null;
        spark = null;
        // Force garbage collection to help with file release
        System.gc();
        try {
          Thread.sleep(2000); // Give more time for cleanup
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
        // Manual cleanup of temp directories
        cleanupTempDirectories();
      }
    }
  }
  
  private void cleanupTempDirectories() {
    try {
      logger.info("Attempting manual cleanup of Spark temp directories...");
      java.io.File tempDir = new java.io.File(sparkTempDir);
      if (tempDir.exists()) {
        deleteDirectory(tempDir);
        logger.info("Manual cleanup completed");
      }
    } catch (Exception e) {
      logger.debug("Manual cleanup failed (this is expected on Windows): {}", e.getMessage());
    }
  }
  
  private void deleteDirectory(java.io.File directory) {
    if (directory == null || !directory.exists()) {
      return;
    }
    
    java.io.File[] files = directory.listFiles();
    if (files != null) {
      for (java.io.File file : files) {
        if (file.isDirectory()) {
          deleteDirectory(file);
        } else {
          // Try multiple times to delete the file
          for (int i = 0; i < 3; i++) {
            if (file.delete()) {
              break;
            }
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              break;
            }
          }
        }
      }
    }
    // Try to delete the directory itself
    for (int i = 0; i < 3; i++) {
      if (directory.delete()) {
        break;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }
  
  protected void setSparkSession(SparkSession session) {
    this.sparkSession = session;
    this.spark = session;
  }
  
  protected boolean isSessionActive() {
    return sparkSession != null && !sparkSession.sparkContext().isStopped();
  }
}
