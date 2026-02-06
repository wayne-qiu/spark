package com.societegenerale.volcker;

import com.societegenerale.volcker.ulti.SparkTemplate;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class VolckerComplianceApplication implements CommandLineRunner {
  
  @Autowired
  private Map<String, SparkTemplate> processors;

  public static void main(String[] args) {
    // This starts the Spring Context
    SpringApplication.run(VolckerComplianceApplication.class, args);
  }

  @Override
  public void run(String... args) throws Exception {
    // Execute services by name using processors map
    System.out.println("Available processors: " + String.join(", ", processors.keySet()));
    
    // Example: Execute services by name
    // processors.get("volckerTop10").run();
    // processors.get("volckerFIFO").run();
    // processors.get("fifoInventory").run();
    // processors.get("crossBatchAging").run();
    // processors.get("mapGroups").run();
    // streamingWindow
    
    // For now, execute volckerTop10 as default
    processors.get("streamingWindow").run();

    // Note: The application will stay alive if non-daemon threads are running
    // (common with Spark), or exit if everything finishes.
    // If you want it to shut down immediately after the job, uncomment:
    // System.exit(0);
  }
}