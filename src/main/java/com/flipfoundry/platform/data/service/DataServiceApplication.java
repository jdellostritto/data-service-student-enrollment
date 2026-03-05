package com.flipfoundry.platform.data.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Bookings Data Service - Kafka Streams Processor
 *
 * Consumes Debezium CDC events from Kafka topics, joins booking data with
 * booking rooms data using foreign key joins, and materializes the results
 * to Cassandra for downstream applications.
 */
@SpringBootApplication(scanBasePackages = {"dbz_mfd_dev_ga_1", "com.flipfoundry.platform.data.service"})
public class DataServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(DataServiceApplication.class, args);
  }

}
