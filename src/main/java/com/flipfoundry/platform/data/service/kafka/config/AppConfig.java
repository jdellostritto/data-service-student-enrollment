package com.flipfoundry.platform.data.service.kafka.config;

import com.flipfoundry.platform.data.service.kafka.utils.NullAwareBeanUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Application configuration for Bookings Data Service.
 * Provides beans for null-aware bean utilities used in CDC event processing.
 */
@Configuration
public class AppConfig {

  @Bean
  public NullAwareBeanUtils nullAwareBeanUtils() {
    return new NullAwareBeanUtils();
  }

}
