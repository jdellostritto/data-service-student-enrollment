package com.flipfoundry.platform.data.service.kafka.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.common.errors.BrokerNotAvailableException;

import java.util.Map;

/**
 *
 * Copyright 2024-2026 Flipfoundry.<br><br>
 *
 * Project:    booking-data-service<br>
 * Class:      CustomProductionExceptionHandler.java<br>
 * References: `NA`<br><br>
 *
 * @implSpec `Production exception handler.`<br>
 *
 *
 * @author Flipfoundry Team
 * @since 2021-08-15
 * @implNote Modernized for Java 21 and Spring Boot 3.5.8 in 2026
 *
 *
 */
@Slf4j
public class CustomProductionExceptionHandler implements ProductionExceptionHandler {
    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record,
                                                     Exception exception) {

        if (exception instanceof RecordTooLargeException) {
            log.error("RecordTooLargeException Continuing Stream Processing: {}", exception.getMessage());
            return ProductionExceptionHandlerResponse.CONTINUE;
        } else if (exception instanceof BrokerNotAvailableException) {
            log.error("RecordTooLargeException Continuing Stream Processing: {}", exception.getMessage());
            return ProductionExceptionHandlerResponse.FAIL;
        }
        log.error("Unhandled Exception Failing Stream Processing: {}", exception.getMessage());
        return ProductionExceptionHandlerResponse.FAIL;
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
