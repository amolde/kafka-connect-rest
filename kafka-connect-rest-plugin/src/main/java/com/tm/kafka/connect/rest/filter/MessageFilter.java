package com.tm.kafka.connect.rest.filter;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * A class used to select which topic a source message should be sent to.
 * <p>
 * Note: This is a Service Provider Interface (SPI)
 * All implementations should be listed in
 * META-INF/services/com.tm.kafka.connect.rest.http.payload.PayloadGenerator
 */
public interface MessageFilter {

  boolean matches(String messageBody) throws ConnectException;
}
