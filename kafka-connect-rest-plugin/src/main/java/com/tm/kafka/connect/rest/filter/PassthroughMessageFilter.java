package com.tm.kafka.connect.rest.filter;

public class PassthroughMessageFilter implements MessageFilter {

  @Override
  public boolean matches(String messageBody) {
      return true;
  }
}
