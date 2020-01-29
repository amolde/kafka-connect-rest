package com.tm.kafka.connect.rest.filter;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.errors.ConnectException;
import java.io.IOException;

import java.util.Map;

import com.api.jsonata4java.expressions.EvaluateException;
import com.api.jsonata4java.expressions.EvaluateRuntimeException;
import com.api.jsonata4java.expressions.Expressions;
import com.api.jsonata4java.expressions.ParseException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonataMessageFilter implements MessageFilter, Configurable {

  private static Logger log = LoggerFactory.getLogger(JsonataMessageFilter.class);

  private Expressions expr;
  private ObjectMapper mapper = new ObjectMapper();

  @Override
  public void configure(Map<String, ?> props) throws ConnectException {

    final JsonataMessageFilterConfig config = new JsonataMessageFilterConfig(props);

    try {
      expr = Expressions.parse(config.getExpression());
    } catch (ParseException e) {
      log.error(e.getLocalizedMessage(), e);
      throw new ConnectException(e);
    } catch (EvaluateRuntimeException ere) {
      log.error(ere.getLocalizedMessage(), ere);
      throw new ConnectException(ere);
    }
  }

  @Override
  public boolean matches(String messageBody) throws ConnectException {
    try {
      JsonNode result = expr.evaluate(mapper.readTree(messageBody));
      if (result == null) {
        return false;
      }
      return true;
    } catch (EvaluateException | JsonProcessingException e) {
      log.error(e.getLocalizedMessage(), e);
      throw new ConnectException(e);
    } catch (IOException e1) {
      log.error(e1.getLocalizedMessage(), e1);
      throw new ConnectException(e1);
    }
  }
}
