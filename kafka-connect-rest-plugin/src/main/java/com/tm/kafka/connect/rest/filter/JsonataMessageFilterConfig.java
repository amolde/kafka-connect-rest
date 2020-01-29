package com.tm.kafka.connect.rest.filter;

import com.tm.kafka.connect.rest.VersionUtil;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonataMessageFilterConfig extends AbstractConfig {

  public static final String MESSAGE_FILTER_EXPRESSION_CONFIG = "rest.sink.jsonata.message.filter.expression";
  private static final String MESSAGE_FILTER_EXPRESSION_DOC = "The JSONata expression to evaluate. If evaluated positively, the message will be sent to the sink.";
  private static final String MESSAGE_FILTER_EXPRESSION_DISPLAY = "Message filter expression";
  private static final String MESSAGE_FILTER_EXPRESSION_DEFAULT = "{}";


  protected JsonataMessageFilterConfig(ConfigDef config, Map<String, ?> parsedConfig) {
    super(config, parsedConfig);
  }

  public JsonataMessageFilterConfig(Map<String, ?> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    String group = "REST_HTTP";
    int orderInGroup = 0;
    return new ConfigDef()
      .define(MESSAGE_FILTER_EXPRESSION_CONFIG,
        Type.STRING,
        MESSAGE_FILTER_EXPRESSION_DEFAULT,
        Importance.HIGH,
        MESSAGE_FILTER_EXPRESSION_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.LONG,
        MESSAGE_FILTER_EXPRESSION_DISPLAY
      )
      ;
  }

  public String getExpression() {
    return this.getString(MESSAGE_FILTER_EXPRESSION_CONFIG);
  }
}
