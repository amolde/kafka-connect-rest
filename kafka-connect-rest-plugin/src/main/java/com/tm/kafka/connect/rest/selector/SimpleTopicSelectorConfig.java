package com.tm.kafka.connect.rest.selector;


import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;


public class SimpleTopicSelectorConfig extends AbstractConfig {

  public static final String TOPIC_LIST_CONFIG = "rest.source.destination.topics";
  private static final String TOPIC_LIST_DOC = "The list of destination topics for the REST source connector.";
  private static final String TOPIC_LIST_DISPLAY = "Source destination topics";


  protected SimpleTopicSelectorConfig(ConfigDef config, Map<String, ?> parsedConfig) {
    super(config, parsedConfig);
  }

  public SimpleTopicSelectorConfig(Map<String, ?> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    String group = "REST_HTTP";
    int orderInGroup = 0;
    return new ConfigDef()
      .define(TOPIC_LIST_CONFIG,
        Type.LIST,
        NO_DEFAULT_VALUE,
        Importance.HIGH,
        TOPIC_LIST_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.SHORT,
        TOPIC_LIST_DISPLAY)
      ;
  }

  public List<String> getTopics() {
    return this.getList(TOPIC_LIST_CONFIG);
  }
}
