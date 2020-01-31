package org.apache.kafka.connect.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.script.ScriptException;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.jsonata.Jsonata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class JsonataEval<R extends ConnectRecord<R>> implements Transformation<R> {

  private static Logger log = LoggerFactory.getLogger(JsonataEval.class);

  private String template;
  private String processFlagTemplate;
  private String processFlagFieldName;
  private String transformedJsonFieldName;

  private JsonConverter jsonConverter;
  private ObjectMapper objectMapper = new ObjectMapper();
  private Jsonata templateExpr;
  private Jsonata processFlagTemplateExpr;

  private static final String TEMPLATE_CONFIG = "template";
  private static final String TEMPLATE_DEFAULT = "$";
  private static final String TEMPLATE_DOC = "JSONata expression.";

  private static final String PROCESS_FLAG_TEMPLATE_CONFIG = "processFlagTemplate";
  private static final String PROCESS_FLAG_TEMPLATE_DEFAULT = "$boolean(true)";
  private static final String PROCESS_FLAG_TEMPLATE_DOC = "JSONata expression to calculate process flag.";

  private static final String PROCESS_FLAG_NAME_CONFIG = "processFlagFieldName";
  private static final String PROCESS_FLAG_NAME_DEFAULT = "__process";
  private static final String PROCESS_FLAG_NAME_DOC = "Field name to store the process flag.";

  private static final String TRANSFORMED_JSON_FIELD_NAME_CONFIG = "transformedResultFieldName";
  private static final String TRANSFORMED_JSON_FIELD_NAME_DEFAULT = "json";
  private static final String TRANSFORMED_JSON_FIELD_NAME_DOC = "Field name to store the transformed JSON.";

  private static final String PURPOSE = "JSONata Transformation";
  private static final String TRUE = "true";

  private static Schema resultSchema;

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(PROCESS_FLAG_TEMPLATE_CONFIG, ConfigDef.Type.STRING, PROCESS_FLAG_TEMPLATE_DEFAULT,
          ConfigDef.Importance.MEDIUM, PROCESS_FLAG_TEMPLATE_DOC)

      .define(TEMPLATE_CONFIG, ConfigDef.Type.STRING, TEMPLATE_DEFAULT, ConfigDef.Importance.MEDIUM, TEMPLATE_DOC)

      .define(TRANSFORMED_JSON_FIELD_NAME_CONFIG, ConfigDef.Type.STRING, TRANSFORMED_JSON_FIELD_NAME_DEFAULT,
          ConfigDef.Importance.MEDIUM, TRANSFORMED_JSON_FIELD_NAME_DOC)

      .define(PROCESS_FLAG_NAME_CONFIG, ConfigDef.Type.STRING, PROCESS_FLAG_NAME_DEFAULT, ConfigDef.Importance.MEDIUM,
          PROCESS_FLAG_NAME_DOC);

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

    try {
      jsonConverter = new JsonConverter();
      Map<String, Object> converterConfig = new HashMap<>();
      converterConfig.put("schemas.enable", "false");
      converterConfig.put("schemas.cache.size", "10");
      jsonConverter.configure(props, false);
    } catch (Exception e) {
      throw new ConfigException(e.getLocalizedMessage(), e);
    }

    template = config.getString(TEMPLATE_CONFIG);
    transformedJsonFieldName = config.getString(TRANSFORMED_JSON_FIELD_NAME_CONFIG);
    processFlagTemplate = config.getString(PROCESS_FLAG_TEMPLATE_CONFIG);
    processFlagFieldName = config.getString(PROCESS_FLAG_NAME_CONFIG);

    try {
      templateExpr = new Jsonata(template);
      processFlagTemplateExpr = new Jsonata(processFlagTemplate);
    } catch (NoSuchMethodException e) {
      log.error(e.getLocalizedMessage(), e);
      throw new ConfigException(e.getLocalizedMessage(), e);
    } catch (ScriptException ere) {
      log.error(ere.getLocalizedMessage(), ere);
      throw new ConfigException(ere.getLocalizedMessage(), ere);
    }

    resultSchema = SchemaBuilder.struct().name("edu.neu.kafka.transforms.jsonata.result").version(1)
        .doc("Transformed JSON with process flag").field(transformedJsonFieldName, Schema.STRING_SCHEMA)
        .field(processFlagFieldName, Schema.BOOLEAN_SCHEMA).build();
  }

  private String parseJson(R record) {
    final Struct value = requireStruct(operatingValue(record), PURPOSE);
    try {
      return objectMapper.writeValueAsString(
          objectMapper.readTree(jsonConverter.fromConnectData(record.topic(), record.valueSchema(), value)));
    } catch (IOException e) {
      log.error(e.getLocalizedMessage(), e);
      throw new ConnectException(e);
    }
  }

  private String getTransformedJson(String jsonPayload) {
    try {
      return templateExpr.evaluate(jsonPayload);
    } catch (NoSuchMethodException e) {
      log.error(e.getLocalizedMessage(), e);
      throw new ConnectException(e);
    } catch (ScriptException e) {
      log.error(e.getLocalizedMessage(), e);
      throw new ConnectException(e);
    }
  }

  private boolean getProcessFlag(String jsonPayload) {
    try {
      return TRUE.equals(processFlagTemplateExpr.evaluate(jsonPayload));
    } catch (NoSuchMethodException e) {
      log.error(e.getLocalizedMessage(), e);
      throw new ConnectException(e);
    } catch (ScriptException e) {
      log.error(e.getLocalizedMessage(), e);
      throw new ConnectException(e);
    }
  }

  @Override
  public R apply(R record) {
    if (operatingSchema(record) == null) {
      throw new ConnectException("This transformation does not work when schema is not enabled!");
    }

    String jsonPayload = parseJson(record);
    String result = getTransformedJson(jsonPayload);
    boolean processFlag = getProcessFlag(jsonPayload);

    final Struct transformedValue = new Struct(resultSchema);
    transformedValue.put(transformedJsonFieldName, result);
    transformedValue.put(processFlagFieldName, processFlag);

    return newRecord(record, resultSchema, transformedValue);
  }

  @Override
  public void close() {
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends JsonataEval<R> {
    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue,
          record.valueSchema(), record.value(), record.timestamp());
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends JsonataEval<R> {
    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema,
          updatedValue, record.timestamp());
    }
  }
}
