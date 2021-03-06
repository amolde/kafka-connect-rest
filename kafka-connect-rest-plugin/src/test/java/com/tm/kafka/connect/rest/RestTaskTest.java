package com.tm.kafka.connect.rest;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.tm.kafka.connect.rest.http.payload.ConstantPayloadGeneratorConfig;
import com.tm.kafka.connect.rest.selector.SimpleTopicSelectorConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.Rule;
import org.junit.Test;

import java.net.ServerSocket;
import java.util.*;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.junit.Assert.assertEquals;

public class RestTaskTest {

  private static final String CONTENT_TYPE = "Content-Type";
  private static final String ACCEPT = "Accept";
  private static final String APPLICATION_JSON = "application/json; charset=UTF-8";

  private static final String TOPIC = "rest-source-destination-topic";
  private static final String REST_SOURCE_DESTINATION_TOPIC_LIST = TOPIC;

  private static final String METHOD = "POST";
  private static final String HEADERS_LIST = "" +
    CONTENT_TYPE + ":" + APPLICATION_JSON + ", " +
    ACCEPT + ":" + APPLICATION_JSON;
  private static final String TOPIC_SELECTOR = "com.tm.kafka.connect.rest.selector.SimpleTopicSelector";
  private static final String DATA = "{\"A\":\"B\"}";
  private static final String RESPONSE_BODY = "{\"B\":\"A\"}";
  private static final int PORT = getPort();
  private static final String PATH = "/my/resource";
  private static final String URL = "http://localhost:" + PORT + PATH;

  // Sink
  private static final int PARTITION12 = 12;
  private static final int PARTITION13 = 13;
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION12);
  private static final TopicPartition TOPIC_PARTITION2 = new TopicPartition(TOPIC, PARTITION13);

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(PORT);

  @Test
  public void restTest() throws InterruptedException {
    stubFor(post(urlEqualTo(PATH))
      .withHeader(ACCEPT, equalTo(APPLICATION_JSON))
      .willReturn(aResponse()
        .withStatus(200)
        .withHeader(CONTENT_TYPE, APPLICATION_JSON)
        .withBody(RESPONSE_BODY)));

    Map<String, String> props;
    props = new HashMap<String, String>() {
      /**
      *
      */
      private static final long serialVersionUID = -7527818904366177641L;

      {
      put(RestSourceConnectorConfig.SOURCE_METHOD_CONFIG, METHOD);
      put(RestSourceConnectorConfig.SOURCE_URL_CONFIG, URL);
      put(ConstantPayloadGeneratorConfig.REQUEST_BODY_CONFIG, DATA);
      put(ConstantPayloadGeneratorConfig.REQUEST_HEADERS_CONFIG, HEADERS_LIST);
      put(RestSourceConnectorConfig.SOURCE_TOPIC_SELECTOR_CONFIG, TOPIC_SELECTOR);
      put(SimpleTopicSelectorConfig.TOPIC_LIST_CONFIG, REST_SOURCE_DESTINATION_TOPIC_LIST);
    }};

    RestSourceTask sourceTask;
    List<SourceRecord> messages;

    sourceTask = new RestSourceTask();
    SourceTaskContext context1 = new MockSourceTaskContext();
    sourceTask.initialize(context1);
    sourceTask.start(props);
    messages = sourceTask.poll();

    assertEquals("Message count: ", 1, messages.size());
    assertEquals("Response class: ", String.class, messages.get(0).value().getClass());
    assertEquals("Response body: ", RESPONSE_BODY, messages.get(0).value());
    assertEquals("Topic: ", TOPIC, messages.get(0).topic());

    verify(postRequestedFor(urlMatching(PATH))
      .withRequestBody(equalTo(DATA))
      .withHeader(CONTENT_TYPE, matching(APPLICATION_JSON)));

    sourceTask = new RestSourceTask();
    sourceTask.initialize(context1);
    sourceTask.start(props);
    messages = sourceTask.poll();

    assertEquals("Message count: ", 1, messages.size());
    assertEquals("Response class: ", String.class, messages.get(0).value().getClass());
    assertEquals("Response body: ", RESPONSE_BODY, messages.get(0).value());
    assertEquals("Topic: ", TOPIC, messages.get(0).topic());

    verify(postRequestedFor(urlMatching(PATH))
      .withRequestBody(equalTo(DATA))
      .withHeader(CONTENT_TYPE, matching(APPLICATION_JSON)));

    wireMockRule.resetRequests();

    props = new HashMap<String, String>() {
      /**
      *
      */
      
      private static final long serialVersionUID = -5907635553106654131L;
      {
      put(RestSinkConnectorConfig.SINK_METHOD_CONFIG, METHOD);
      put(RestSinkConnectorConfig.SINK_URL_CONFIG, URL);
      put(RestSinkConnectorConfig.SINK_HEADERS_LIST_CONFIG, HEADERS_LIST);
    }};
    String key = "key1";

    long offset = 100;
    long timestamp = 200L;

    ArrayList<SinkRecord> records = new ArrayList<>();
    records.add(
      new SinkRecord(
        TOPIC,
        PARTITION12,
        STRING_SCHEMA,
        key,
        STRING_SCHEMA,
        DATA,
        offset,
        timestamp,
        TimestampType.CREATE_TIME
      ));

    RestSinkTask sinkTask;
    sinkTask = new RestSinkTask();
    Set<TopicPartition> assignment = new HashSet<>();
    assignment.add(TOPIC_PARTITION);
    assignment.add(TOPIC_PARTITION2);
    MockSinkTaskContext context = new MockSinkTaskContext(assignment);
    sinkTask.initialize(context);
    sinkTask.start(props);
    sinkTask.put(records);

    verify(postRequestedFor(urlMatching(PATH))
      .withRequestBody(equalTo(DATA))
      .withHeader(CONTENT_TYPE, matching(APPLICATION_JSON)));

    wireMockRule.resetAll();

    props.put(RestSinkConnectorConfig.SINK_METHOD_CONFIG, "GET");
    props.put(RestSinkConnectorConfig.SINK_URL_CONFIG, URL + "?");

    stubFor(get(urlMatching(PATH + ".*"))
      .withHeader(ACCEPT, equalTo(APPLICATION_JSON))
      .willReturn(aResponse()
        .withStatus(200)
        .withHeader(CONTENT_TYPE, APPLICATION_JSON)
        .withBody(RESPONSE_BODY)));

    sinkTask = new RestSinkTask();
    sinkTask.initialize(context);
    sinkTask.start(props);

    records.clear();
    Object value = "{\"id\":1, \"content\":\"Joe\"}";
    records.add(
      new SinkRecord(
        TOPIC,
        PARTITION12,
        STRING_SCHEMA,
        key,
        STRING_SCHEMA,
        value,
        offset,
        timestamp,
        TimestampType.CREATE_TIME
      ));

    sinkTask.put(records);

    verify(getRequestedFor(urlMatching(PATH + "\\?.*"))
      .withHeader(CONTENT_TYPE, matching(APPLICATION_JSON)));
  }

  private static int getPort() {
    try {
      ServerSocket s = new ServerSocket(0);
      int localPort = s.getLocalPort();
      s.close();
      return localPort;
    } catch (Exception e) {
      throw new RuntimeException("Failed to get a free PORT", e);
    }
  }

  protected static class MockSourceTaskContext implements SourceTaskContext {
    @Override
    public Map<String, String> configs() {
      return null;
    }

    @Override
    public OffsetStorageReader offsetStorageReader() {
      return new OffsetStorageReader() {

        @Override
        public <T> Map<String, Object> offset(Map<String, T> map) {
          return null;
        }

        @Override
        public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> collection) {
          return null;
        }
      };
    }
  }

  protected static class MockSinkTaskContext implements SinkTaskContext {

    private final Map<TopicPartition, Long> offsets;
    private long timeoutMs;
    private Set<TopicPartition> assignment;

    MockSinkTaskContext(Set<TopicPartition> assignment) {
      this.offsets = new HashMap<>();
      this.timeoutMs = -1L;
      this.assignment = assignment;
    }

    @Override
    public Map<String, String> configs() {
      return null;
    }

    @Override
    public void offset(Map<TopicPartition, Long> offsets) {
      this.offsets.putAll(offsets);
    }

    @Override
    public void offset(TopicPartition tp, long offset) {
      offsets.put(tp, offset);
    }

    public Map<TopicPartition, Long> offsets() {
      return offsets;
    }

    @Override
    public void timeout(long timeoutMs) {
      this.timeoutMs = timeoutMs;
    }

    public long timeout() {
      return timeoutMs;
    }

    @Override
    public Set<TopicPartition> assignment() {
      return assignment;
    }

    public void setAssignment(Set<TopicPartition> nextAssignment) {
      assignment = nextAssignment;
    }

    @Override
    public void pause(TopicPartition... partitions) {
    }

    @Override
    public void resume(TopicPartition... partitions) {
    }

    @Override
    public void requestCommit() {
    }
  }
}
