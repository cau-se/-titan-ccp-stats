package titan.ccp.stats.streamprocessing;

import com.datastax.driver.core.Session;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.TimeWindows;
import titan.ccp.model.records.DayOfWeekActivePowerRecord;
import titan.ccp.model.records.HourOfDayActivePowerRecord;


public class KafkaStreamsBuilder {

  private static final String APPLICATION_NAME = "titan-ccp-history";
  private static final String APPLICATION_VERSION = "0.0.1";
  private static final int COMMIT_INTERVAL_MS = 1000;

  private String bootstrapServers; // NOPMD
  private String inputTopic; // NOPMD
  private String outputTopic; // NOPMD
  private String configurationTopic; // NOPMD
  private Session cassandraSession; // NOPMD

  public KafkaStreamsBuilder bootstrapServers(final String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
    return this;
  }

  public KafkaStreamsBuilder cassandraSession(final Session cassandraSession) {
    this.cassandraSession = cassandraSession;
    return this;
  }

  public KafkaStreamsBuilder inputTopic(final String inputTopic) {
    this.inputTopic = inputTopic;
    return this;
  }

  public KafkaStreamsBuilder outputTopic(final String outputTopic) {
    this.outputTopic = outputTopic;
    return this;
  }

  public KafkaStreamsBuilder configurationTopic(final String configurationTopic) {
    this.configurationTopic = configurationTopic;
    return this;
  }



  /**
   * Builds the {@link KafkaStreams} instance.
   */
  public KafkaStreams build() {
    return new KafkaStreams(this.buildTopology(), this.buildProperties());
  }

  private Topology buildTopology() {
    // Objects.requireNonNull(this.inputTopic, "Input topic has not been set.");
    // Objects.requireNonNull(this.outputTopic, "Output topic has not been set.");
    // Objects.requireNonNull(this.configurationTopic, "Configuration topic has not been set.");
    Objects.requireNonNull(this.cassandraSession, "Cassandra session has not been set.");
    // TODO log parameters
    final TopologyBuilder topologyBuilder = new TopologyBuilder(this.cassandraSession);
    topologyBuilder.<WeekdayKey, DayOfWeekActivePowerRecord>addStat(
        new WeekdayKeyFactory(),
        WeekdayKeySerde.serde(),
        TimeWindows.of(Duration.ofDays(365)).advanceBy(Duration.ofDays(30)),
        new DayOfWeekRecordFactory(),
        new RecordDatabaseAdapter<>(DayOfWeekActivePowerRecord.class,
            "dayOfWeek"));
    topologyBuilder.addStat(
        new HourKeyFactory(),
        HourKeySerde.serde(),
        TimeWindows.of(Duration.ofDays(30)).advanceBy(Duration.ofDays(1)),
        new HourOfDayRecordFactory(),
        new RecordDatabaseAdapter<>(HourOfDayActivePowerRecord.class, "hourOfDay"));
    return topologyBuilder.build();
  }

  private Properties buildProperties() {
    final Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG,
        APPLICATION_NAME + '-' + APPLICATION_VERSION); // TODO as parameter
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS); // TODO as param.
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
    return properties;
  }

}
