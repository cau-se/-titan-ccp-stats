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
import titan.ccp.model.records.HourOfWeekActivePowerRecord;

/**
 * Builder for the statistics {@link KafkaStreams} configuration.
 */
public class KafkaStreamsBuilder {

  private static final String APPLICATION_NAME = "titan-ccp-stats";
  private static final String APPLICATION_VERSION = "0.0.1";

  private String bootstrapServers; // NOPMD
  private String activePowerTopic; // NOPMD
  private String aggrActivePowerTopic; // NOPMD
  private Session cassandraSession; // NOPMD
  private int numThreads = -1; // NOPMD
  private int commitIntervalMs = -1; // NOPMD
  private int cacheMaxBytesBuffering = -1; // NOPMD

  public KafkaStreamsBuilder bootstrapServers(final String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
    return this;
  }

  public KafkaStreamsBuilder cassandraSession(final Session cassandraSession) {
    this.cassandraSession = cassandraSession;
    return this;
  }

  public KafkaStreamsBuilder activePowerTopic(final String activePowerTopic) {
    this.activePowerTopic = activePowerTopic;
    return this;
  }

  public KafkaStreamsBuilder aggrActivePowerTopic(final String aggrActivePowerTopic) {
    this.aggrActivePowerTopic = aggrActivePowerTopic;
    return this;
  }

  /**
   * Sets the Kafka Streams property for the number of threads (num.stream.threads). Can be minus
   * one for using the default.
   */
  public KafkaStreamsBuilder numThreads(final int numThreads) {
    if (numThreads < -1 || numThreads == 0) {
      throw new IllegalArgumentException("Number of threads must be greater 0 or -1.");
    }
    this.numThreads = numThreads;
    return this;
  }

  /**
   * Sets the Kafka Streams property for the frequency with which to save the position (offsets in
   * source topics) of tasks (commit.interval.ms). Must be zero for processing all record, for
   * example, when processing bulks of records. Can be minus one for using the default.
   */
  public KafkaStreamsBuilder commitIntervalMs(final int commitIntervalMs) {
    if (commitIntervalMs < -1) {
      throw new IllegalArgumentException("Commit interval must be greater or equal -1.");
    }
    this.commitIntervalMs = commitIntervalMs;
    return this;
  }

  /**
   * Sets the Kafka Streams property for maximum number of memory bytes to be used for record caches
   * across all threads (cache.max.bytes.buffering). Must be zero for processing all record, for
   * example, when processing bulks of records. Can be minus one for using the default.
   */
  public KafkaStreamsBuilder cacheMaxBytesBuffering(final int cacheMaxBytesBuffering) {
    if (cacheMaxBytesBuffering < -1) {
      throw new IllegalArgumentException("Cache max bytes buffering must be greater or equal -1.");
    }
    this.cacheMaxBytesBuffering = cacheMaxBytesBuffering;
    return this;
  }

  /**
   * Builds the {@link KafkaStreams} instance.
   */
  public KafkaStreams build() {
    return new KafkaStreams(this.buildTopology(), this.buildProperties());
  }

  private Topology buildTopology() {
    Objects.requireNonNull(this.activePowerTopic,
        "Kafka topic for active power records has not been set.");
    Objects.requireNonNull(this.aggrActivePowerTopic,
        "Kafka topic for aggregated active power records has not been set.");
    Objects.requireNonNull(this.cassandraSession, "Cassandra session has not been set.");
    // TODO log parameters
    final TopologyBuilder topologyBuilder = new TopologyBuilder(
        this.cassandraSession,
        this.activePowerTopic,
        this.aggrActivePowerTopic);
    topologyBuilder.addStat(
        new DayOfWeekKeyFactory(),
        DayOfWeekKeySerde.create(),
        new DayOfWeekRecordFactory(),
        new RecordDatabaseAdapter<>(DayOfWeekActivePowerRecord.class,
            "dayOfWeek"),
        TimeWindows.of(Duration.ofDays(365)).advanceBy(Duration.ofDays(30))); // NOCS
    topologyBuilder.addStat(
        new HourOfDayKeyFactory(),
        HourOfDayKeySerde.create(),
        new HourOfDayRecordFactory(),
        new RecordDatabaseAdapter<>(HourOfDayActivePowerRecord.class, "hourOfDay"),
        TimeWindows.of(Duration.ofDays(30)).advanceBy(Duration.ofDays(1))); // NOCS
    topologyBuilder.addStat(
        new HourOfWeekKeyFactory(),
        HourOfWeekKeySerde.create(),
        new HourOfWeekRecordFactory(),
        new RecordDatabaseAdapter<>(HourOfWeekActivePowerRecord.class, "hourOfWeek"),
        TimeWindows.of(Duration.ofDays(365)).advanceBy(Duration.ofDays(30))); // NOCS
    return topologyBuilder.build();
  }

  private Properties buildProperties() {
    Objects.requireNonNull(this.bootstrapServers, "Kafka bootstrap servers have not been set.");
    final Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG,
        APPLICATION_NAME + '-' + APPLICATION_VERSION); // TODO as parameter
    if (this.numThreads > 0) {
      properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, this.numThreads);
    }
    if (this.commitIntervalMs >= 0) {
      properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, this.commitIntervalMs);
    }
    if (this.cacheMaxBytesBuffering >= 0) {
      properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, this.cacheMaxBytesBuffering);
    }
    return properties;
  }

}
