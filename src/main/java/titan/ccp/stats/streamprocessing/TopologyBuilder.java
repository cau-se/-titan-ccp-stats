package titan.ccp.stats.streamprocessing;

import com.datastax.driver.core.Session;
import com.google.common.math.Stats;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import titan.ccp.common.avro.cassandra.AvroDataAdapter;
import titan.ccp.common.cassandra.CassandraWriter;
import titan.ccp.common.cassandra.PredefinedTableNameMappers;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.stats.streamprocessing.util.StatsFactory;

/**
 * Builds Kafka Stream Topology for the Stats microservice.
 */
public class TopologyBuilder {

  private final ZoneId zone = ZoneId.of("Europe/Paris"); // TODO as parameter
  private final String activePowerTopic = "active-power"; // TODO as parameter
  private final Serdes serdes = new Serdes("http://localhost:8081"); // TODO as paramter

  private final StreamsBuilder builder = new StreamsBuilder();
  private final KStream<String, ActivePowerRecord> recordStream;
  private final CassandraWriter<SpecificRecord> cassandraWriter;
  private final CassandraKeySelector cassandraKeySelector;

  public TopologyBuilder(final Session cassandraSession) {
    this.recordStream = this.builder.stream(this.activePowerTopic,
        Consumed.with(this.serdes.string(), this.serdes.activePower()));

    this.cassandraKeySelector = new CassandraKeySelector();
    this.cassandraWriter = CassandraWriter.builder(cassandraSession, new AvroDataAdapter())
        .tableNameMapper(PredefinedTableNameMappers.SIMPLE_CLASS_NAME)
        .primaryKeySelectionStrategy(this.cassandraKeySelector)
        .build();
  }

  public <K, R extends SpecificRecord> void addStat(
      final StatsKeyFactory<K> keyFactory,
      final Serde<K> keySerde,
      final TimeWindows timeWindows,
      final StatsRecordFactory<K, R> statsRecordFactory,
      final RecordDatabaseAdapter<R> recordDatabaseAdapter) {

    this.cassandraKeySelector.addRecordDatabaseAdapter(recordDatabaseAdapter);

    this.recordStream
        .selectKey((key, value) -> {
          final Instant instant = Instant.ofEpochMilli(value.getTimestamp());
          final LocalDateTime dateTime = LocalDateTime.ofInstant(instant, this.zone);
          return keyFactory.createKey(value.getIdentifier(), dateTime);
        })
        .groupByKey(Grouped.with(keySerde, this.serdes.activePower()))
        .windowedBy(timeWindows)
        .aggregate(
            () -> Stats.of(),
            (k, record, stats) -> StatsFactory.accumulate(stats, record.getValueInW()),
            Materialized.with(keySerde, this.serdes.stats()))
        .toStream()
        .map((key, value) -> KeyValue.pair(
            keyFactory.getSensorId(key.key()),
            statsRecordFactory.create(key, value)))
        // TODO Temp
        .peek((k, v) -> System.out.println(k + ": " + v))
        // .through("my-topic", Produced.with(serdes.string(), serdes.windowedActivePowerValues()))
        .foreach((k, record) -> this.cassandraWriter.write(record));
    // TODO Publish
    // TODO write to Cassandra
  }

  public Topology build() {
    return this.builder.build();
  }

}
