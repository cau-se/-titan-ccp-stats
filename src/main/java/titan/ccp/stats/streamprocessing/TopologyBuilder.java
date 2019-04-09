package titan.ccp.stats.streamprocessing;

import com.google.common.math.Stats;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
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

  public TopologyBuilder() {
    this.recordStream = this.builder.stream(this.activePowerTopic,
        Consumed.with(this.serdes.string(), this.serdes.activePower()));
  }

  public <T> void addStat(final StatsKeyFactory<T> keyFactory,
      final Serde<T> keySerde,
      final TimeWindows timeWindows) {

    this.recordStream
        .selectKey((key, value) -> {
          final Instant instant = Instant.ofEpochMilli(value.getTimestamp());
          final LocalDateTime dateTime = LocalDateTime.ofInstant(instant, this.zone);
          return keyFactory.create(value.getIdentifier(), dateTime);
        })
        .groupByKey(Grouped.with(keySerde, this.serdes.activePower()))
        .windowedBy(timeWindows)
        .aggregate(
            () -> Stats.of(),
            (k, record, stats) -> StatsFactory.accumulate(stats, record.getValueInW()),
            Materialized.with(keySerde, this.serdes.stats()))
        .toStream()
        // TODO Temp
        .foreach((k, v) -> System.out.println(
            "" + k.key() + ',' + k.window() + ',' + v.max() + ',' + v.min() + ',' + v.mean()));
    // TODO Publish
    // TODO write to Cassandra
  }

  public Topology build() {
    return this.builder.build();
  }

}
