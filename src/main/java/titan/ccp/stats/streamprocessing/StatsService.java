package titan.ccp.stats.streamprocessing;


import com.google.common.math.Stats;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.common.kafka.simpleserdes.BufferDeserializer;
import titan.ccp.common.kafka.simpleserdes.BufferSerializer;
import titan.ccp.common.kafka.simpleserdes.ReadBuffer;
import titan.ccp.common.kafka.simpleserdes.SimpleSerdes;
import titan.ccp.common.kafka.simpleserdes.WriteBuffer;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.stats.streamprocessing.util.StatsFactory;


public class StatsService {

  private static final Logger LOGGER = LoggerFactory.getLogger(StatsService.class);

  private final Serdes serdes = new Serdes("http://localhost:8081");

  public void run() {

    final Serde<WeekdayKey> weekdayKeySerde =
        SimpleSerdes.create(new WeekdayKeySerializer(), new WeekdayKeyDeserializer());
    final Serde<HourKey> hourKeySerde =
        SimpleSerdes.create(new HourKeySerializer(), new HourKeyDeserializer());

    final StreamsBuilder streamBuilder = new StreamsBuilder();
    // streamBuilder.stream(topic, consumed)
    final KStream<String, ActivePowerRecord> realRecords = streamBuilder.stream("active-power",
        Consumed.with(this.serdes.string(), this.serdes.activePower()));
    // .peek((k, v) -> LOGGER.info("Received record {}.", v));


    final ZoneId zone = ZoneId.of("Europe/Paris");

    realRecords.selectKey((key, value) -> {
      final Instant instant = Instant.ofEpochMilli(value.getTimestamp());
      final LocalDateTime dateTime = LocalDateTime.ofInstant(instant, zone);
      final DayOfWeek dayOfWeek = dateTime.getDayOfWeek();
      return new WeekdayKey(dayOfWeek, value.getIdentifier());
    }).groupByKey(Grouped.with(weekdayKeySerde, this.serdes.activePower()))
        .windowedBy(TimeWindows.of(Duration.ofDays(365)).advanceBy(Duration.ofDays(30)))
        .aggregate(() -> Stats.of(),
            (k, record, stats) -> StatsFactory.accumulate(stats, record.getValueInW()),
            Materialized.with(weekdayKeySerde, this.serdes.stats()))
        .toStream().foreach((k, v) -> System.out.println("" + k.key().dayOfWeek + ',' + k.window()
            + ',' + v.max() + ',' + v.min() + ',' + v.mean()));
    // .print(Printed.toSysOut());


    realRecords.selectKey((key, value) -> {
      final Instant instant = Instant.ofEpochMilli(value.getTimestamp());
      final LocalDateTime dateTime = LocalDateTime.ofInstant(instant, zone);
      final int hour = dateTime.getHour();
      return new HourKey(hour, value.getIdentifier());
    }).groupByKey(Grouped.with(hourKeySerde, this.serdes.activePower()))
        .aggregate(() -> Stats.of(),
            (k, record, stats) -> StatsFactory.accumulate(stats, record.getValueInW()),
            Materialized.with(hourKeySerde, this.serdes.stats()))
        .toStream().foreach((k, v) -> System.out
            .println("" + k.hour + ',' + v.max() + ',' + v.min() + ',' + v.mean()));
    // .print(Printed.toSysOut());


    // realRecordsGrouped.windowedBy(TimeWindows.of(Duration.ofDays(30)).advanceBy(Duration.ofDays(1)));


    final Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "titan-ccp-stats-test-0.0.9");
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    final Topology topology = streamBuilder.build();
    final KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
    kafkaStreams.start();
  }

  private static class WeekdayKey {

    public final DayOfWeek dayOfWeek;

    public final String sensorId;

    public WeekdayKey(final DayOfWeek dayOfWeek, final String sensorId) {
      this.dayOfWeek = dayOfWeek;
      this.sensorId = sensorId;
    }

    @Override
    public String toString() {
      return this.sensorId + ";" + this.dayOfWeek.toString();
    }

  }

  private static class WeekdayKeySerializer implements BufferSerializer<WeekdayKey> {

    @Override
    public void serialize(final WriteBuffer buffer, final WeekdayKey data) {
      buffer.putInt(data.dayOfWeek.getValue());
      buffer.putString(data.sensorId);
    }

  }

  private static class WeekdayKeyDeserializer implements BufferDeserializer<WeekdayKey> {

    @Override
    public WeekdayKey deserialize(final ReadBuffer buffer) {
      final DayOfWeek dayOfWeek = DayOfWeek.of(buffer.getInt());
      final String sensorId = buffer.getString();
      return new WeekdayKey(dayOfWeek, sensorId);
    }

  }

  private static class HourKey {

    public final int hour;

    public final String sensorId;

    public HourKey(final int hour, final String sensorId) {
      this.hour = hour;
      this.sensorId = sensorId;
    }

    @Override
    public String toString() {
      return this.sensorId + ";" + this.hour;
    }

  }

  private static class HourKeySerializer implements BufferSerializer<HourKey> {

    @Override
    public void serialize(final WriteBuffer buffer, final HourKey data) {
      buffer.putInt(data.hour);
      buffer.putString(data.sensorId);
    }

  }

  private static class HourKeyDeserializer implements BufferDeserializer<HourKey> {

    @Override
    public HourKey deserialize(final ReadBuffer buffer) {
      final int hour = buffer.getInt();
      final String sensorId = buffer.getString();
      return new HourKey(hour, sensorId);
    }

  }


  public static void main(final String[] args) {
    new StatsService().run();

  }

}
