package titan.ccp.stats.streamprocessing;


import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Properties;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import titan.ccp.model.records.ActivePowerRecord;
import titan.ccp.model.records.AggregatedActivePowerRecord;
import titan.ccp.model.records.HourOfDayActivePowerRecord;

public class TopologyTest {

  private static final String POWER_TOPIC = "input";
  private static final String AGGREGATED_POWER_TOPIC = "output";
  private static final String STATS_TOPIC = "hour-of-day";
  private static final ZoneId ZONE = ZoneId.of("Europe/Paris"); // TODO as parameter

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, ActivePowerRecord> powerTopic;
  private TestInputTopic<String, AggregatedActivePowerRecord> aggregatedPowerTopic;
  private TestOutputTopic<String, HourOfDayActivePowerRecord> statsTopic;
  private Serdes serdes;

  @Before
  public void setup() {
    this.serdes = new MockedSchemaRegistrySerdes();

    final TopologyBuilder topologyBuilder = new TopologyBuilder(
        this.serdes,
        null, // Do not store to Cassandra
        POWER_TOPIC,
        AGGREGATED_POWER_TOPIC);
    topologyBuilder.addStat(
        new HourOfDayKeyFactory(),
        HourOfDayKeySerde.create(),
        new HourOfDayRecordFactory(),
        null, // Do not store to Cassandra
        TimeWindows.of(Duration.ofDays(30)).advanceBy(Duration.ofDays(1)),
        STATS_TOPIC);
    final Topology topology = topologyBuilder.build();

    // setup test driver
    final Properties props = new Properties();
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "dummy-aggregation");
    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    this.testDriver = new TopologyTestDriver(topology, props);
    this.powerTopic = this.testDriver.createInputTopic(
        POWER_TOPIC,
        this.serdes.string().serializer(),
        this.serdes.activePowerRecordValues().serializer());
    this.aggregatedPowerTopic = this.testDriver.createInputTopic(
        AGGREGATED_POWER_TOPIC,
        this.serdes.string().serializer(),
        this.serdes.aggregatedActivePowerRecordValues().serializer());
    this.aggregatedPowerTopic = this.testDriver.createInputTopic(
        AGGREGATED_POWER_TOPIC,
        this.serdes.string().serializer(),
        this.serdes.aggregatedActivePowerRecordValues().serializer());
    this.statsTopic = this.testDriver.createOutputTopic(
        STATS_TOPIC,
        this.serdes.string().deserializer(),
        this.serdes.<HourOfDayActivePowerRecord>avroValues().deserializer());
  }

  @After
  public void tearDown() {
    this.testDriver.close();
  }

  @Test
  public void testTwoRecordsSameHourOfDay() {
    // Publish input records
    final LocalDate date = LocalDate.of(2020, 01, 01);
    this.pipeInput("machine", LocalDateTime.of(date, LocalTime.of(05, 10)), 50.0);
    this.pipeInput("machine", LocalDateTime.of(date, LocalTime.of(05, 15)), 100.0);

    // Check results
    Assert.assertEquals(2, this.statsTopic.getQueueSize());

    final HourOfDayActivePowerRecord result1 = this.statsTopic.readValue();
    Assert.assertEquals("machine", result1.getIdentifier());
    Assert.assertEquals(5, result1.getHourOfDay());
    Assert.assertEquals(1, result1.getCount());
    Assert.assertEquals(50.0, result1.getMean(), 0.1);

    final HourOfDayActivePowerRecord result2 = this.statsTopic.readValue();
    Assert.assertEquals("machine", result2.getIdentifier());
    Assert.assertEquals(5, result2.getHourOfDay());
    Assert.assertEquals(2, result2.getCount());
    Assert.assertEquals(75.0, result2.getMean(), 0.1);
  }

  @Test
  public void testTwoRecordsDifferentHourOfDay() {
    // Publish input records
    final LocalDate date = LocalDate.of(2020, 01, 01);
    this.pipeInput("machine", LocalDateTime.of(date, LocalTime.of(05, 10)), 50.0);
    this.pipeInput("machine", LocalDateTime.of(date, LocalTime.of(06, 10)), 100.0);

    // Check results
    Assert.assertEquals(2, this.statsTopic.getQueueSize());

    final HourOfDayActivePowerRecord result1 = this.statsTopic.readValue();
    Assert.assertEquals("machine", result1.getIdentifier());
    Assert.assertEquals(5, result1.getHourOfDay());
    Assert.assertEquals(1, result1.getCount());
    Assert.assertEquals(50.0, result1.getMean(), 0.1);

    final HourOfDayActivePowerRecord result2 = this.statsTopic.readValue();
    Assert.assertEquals("machine", result2.getIdentifier());
    Assert.assertEquals(6, result2.getHourOfDay());
    Assert.assertEquals(1, result2.getCount());
    Assert.assertEquals(100.0, result2.getMean(), 0.1);
  }

  @Test
  public void testForwardedResultIsEarliestWindow() {
    // Publish input records
    final LocalDateTime recordTime =
        LocalDateTime.of(LocalDate.of(2020, 01, 01), LocalTime.of(05, 10));
    this.pipeInput("machine", recordTime, 50.0);

    // Check results
    Assert.assertEquals(1, this.statsTopic.getQueueSize());

    final HourOfDayActivePowerRecord result = this.statsTopic.readValue();
    System.out.println(result);
    Assert.assertEquals("machine", result.getIdentifier());
    Assert.assertEquals(5, result.getHourOfDay());
    final Instant periodEndInstant = Instant.ofEpochMilli(result.getPeriodEnd());
    final LocalDateTime periodEnd = LocalDateTime.ofInstant(periodEndInstant, ZONE);
    Assert.assertTrue(recordTime.isBefore(periodEnd) || recordTime.equals(periodEnd));
    Assert.assertTrue(
        recordTime.isAfter(periodEnd.minusDays(1)) || recordTime.equals(periodEnd.minusDays(1)));
  }

  private void pipeInput(final String identifier, final LocalDateTime timestamp,
      final double value) {
    this.pipeInput(identifier, timestamp.atZone(ZONE).toInstant(), value);
  }

  private void pipeInput(final String identifier, final Instant timestamp, final double value) {
    this.powerTopic.pipeInput(
        identifier,
        new ActivePowerRecord(identifier, timestamp.toEpochMilli(), value),
        timestamp);
  }

}
