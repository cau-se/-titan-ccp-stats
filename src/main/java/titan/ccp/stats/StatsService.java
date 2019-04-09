package titan.ccp.stats;

import org.apache.kafka.streams.KafkaStreams;
import titan.ccp.stats.streamprocessing.KafkaStreamsBuilder;

public class StatsService {

  public void run() {
    final KafkaStreams kafkaStreams = new KafkaStreamsBuilder().build();
    kafkaStreams.start();
  }

  public static void main(final String[] args) {
    new StatsService().run();

  }

}
