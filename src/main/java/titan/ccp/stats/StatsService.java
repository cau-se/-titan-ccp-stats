package titan.ccp.stats;

import org.apache.kafka.streams.KafkaStreams;
import titan.ccp.common.cassandra.SessionBuilder;
import titan.ccp.common.cassandra.SessionBuilder.ClusterSession;
import titan.ccp.stats.streamprocessing.KafkaStreamsBuilder;

public class StatsService {

  public void run() {
    final ClusterSession clusterSession = new SessionBuilder()
        .contactPoint("localhost")
        .port(9042)
        .keyspace("titanccp")
        .build();

    final KafkaStreams kafkaStreams = new KafkaStreamsBuilder()
        .cassandraSession(clusterSession.getSession())
        .bootstrapServers("localhost:9092")
        .build();
    kafkaStreams.start();
  }

  public static void main(final String[] args) {
    new StatsService().run();

  }

}
