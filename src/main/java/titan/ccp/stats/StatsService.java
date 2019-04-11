package titan.ccp.stats;

import org.apache.kafka.streams.KafkaStreams;
import titan.ccp.common.cassandra.SessionBuilder;
import titan.ccp.common.cassandra.SessionBuilder.ClusterSession;
import titan.ccp.stats.streamprocessing.KafkaStreamsBuilder;

/**
 * The Stats microservice.
 */
public class StatsService {

  private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String CASSANDRA_HOST = "localhost";
  private static final int CASSANDRA_PORT = 9042;
  private static final String CASSANDRA_KEYSPACE = "titanccp";

  /**
   * Start the microservice.
   */
  public void run() {
    final ClusterSession clusterSession = new SessionBuilder()
        .contactPoint(CASSANDRA_HOST)
        .port(CASSANDRA_PORT)
        .keyspace(CASSANDRA_KEYSPACE)
        .build();

    final KafkaStreams kafkaStreams = new KafkaStreamsBuilder()
        .cassandraSession(clusterSession.getSession())
        .bootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
        .build();
    kafkaStreams.start();
  }

  public static void main(final String[] args) {
    new StatsService().run();
  }

}
