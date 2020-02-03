package titan.ccp.stats;

import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.streams.KafkaStreams;
import titan.ccp.common.cassandra.SessionBuilder;
import titan.ccp.common.cassandra.SessionBuilder.ClusterSession;
import titan.ccp.common.configuration.Configurations;
import titan.ccp.stats.api.RestApiServer;
import titan.ccp.stats.streamprocessing.KafkaStreamsBuilder;

/**
 * The Stats microservice.
 */
public class StatsService {

  private final Configuration config = Configurations.create();

  /**
   * Start the microservice.
   */
  public void run() {
    final ClusterSession clusterSession = new SessionBuilder()
        .contactPoint(this.config.getString(ConfigurationKeys.CASSANDRA_HOST))
        .port(this.config.getInt(ConfigurationKeys.CASSANDRA_PORT))
        .keyspace(this.config.getString(ConfigurationKeys.CASSANDRA_KEYSPACE))
        .timeoutInMillis(this.config.getInt(ConfigurationKeys.CASSANDRA_INIT_TIMEOUT_MS))
        .build();

    final KafkaStreams kafkaStreams = new KafkaStreamsBuilder()
        .cassandraSession(clusterSession.getSession())
        .bootstrapServers(this.config.getString(ConfigurationKeys.KAFKA_BOOTSTRAP_SERVERS))
        .activePowerTopic(this.config.getString(ConfigurationKeys.KAFKA_TOPIC_ACTIVE_POWER))
        .aggrActivePowerTopic(
            this.config.getString(ConfigurationKeys.KAFKA_TOPIC_AGGR_ACTIVE_POWER))
        .schemaRegistry(this.config.getString(ConfigurationKeys.SCHEMA_REGISTRY_URL))
        .build();
    kafkaStreams.start();

    final RestApiServer apiServer = new RestApiServer(
        clusterSession.getSession(),
        this.config.getInt(ConfigurationKeys.WEBSERVER_PORT),
        this.config.getBoolean(ConfigurationKeys.WEBSERVER_CORS));
    apiServer.start();
  }

  public static void main(final String[] args) {
    new StatsService().run();
  }

}
