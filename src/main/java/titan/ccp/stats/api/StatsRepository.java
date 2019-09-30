package titan.ccp.stats.api;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.stats.api.util.Interval;

/**
 * A proxy class to encapsulate the database and queries to it.
 *
 * @param <T> type of records in this repository
 */
public class StatsRepository<T extends SpecificRecord> {

  private static final Logger LOGGER = LoggerFactory.getLogger(StatsRepository.class);

  private static final Duration WINDOW_UPDATE_RATE = Duration.ofHours(1);
  private static final Duration WINDOW_UPDATE_RETRY_DELAY = Duration.ofSeconds(5);

  private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
  private final Session cassandraSession;
  private final TableRecordMapping<T> mapping;

  private volatile Interval currentInterval;

  /**
   * Create a new {@link StatsRepository}.
   */
  public StatsRepository(final Session cassandraSession, final TableRecordMapping<T> mapping) {
    this.cassandraSession = cassandraSession;
    this.mapping = mapping;

    this.executor.scheduleAtFixedRate(
        this::updateCurrentInterval, 0, // Call immediately the first time
        WINDOW_UPDATE_RATE.toMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * Returns the most recent statistics for a given sensor identifier.
   */
  public List<T> get(final String identifier) {
    // Copy ref to interval for concurrent modification
    final Interval currentInterval = this.currentInterval;
    if (currentInterval == null) {
      return List.of();
    }
    return this.get(identifier, currentInterval);
  }

  /**
   * Returns the statistics for a given sensor identifier and interval.
   */
  public List<T> get(final String identifier, final Interval interval) {
    final Statement statement = QueryBuilder // NOPMD no close()
        .select().all().from(this.mapping.getTableName())
        .where(QueryBuilder.eq(this.mapping.getIdentifierColumn(), identifier))
        .and(QueryBuilder.eq(this.mapping.getPeriodStartColumn(),
            interval.getStart().toEpochMilli()))
        .and(QueryBuilder.eq(this.mapping.getPeriodEndColumn(), interval.getEnd().toEpochMilli()));

    return this.executeQuery(statement).stream().map(this.mapping.getMapper())
        .collect(Collectors.toList());
  }

  /**
   * Get all intervals of the repository.
   * 
   * @return the list of intervals
   */
  public List<Interval> getIntervals() {
    final Statement statement = QueryBuilder
        .select(this.mapping.getPeriodStartColumn(), this.mapping.getPeriodEndColumn())
        .from(this.mapping.getTableName());

    return this.executeQuery(statement).stream()
        .map(record -> Interval.of(
            Instant
                .ofEpochMilli(record.get(this.mapping.getPeriodStartColumn(), TypeCodec.bigint())),
            Instant
                .ofEpochMilli(record.get(this.mapping.getPeriodEndColumn(), TypeCodec.bigint()))))
        .distinct().sorted((i1, i2) -> i1.getEnd().compareTo(i2.getEnd()))
        .collect(Collectors.toList());
  }

  private void updateCurrentInterval() {
    final Instant now = Instant.now();
    LOGGER.info("Updating the current interval.");

    final Statement statement = QueryBuilder // NOPMD no close()
        .select(this.mapping.getIdentifierColumn(), this.mapping.getPeriodStartColumn(),
            this.mapping.getPeriodEndColumn())
        .distinct().from(this.mapping.getTableName());

    this.currentInterval = this.executeQuery(statement).stream()
        .map(row -> Interval.of(
            Instant.ofEpochMilli(row.get(this.mapping.getPeriodStartColumn(), TypeCodec.bigint())),
            Instant.ofEpochMilli(row.get(this.mapping.getPeriodEndColumn(), TypeCodec.bigint()))))
        .distinct().sorted((i1, i2) -> i1.getEnd().compareTo(i2.getEnd()))
        .filter(interval -> !interval.getEnd().isBefore(now)).findFirst()
        .orElse(this.currentInterval);

    if (this.currentInterval == null) {
      final long retryDelyinMs = WINDOW_UPDATE_RETRY_DELAY.toMillis();
      LOGGER.warn("No interval found so far. Retry in {} ms.", retryDelyinMs);
      this.executor.schedule(this::updateCurrentInterval, retryDelyinMs, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Execute the given Cassandra statement and returns a list of all rows or an empty list if this
   * query failed.
   */
  private List<Row> executeQuery(final Statement statement) {
    try {
      final ResultSet resultSet = this.cassandraSession.execute(statement); // NOPMD no close()
      return resultSet.all();
    } catch (final InvalidQueryException e) {
      LOGGER.error("Cassandra query could not be executed.", e);
      return List.of();
    }
  }
}
