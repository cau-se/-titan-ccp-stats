package titan.ccp.stats.api;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.specific.SpecificRecord;
import titan.ccp.stats.api.util.Interval;

/**
 * A proxy class to encapsulate the database and queries to it.
 *
 * @param <T> type of records in this repository
 */
public class StatsRepository<T extends SpecificRecord> {

  private static final Duration WINDOW_UPDATE_RATE = Duration.ofHours(1);

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
        this::updateCurrentInterval,
        0, // Call immediately the first time
        WINDOW_UPDATE_RATE.toMillis(),
        TimeUnit.MILLISECONDS);
    // this.updateCurrentInterval();
  }

  /**
   * Returns the most recent statistics for a given sensor identifier.
   */
  public List<T> get(final String identifier) {
    final Interval currentInterval = this.currentInterval;
    if (currentInterval == null) {
      return List.of();
    }

    final Statement statement = QueryBuilder // NOPMD no close()
        .select().all()
        .from(this.mapping.getTableName())
        .where(QueryBuilder.eq(this.mapping.getIdentifierColumn(), identifier))
        .and(QueryBuilder.eq(
            this.mapping.getPeriodStartColumn(),
            currentInterval.getStart().toEpochMilli()))
        .and(QueryBuilder.eq(
            this.mapping.getPeriodEndColumn(),
            currentInterval.getEnd().toEpochMilli()));

    final ResultSet resultSet = this.cassandraSession.execute(statement); // NOPMD no close()

    return resultSet.all().stream()
        .map(this.mapping.getMapper())
        .collect(Collectors.toList());
  }

  private void updateCurrentInterval() {
    final Instant now = Instant.now();

    final Statement statement = QueryBuilder // NOPMD no close()
        .select(
            this.mapping.getIdentifierColumn(),
            this.mapping.getPeriodStartColumn(),
            this.mapping.getPeriodEndColumn())
        .distinct()
        .from(this.mapping.getTableName());

    final ResultSet resultSet = this.cassandraSession.execute(statement); // NOPMD no close()

    this.currentInterval = resultSet.all().stream()
        .map(row -> Interval.of(
            Instant.ofEpochMilli(row.get(this.mapping.getPeriodStartColumn(), TypeCodec.bigint())),
            Instant.ofEpochMilli(row.get(this.mapping.getPeriodEndColumn(), TypeCodec.bigint()))))
        .distinct()
        .sorted((i1, i2) -> i1.getEnd().compareTo(i2.getEnd()))
        .filter(interval -> !interval.getEnd().isBefore(now))
        .findFirst()
        .orElse(this.currentInterval);
  }

}
