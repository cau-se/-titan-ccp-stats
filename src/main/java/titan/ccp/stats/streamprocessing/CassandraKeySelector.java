package titan.ccp.stats.streamprocessing;

import java.util.List;
import java.util.Set;
import titan.ccp.common.cassandra.ExplicitPrimaryKeySelectionStrategy;
import titan.ccp.common.cassandra.PrimaryKeySelectionStrategy;

public class CassandraKeySelector implements PrimaryKeySelectionStrategy {

  private final ExplicitPrimaryKeySelectionStrategy keySelectionStrategy;

  private static final Set<String> PARTITION_KEYS = Set.of("identifier");
  private static final Set<String> CLUSTERING_COLUMNS = Set.of("periodStart", "periodEnd");

  public CassandraKeySelector() {
    this.keySelectionStrategy = new ExplicitPrimaryKeySelectionStrategy(new AlwaysFailStrategy());
  }

  public void addRecordDatabaseAdapter(final RecordDatabaseAdapter<?> adapter) {
    final String className = adapter.getClazz().getSimpleName();
    this.keySelectionStrategy.registerPartitionKeys(className, adapter.getIdentifierField());
    this.keySelectionStrategy.registerClusteringColumns(
        className,
        adapter.getPeriodStartField(),
        adapter.getPeriodEndField(),
        adapter.getTimeUnitField());
  }

  @Override
  public Set<String> selectPartitionKeys(final String tableName,
      final List<String> possibleColumns) {
    return this.keySelectionStrategy.selectPartitionKeys(tableName, possibleColumns);
  }

  @Override
  public Set<String> selectClusteringColumns(final String tableName,
      final List<String> possibleColumns) {
    return this.keySelectionStrategy.selectClusteringColumns(tableName, possibleColumns);
  }

  private static class AlwaysFailStrategy implements PrimaryKeySelectionStrategy {

    @Override
    public Set<String> selectPartitionKeys(final String tableName,
        final List<String> possibleColumns) {
      throw new IllegalArgumentException(
          "Partition keys for " + tableName + " are not registered."); // NOCS
    }

    @Override
    public Set<String> selectClusteringColumns(final String tableName,
        final List<String> possibleColumns) {
      throw new IllegalArgumentException(
          "Clustering columns for " + tableName + " are not registered."); // NOCS
    }

  }

}
