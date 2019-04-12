package titan.ccp.stats.api;

import com.datastax.driver.core.Row;
import java.util.function.Function;
import org.apache.avro.specific.SpecificRecord;

/**
 * Represents the mapping of Cassandra table rows to Avro records.
 *
 * @param <T> The Avro record to be mapped from a Cassandra table.
 */
public class TableRecordMapping<T extends SpecificRecord> {

  public static final String DEFAULT_IDENTIFIER_COLUMN = "identifier";
  public static final String DEFAULT_PERIOD_START_COLUMN = "periodStart";
  public static final String DEFAULT_PERIOD_END_COLUMN = "periodEnd";

  private final String tableName;
  private final String identifierColumn;
  private final String periodStartColumn;
  private final String periodEndColumn;
  private final Function<Row, T> mapper;

  /**
   * Create a new {@link TableRecordMapping}.
   */
  public TableRecordMapping(
      final String tableName,
      final String identifierColumn,
      final String periodStartColumn,
      final String periodEndColumn,
      final Function<Row, T> mapper) {
    this.tableName = tableName;
    this.identifierColumn = identifierColumn;
    this.periodStartColumn = periodStartColumn;
    this.periodEndColumn = periodEndColumn;
    this.mapper = mapper;
  }

  /**
   * Create a new {@link TableRecordMapping} with default values for the 'identifier', 'periodStart'
   * and 'periodEnd' column names.
   */
  public TableRecordMapping(final String tableName, final Function<Row, T> mapper) {
    this(
        tableName,
        DEFAULT_IDENTIFIER_COLUMN,
        DEFAULT_PERIOD_START_COLUMN,
        DEFAULT_PERIOD_END_COLUMN,
        mapper);
  }

  public String getTableName() {
    return this.tableName;
  }

  public String getIdentifierColumn() {
    return this.identifierColumn;
  }

  public String getPeriodStartColumn() {
    return this.periodStartColumn;
  }

  public String getPeriodEndColumn() {
    return this.periodEndColumn;
  }

  public Function<Row, T> getMapper() {
    return this.mapper;
  }



}
