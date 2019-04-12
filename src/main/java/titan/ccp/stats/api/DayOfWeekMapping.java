package titan.ccp.stats.api;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import titan.ccp.model.records.DayOfWeekActivePowerRecord;

/**
 * Class providing a factory method for creating a {@link TableRecordMapping} for
 * {@link DayOfWeekActivePowerRecord}s.
 */
public final class DayOfWeekMapping {

  private static final String TABLE_NAME = DayOfWeekActivePowerRecord.class.getSimpleName();

  private static final String DAY_OF_WEEK_COLUMN = "dayOfWeek";
  private static final String COUNT_COLUMN = "count";
  private static final String MEAN_COLUMN = "mean";
  private static final String POPULATION_VARIANCE_COLUMN = "populationVariance";
  private static final String MIN_COLUMN = "min";
  private static final String MAX_COLUMN = "max";

  private DayOfWeekMapping() {}

  private static DayOfWeekActivePowerRecord map(final Row row) {
    return new DayOfWeekActivePowerRecord(
        row.get(TableRecordMapping.DEFAULT_IDENTIFIER_COLUMN, TypeCodec.ascii()),
        row.get(DAY_OF_WEEK_COLUMN, TypeCodec.cint()),
        row.get(TableRecordMapping.DEFAULT_PERIOD_START_COLUMN, TypeCodec.bigint()),
        row.get(TableRecordMapping.DEFAULT_PERIOD_END_COLUMN, TypeCodec.bigint()),
        row.get(COUNT_COLUMN, TypeCodec.bigint()),
        row.get(MEAN_COLUMN, TypeCodec.cdouble()),
        row.get(POPULATION_VARIANCE_COLUMN, TypeCodec.cdouble()),
        row.get(MIN_COLUMN, TypeCodec.cdouble()),
        row.get(MAX_COLUMN, TypeCodec.cdouble()));
  }

  public static TableRecordMapping<DayOfWeekActivePowerRecord> create() {
    return new TableRecordMapping<>(TABLE_NAME, DayOfWeekMapping::map);
  }

}
