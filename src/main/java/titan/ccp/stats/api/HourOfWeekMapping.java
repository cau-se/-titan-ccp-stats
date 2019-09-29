package titan.ccp.stats.api;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import titan.ccp.model.records.HourOfWeekActivePowerRecord;

/**
 * Class providing a factory method for creating a {@link TableRecordMapping} for
 * {@link HourOfWeekActivePowerRecord}s.
 */
public final class HourOfWeekMapping {

  private static final String TABLE_NAME = HourOfWeekActivePowerRecord.class.getSimpleName();

  private static final String DAY_OF_WEEK_COLUMN = "dayOfWeek";
  private static final String HOUR_OF_DAY_COLUMN = "hourOfDay";
  private static final String COUNT_COLUMN = "count";
  private static final String MEAN_COLUMN = "mean";
  private static final String POPULATION_VARIANCE_COLUMN = "populationVariance";
  private static final String MIN_COLUMN = "min";
  private static final String MAX_COLUMN = "max";

  private HourOfWeekMapping() {}

  private static HourOfWeekActivePowerRecord map(final Row row) {
    return new HourOfWeekActivePowerRecord(
        row.get(TableRecordMapping.DEFAULT_IDENTIFIER_COLUMN, TypeCodec.varchar()),
        row.get(DAY_OF_WEEK_COLUMN, TypeCodec.cint()),
        row.get(HOUR_OF_DAY_COLUMN, TypeCodec.cint()),
        row.get(TableRecordMapping.DEFAULT_PERIOD_START_COLUMN, TypeCodec.bigint()),
        row.get(TableRecordMapping.DEFAULT_PERIOD_END_COLUMN, TypeCodec.bigint()),
        row.get(COUNT_COLUMN, TypeCodec.bigint()),
        row.get(MEAN_COLUMN, TypeCodec.cdouble()),
        row.get(POPULATION_VARIANCE_COLUMN, TypeCodec.cdouble()),
        row.get(MIN_COLUMN, TypeCodec.cdouble()),
        row.get(MAX_COLUMN, TypeCodec.cdouble()));
  }

  public static TableRecordMapping<HourOfWeekActivePowerRecord> create() {
    return new TableRecordMapping<>(TABLE_NAME, HourOfWeekMapping::map);
  }

}
