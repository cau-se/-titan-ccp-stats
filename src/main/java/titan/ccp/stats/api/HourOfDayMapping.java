package titan.ccp.stats.api;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import titan.ccp.model.records.HourOfDayActivePowerRecord;

/**
 * Class providing a factory method for creating a {@link TableRecordMapping} for
 * {@link HourOfDayActivePowerRecord}s.
 */
public final class HourOfDayMapping {

  private static final String TABLE_NAME = HourOfDayActivePowerRecord.class.getSimpleName();

  private static final String HOUR_OF_DAY_COLUMN = "hourOfDay";
  private static final String COUNT_COLUMN = "count";
  private static final String MEAN_COLUMN = "mean";
  private static final String POPULATION_VARIANCE_COLUMN = "populationVariance";
  private static final String MIN_COLUMN = "min";
  private static final String MAX_COLUMN = "max";

  private HourOfDayMapping() {}

  private static HourOfDayActivePowerRecord map(final Row row) {
    return new HourOfDayActivePowerRecord(
        row.get(TableRecordMapping.DEFAULT_IDENTIFIER_COLUMN, TypeCodec.varchar()),
        row.get(HOUR_OF_DAY_COLUMN, TypeCodec.cint()),
        row.get(TableRecordMapping.DEFAULT_PERIOD_START_COLUMN, TypeCodec.bigint()),
        row.get(TableRecordMapping.DEFAULT_PERIOD_END_COLUMN, TypeCodec.bigint()),
        row.get(COUNT_COLUMN, TypeCodec.bigint()),
        row.get(MEAN_COLUMN, TypeCodec.cdouble()),
        row.get(POPULATION_VARIANCE_COLUMN, TypeCodec.cdouble()),
        row.get(MIN_COLUMN, TypeCodec.cdouble()),
        row.get(MAX_COLUMN, TypeCodec.cdouble()));
  }

  public static TableRecordMapping<HourOfDayActivePowerRecord> create() {
    return new TableRecordMapping<>(TABLE_NAME, HourOfDayMapping::map);
  }

}
