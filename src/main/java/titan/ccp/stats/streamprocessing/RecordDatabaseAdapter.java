package titan.ccp.stats.streamprocessing;

import org.apache.avro.specific.SpecificRecord;

/**
 * Holds the property names for a statistics record (which is an Avro record).
 *
 * @param <T> Record type this adapter is for.
 */
public class RecordDatabaseAdapter<T extends SpecificRecord> {

  private static final String DEFAULT_IDENTIFIER_FIELD = "identifier";
  private static final String DEFAULT_PERIOD_START_FIELD = "periodStart";
  private static final String DEFAULT_PERIOD_END_FIELD = "periodEnd";

  private final Class<? extends T> clazz;
  private final String identifierField;
  private final String timeUnitField;
  private final String periodStartField;
  private final String periodEndField;

  /**
   * Create a new {@link RecordDatabaseAdapter} for the given record type by setting its time unit
   * property (e.g., day of week or hour of day) and default fields for the other properties.
   */
  public RecordDatabaseAdapter(final Class<? extends T> clazz, final String timeUnitField) {
    this(clazz,
        DEFAULT_IDENTIFIER_FIELD,
        timeUnitField,
        DEFAULT_PERIOD_START_FIELD,
        DEFAULT_PERIOD_END_FIELD);
  }

  /**
   * Create a new {@link RecordDatabaseAdapter} for the given record type by setting all its
   * required properties.
   */
  public RecordDatabaseAdapter(final Class<? extends T> clazz,
      final String identifierField,
      final String timeUnitField,
      final String periodStartField,
      final String periodEndField) {
    this.clazz = clazz;
    this.identifierField = identifierField;
    this.timeUnitField = timeUnitField;
    this.periodStartField = periodStartField;
    this.periodEndField = periodEndField;
  }

  public Class<? extends T> getClazz() {
    return this.clazz;
  }

  public String getIdentifierField() {
    return this.identifierField;
  }

  public String getTimeUnitField() {
    return this.timeUnitField;
  }

  public String getPeriodStartField() {
    return this.periodStartField;
  }

  public String getPeriodEndField() {
    return this.periodEndField;
  }

}
