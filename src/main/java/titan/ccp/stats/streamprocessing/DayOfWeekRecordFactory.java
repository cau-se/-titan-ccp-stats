package titan.ccp.stats.streamprocessing;

import com.google.common.math.Stats;
import org.apache.kafka.streams.kstream.Windowed;
import titan.ccp.model.records.DayOfWeekActivePowerRecord;

public class DayOfWeekRecordFactory
    implements StatsRecordFactory<WeekdayKey, DayOfWeekActivePowerRecord> {

  @Override
  public DayOfWeekActivePowerRecord create(final Windowed<WeekdayKey> windowed, final Stats stats) {
    return new DayOfWeekActivePowerRecord(
        windowed.key().getSensorId(),
        windowed.key().getDayOfWeek().getValue(),
        windowed.window().start(),
        windowed.window().end(),
        stats.count(),
        stats.mean(),
        stats.populationVariance(),
        stats.min(),
        stats.max());
  }

}
