package titan.ccp.stats.streamprocessing;

import com.google.common.math.Stats;
import org.apache.kafka.streams.kstream.Windowed;
import titan.ccp.model.records.HourOfDayActivePowerRecord;

public class HourOfDayRecordFactory
    implements StatsRecordFactory<HourKey, HourOfDayActivePowerRecord> {

  @Override
  public HourOfDayActivePowerRecord create(final Windowed<HourKey> windowed, final Stats stats) {
    return new HourOfDayActivePowerRecord(
        windowed.key().getSensorId(),
        windowed.key().getHour(),
        windowed.window().start(),
        windowed.window().end(),
        stats.count(),
        stats.mean(),
        stats.populationVariance(),
        stats.min(),
        stats.max());
  }

}
