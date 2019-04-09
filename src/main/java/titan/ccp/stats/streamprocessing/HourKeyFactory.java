package titan.ccp.stats.streamprocessing;

import java.time.LocalDateTime;

public class HourKeyFactory implements StatsKeyFactory<HourKey> {

  @Override
  public HourKey create(final String sensorId, final LocalDateTime dateTime) {
    final int hourOfDay = dateTime.getHour();
    return new HourKey(hourOfDay, sensorId);
  }

}
