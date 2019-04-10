package titan.ccp.stats.streamprocessing;

import java.time.LocalDateTime;

public class HourKeyFactory implements StatsKeyFactory<HourKey> {

  @Override
  public HourKey createKey(final String sensorId, final LocalDateTime dateTime) {
    final int hourOfDay = dateTime.getHour();
    return new HourKey(hourOfDay, sensorId);
  }

  @Override
  public String getSensorId(final HourKey key) {
    return key.getSensorId();
  }

}
