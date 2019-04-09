package titan.ccp.stats.streamprocessing;

import java.time.DayOfWeek;

public class WeekdayKey {

  private final DayOfWeek dayOfWeek;
  private final String sensorId;

  public WeekdayKey(final DayOfWeek dayOfWeek, final String sensorId) {
    this.dayOfWeek = dayOfWeek;
    this.sensorId = sensorId;
  }

  public DayOfWeek getDayOfWeek() {
    return this.dayOfWeek;
  }

  public String getSensorId() {
    return this.sensorId;
  }

  @Override
  public String toString() {
    return this.sensorId + ";" + this.dayOfWeek.toString();
  }

}
