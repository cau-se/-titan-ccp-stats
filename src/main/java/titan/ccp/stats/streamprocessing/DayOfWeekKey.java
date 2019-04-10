package titan.ccp.stats.streamprocessing;

import java.time.DayOfWeek;

public class DayOfWeekKey {

  private final DayOfWeek dayOfWeek;
  private final String sensorId;

  public DayOfWeekKey(final DayOfWeek dayOfWeek, final String sensorId) {
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
