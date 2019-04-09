package titan.ccp.stats.streamprocessing;

public class HourKey {

  private final int hour;
  private final String sensorId;

  public HourKey(final int hour, final String sensorId) {
    this.hour = hour;
    this.sensorId = sensorId;
  }

  public int getHour() {
    return this.hour;
  }

  public String getSensorId() {
    return this.sensorId;
  }

  @Override
  public String toString() {
    return this.sensorId + ";" + this.hour;
  }

}
