package titan.ccp.stats.streamprocessing;

import java.time.DayOfWeek;
import java.util.Objects;

/**
 * Composed key of a {@link DayOfWeek} and a sensor id.
 */
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

  @Override
  public int hashCode() {
    return Objects.hash(this.dayOfWeek, this.sensorId);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof DayOfWeekKey) {
      final DayOfWeekKey other = (DayOfWeekKey) obj;
      return Objects.equals(this.dayOfWeek, other.dayOfWeek)
          && Objects.equals(this.sensorId, other.sensorId);
    }
    return false;
  }

}
