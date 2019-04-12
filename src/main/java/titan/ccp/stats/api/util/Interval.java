package titan.ccp.stats.api.util;

import java.time.Instant;
import java.util.Objects;

/**
 * Representing an interval of time consisting of a start {@link Instant} and an end
 * {@link Instant}.
 */
public final class Interval {

  private final Instant start;
  private final Instant end;

  private Interval(final Instant start, final Instant stop) {
    this.start = start;
    this.end = stop;
  }

  public Instant getStart() {
    return this.start;
  }

  public Instant getEnd() {
    return this.end;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.end, this.start);
  }

  @Override
  public boolean equals(final Object otherInterval) {
    if (this == otherInterval) {
      return true;
    }
    if (otherInterval instanceof Interval) {
      final Interval other = (Interval) otherInterval;
      return Objects.equals(this.end, other.end) && Objects.equals(this.start, other.start);
    }
    return false;
  }

  @Override
  public String toString() {
    return '[' + this.start.toString() + ';' + this.end.toString() + ']';
  }

  public static Interval of(final Instant start, final Instant stop) { // NOPMD
    return new Interval(start, stop);
  }

}
