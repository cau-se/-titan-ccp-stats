package titan.ccp.stats.api.util;

import java.time.Instant;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Representing an interval of time consisting of a start {@link Instant} and an end
 * {@link Instant}.
 */
public final class Interval {

  private static final Pattern PATTERN = Pattern.compile("\\[(.*);(.*)\\]");

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

  /**
   * Obtains an instance of {@link Interval} from a text string such as
   * {@code [2018-05-19T00:00:00Z;2019-05-19T00:00:00Z]}.
   */
  public static Interval parse(final CharSequence text) {
    final Matcher matcher = PATTERN.matcher(text);
    if (!matcher.matches()) {
      throw new IllegalArgumentException();
    }
    final Instant start = Instant.parse(matcher.group(1));
    final Instant stop = Instant.parse(matcher.group(2));
    return Interval.of(start, stop);
  }

}
