package titan.ccp.stats.streamprocessing;

import com.google.common.math.Stats;
import com.google.common.math.StatsAccumulator;
import titan.ccp.model.records.ActivePowerRecord;

/**
 * Class representing summary statistics associated with a timestamp indicating its creation.
 */
public class SummaryStatistics { // TODO maybe rename class

  private final Stats stats;

  private final long timestamp;

  public SummaryStatistics() {
    this.stats = Stats.of();
    this.timestamp = -1;
  }

  public SummaryStatistics(final Stats stats, final long timestamp) {
    this.stats = stats;
    this.timestamp = timestamp;
  }

  public Stats getStats() {
    return this.stats;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  /**
   * Add an {@link ActivePowerRecord} to these {@link SummaryStatistics} by creating a new instance.
   */
  public SummaryStatistics add(final ActivePowerRecord record) {
    final StatsAccumulator statsAccumulator = new StatsAccumulator();
    statsAccumulator.addAll(this.stats);
    statsAccumulator.add(record.getValueInW());
    final Stats stats = statsAccumulator.snapshot();
    final long timestamp = record.getTimestamp();
    return new SummaryStatistics(stats, timestamp);
  }

}
