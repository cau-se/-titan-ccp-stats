package titan.ccp.stats.streamprocessing;

import com.google.common.math.Stats;
import titan.ccp.model.records.ActivePowerRecord;
import titan.ccp.stats.streamprocessing.util.StatsFactory;

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

  public SummaryStatistics add(final ActivePowerRecord record) {
    final Stats stats = StatsFactory.accumulate(this.stats, record.getValueInW());
    final long timestamp = record.getTimestamp();
    return new SummaryStatistics(stats, timestamp);
  }

}
