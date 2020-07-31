package titan.ccp.stats.streamprocessing;

import com.google.common.math.Stats;
import titan.ccp.common.kafka.simpleserdes.BufferSerde;
import titan.ccp.common.kafka.simpleserdes.ReadBuffer;
import titan.ccp.common.kafka.simpleserdes.WriteBuffer;

public class SummaryStatisticsSerde implements BufferSerde<SummaryStatistics> {

  @Override
  public void serialize(final WriteBuffer buffer, final SummaryStatistics data) {
    buffer.putBytes(data.getStats().toByteArray());
    buffer.putLong(data.getTimestamp());
  }

  @Override
  public SummaryStatistics deserialize(final ReadBuffer buffer) {
    final Stats stats = Stats.fromByteArray(buffer.getBytes());
    final long timestamp = buffer.getLong();
    return new SummaryStatistics(stats, timestamp);
  }

}
