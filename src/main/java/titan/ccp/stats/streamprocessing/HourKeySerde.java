package titan.ccp.stats.streamprocessing;

import org.apache.kafka.common.serialization.Serde;
import titan.ccp.common.kafka.simpleserdes.BufferSerde;
import titan.ccp.common.kafka.simpleserdes.ReadBuffer;
import titan.ccp.common.kafka.simpleserdes.SimpleSerdes;
import titan.ccp.common.kafka.simpleserdes.WriteBuffer;

public class HourKeySerde implements BufferSerde<HourKey> {

  @Override
  public void serialize(final WriteBuffer buffer, final HourKey data) {
    buffer.putInt(data.getHour());
    buffer.putString(data.getSensorId());
  }

  @Override
  public HourKey deserialize(final ReadBuffer buffer) {
    final int hour = buffer.getInt();
    final String sensorId = buffer.getString();
    return new HourKey(hour, sensorId);
  }

  public static Serde<HourKey> serde() {
    return SimpleSerdes.create(new HourKeySerde());
  }

}
