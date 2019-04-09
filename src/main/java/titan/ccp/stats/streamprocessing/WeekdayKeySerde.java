package titan.ccp.stats.streamprocessing;

import java.time.DayOfWeek;
import org.apache.kafka.common.serialization.Serde;
import titan.ccp.common.kafka.simpleserdes.BufferSerde;
import titan.ccp.common.kafka.simpleserdes.ReadBuffer;
import titan.ccp.common.kafka.simpleserdes.SimpleSerdes;
import titan.ccp.common.kafka.simpleserdes.WriteBuffer;

public class WeekdayKeySerde implements BufferSerde<WeekdayKey> {

  @Override
  public void serialize(final WriteBuffer buffer, final WeekdayKey data) {
    buffer.putInt(data.getDayOfWeek().getValue());
    buffer.putString(data.getSensorId());
  }

  @Override
  public WeekdayKey deserialize(final ReadBuffer buffer) {
    final DayOfWeek dayOfWeek = DayOfWeek.of(buffer.getInt());
    final String sensorId = buffer.getString();
    return new WeekdayKey(dayOfWeek, sensorId);
  }

  public static Serde<WeekdayKey> serde() {
    return SimpleSerdes.create(new WeekdayKeySerde());
  }

}
