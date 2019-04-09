package titan.ccp.stats.streamprocessing;

import java.time.LocalDateTime;

public interface StatsKeyFactory<T> {

  T create(String sensorId, LocalDateTime dateTime);

}
