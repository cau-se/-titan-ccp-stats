package titan.ccp.stats.streamprocessing;

import java.time.LocalDateTime;

public interface StatsKeyFactory<T> {

  T createKey(String sensorId, LocalDateTime dateTime);

  String getSensorId(T key);

}
