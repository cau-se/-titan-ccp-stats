package titan.ccp.stats.streamprocessing;

import com.google.common.math.Stats;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.kstream.Windowed;

@FunctionalInterface
public interface StatsRecordFactory<K, R extends SpecificRecord> {

  R create(Windowed<K> windowed, Stats stats);

}
