package titan.ccp.stats.streamprocessing;

import com.google.common.math.Stats;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import titan.ccp.common.kafka.GenericSerde;
import titan.ccp.common.kafka.avro.SchemaRegistryAvroSerdeFactory;
import titan.ccp.common.kafka.simpleserdes.SimpleSerdes;
import titan.ccp.model.records.ActivePowerRecord;
import titan.ccp.model.records.AggregatedActivePowerRecord;

class Serdes {

  private final SchemaRegistryAvroSerdeFactory avroSerdeFactory;

  public Serdes(final String schemaRegistryUrl) {
    this.avroSerdeFactory = new SchemaRegistryAvroSerdeFactory(schemaRegistryUrl);
  }

  public Serde<String> string() {
    return org.apache.kafka.common.serialization.Serdes.String();
  }

  public Serde<ActivePowerRecord> activePowerRecordValues() {
    return this.avroSerdeFactory.forValues();
  }

  public Serde<AggregatedActivePowerRecord> aggregatedActivePowerRecordValues() {
    return this.avroSerdeFactory.forValues();
  }

  public <T extends SpecificRecord> Serde<T> avroValues() {
    return this.avroSerdeFactory.forValues();
  }

  public Serde<Stats> stats() {
    return GenericSerde.from(Stats::toByteArray, Stats::fromByteArray);
  }

  public Serde<SummaryStatistics> summaryStatistics() {
    return SimpleSerdes.create(new SummaryStatisticsSerde());
  }


}
