package titan.ccp.stats.api;

import static org.junit.Assert.assertEquals;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import org.junit.Before;
import org.junit.Test;
import titan.ccp.stats.api.util.InstantSerializer;

public class InstantSerializerTest {

  private Gson gson;

  @Before
  public void init() {
    this.gson = new GsonBuilder()
        .registerTypeAdapter(Instant.class, new InstantSerializer())
        .create();
  }

  @Test
  public void testInstantSerializer() {
    final String iso = "2019-10-17T13:47:16.439Z";

    final TemporalAccessor accessor = DateTimeFormatter.ISO_DATE_TIME.parse(iso);

    final Instant instant = Instant.from(accessor);

    final String serializedInstant = this.gson.toJson(instant);

    final String expectedSerializedInstant = "\"" + iso + "\"";

    assertEquals(expectedSerializedInstant, serializedInstant);
  }

}
