package titan.ccp.stats.api;

import static org.junit.Assert.assertEquals;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import org.junit.Test;
import titan.ccp.stats.api.util.InstantSerializer;

public class InstantSerializerTest {

  private final Gson gson = new GsonBuilder()
      .registerTypeAdapter(Instant.class, new InstantSerializer())
      .create();

  @Test
  public void test() {
    final Instant instant = Instant.ofEpochMilli(1L);

    final String serializedInstant = this.gson.toJson(instant);

    final String expectedSerializedInstant =
        new JsonPrimitive(DateTimeFormatter.ISO_INSTANT.format(instant)).toString();

    assertEquals(serializedInstant, expectedSerializedInstant);
  }

}
