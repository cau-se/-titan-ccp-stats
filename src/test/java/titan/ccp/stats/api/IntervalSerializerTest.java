package titan.ccp.stats.api;

import static org.junit.Assert.assertEquals;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import org.junit.Test;
import titan.ccp.stats.api.util.InstantSerializer;
import titan.ccp.stats.api.util.Interval;
import titan.ccp.stats.api.util.IntervalSerializer;

public class IntervalSerializerTest {

  private final Gson gson = new GsonBuilder()
      .registerTypeAdapter(Instant.class, new InstantSerializer())
      .registerTypeAdapter(Interval.class, new IntervalSerializer())
      .create();

  @Test
  public void test() {
    final Instant start = Instant.ofEpochMilli(0L);
    final Instant end = Instant.ofEpochMilli(1L);

    final Interval interval = Interval.of(start, end);

    final String serializedInterval = this.gson.toJson(interval);

    final JsonElement expectedStart =
        new JsonPrimitive(DateTimeFormatter.ISO_INSTANT.format(start));
    final JsonElement expectedEnd =
        new JsonPrimitive(DateTimeFormatter.ISO_INSTANT.format(end));
    final JsonObject expectedInterval = new JsonObject();
    expectedInterval.add("intervalStart", expectedStart);
    expectedInterval.add("intervalEnd", expectedEnd);

    assertEquals(serializedInterval, expectedInterval.toString());
  }

}
