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
import titan.ccp.stats.api.util.Interval;
import titan.ccp.stats.api.util.IntervalSerializer;

public class IntervalSerializerTest {
  private Gson gson;

  @Before
  public void init() {
    this.gson = new GsonBuilder()
        .registerTypeAdapter(Instant.class, new InstantSerializer())
        .registerTypeAdapter(Interval.class, new IntervalSerializer())
        .create();
  }

  @Test
  public void testIntervalSerializer() {
    final String isoStart = "2019-10-17T13:47:16.439Z";
    final String isoEnd = "2019-10-17T14:47:16.439Z";

    final TemporalAccessor startAccessor = DateTimeFormatter.ISO_DATE_TIME.parse(isoStart);
    final TemporalAccessor endAccessor = DateTimeFormatter.ISO_DATE_TIME.parse(isoEnd);

    final Instant start = Instant.from(startAccessor);
    final Instant end = Instant.from(endAccessor);

    final Interval interval = Interval.of(start, end);

    final String serializedInterval = this.gson.toJson(interval);

    final String expectedStart = "\"" + isoStart + "\"";
    final String expectedEnd = "\"" + isoEnd + "\"";

    final String expectedInterval =
        "{\"intervalStart\":" + expectedStart + ",\"intervalEnd\":" + expectedEnd + "}";

    assertEquals(serializedInterval, expectedInterval);
  }

}
