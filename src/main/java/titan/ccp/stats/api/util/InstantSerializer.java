package titan.ccp.stats.api.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Serializer class for serializing an {@link java.time.Instant Instant} with GSON.
 *
 * @see java.time.Instant Instant
 *
 */
public class InstantSerializer implements JsonSerializer<Instant> {

  @Override
  public JsonElement serialize(final Instant src, final Type typeOfSrc,
      final JsonSerializationContext context) {
    final String iso = DateTimeFormatter.ISO_INSTANT.format(src);
    return new JsonPrimitive(iso);
  }

}
