package titan.ccp.stats.api.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;

public class IntervalSerializer implements JsonSerializer<Interval> {

	@Override
	public JsonElement serialize(final Interval src, final Type typeOfSrc, final JsonSerializationContext context) {
		final JsonObject object = new JsonObject();
		final JsonElement intervalStart = context.serialize(src.getStart());
		final JsonElement intervalEnd = context.serialize(src.getEnd());
		object.addProperty("intervalStart", intervalStart.getAsString());
		object.addProperty("intervalEnd", intervalEnd.getAsString());
		return object;
	}

}
