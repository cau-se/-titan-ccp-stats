package titan.ccp.stats.api;

import com.datastax.driver.core.Session;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Service;
import titan.ccp.model.records.DayOfWeekActivePowerRecord;
import titan.ccp.model.records.HourOfDayActivePowerRecord;
import titan.ccp.model.records.HourOfWeekActivePowerRecord;
import titan.ccp.stats.api.util.InstantSerializer;
import titan.ccp.stats.api.util.Interval;
import titan.ccp.stats.api.util.IntervalSerializer;

/**
 * Contains a web server for accessing the stats via a REST interface.
 */
public class RestApiServer {
	// TODO make a builder that returns this server

	private static final Logger LOGGER = LoggerFactory.getLogger(RestApiServer.class);

	private final Gson gson = new GsonBuilder().registerTypeAdapter(Interval.class, new IntervalSerializer())
			.registerTypeAdapter(Instant.class, new InstantSerializer()).create();
	private final StatsRepository<DayOfWeekActivePowerRecord> dayOfWeekRepository;
	private final StatsRepository<HourOfDayActivePowerRecord> hourOfDayRepository;
	private final StatsRepository<HourOfWeekActivePowerRecord> hourOfWeekRepository;
	private final Service webService;
	private final boolean enableCors; // NOPMD

	/**
	 * Creates a new API server using the passed parameters.
	 */
	public RestApiServer(final Session cassandraSession, final int port, final boolean enableCors) {
		this.dayOfWeekRepository = new StatsRepository<>(cassandraSession, DayOfWeekMapping.create());
		this.hourOfDayRepository = new StatsRepository<>(cassandraSession, HourOfDayMapping.create());
		this.hourOfWeekRepository = new StatsRepository<>(cassandraSession, HourOfWeekMapping.create());
		LOGGER.info("Instantiate API server.");
		this.webService = Service.ignite().port(port);
		this.enableCors = enableCors;
	}

	/**
	 * Start the web server by setting up the API routes.
	 */
	public void start() { // NPMD declaration of routes
		if (this.enableCors) {
			this.enableCors();
		}

		this.instantiateRoutes();
	}

	private void enableCors() {
		this.webService.options("/*", (request, response) -> {

			final String accessControlRequestHeaders = request.headers("Access-Control-Request-Headers");
			if (accessControlRequestHeaders != null) {
				response.header("Access-Control-Allow-Headers", accessControlRequestHeaders);
			}

			final String accessControlRequestMethod = request.headers("Access-Control-Request-Method");
			if (accessControlRequestMethod != null) {
				response.header("Access-Control-Allow-Methods", accessControlRequestMethod);
			}

			return "OK";
		});

		this.webService.before((request, response) -> {
			response.header("Access-Control-Allow-Origin", "*");
		});
	}

	private void instantiateRoutes() {
		LOGGER.info("Instantiate API routes.");

		this.webService.get("/sensor/:sensorId/day-of-week", (request, response) -> {
			final String sensorId = request.params("sensorId"); // NOCS
			final String intervalStartParam = request.queryParams("intervalStart"); // NOCS
			final String intervalEndParam = request.queryParams("intervalEnd"); // NOCS
			if (intervalStartParam == null || intervalEndParam == null) {
				return this.dayOfWeekRepository.get(sensorId);
			} else {
				final Interval interval = Interval.of(Instant.parse(intervalStartParam),
						Instant.parse(intervalEndParam));
				return this.dayOfWeekRepository.get(sensorId, interval);
			}
		}, this.gson::toJson);

		this.webService.get("/sensor/:sensorId/hour-of-day", (request, response) -> {
			final String sensorId = request.params("sensorId"); // NOCS
			final String intervalStartParam = request.queryParams("intervalStart"); // NOCS
			final String intervalEndParam = request.queryParams("intervalEnd"); // NOCS
			if (intervalStartParam == null || intervalEndParam == null) {
				return this.hourOfDayRepository.get(sensorId);
			} else {
				final Interval interval = Interval.of(Instant.parse(intervalStartParam),
						Instant.parse(intervalEndParam));
				return this.hourOfDayRepository.get(sensorId, interval);
			}
		}, this.gson::toJson);

		this.webService.get("/sensor/:sensorId/hour-of-week", (request, response) -> {
			final String sensorId = request.params("sensorId"); // NOCS
			final String intervalStartParam = request.queryParams("intervalStart"); // NOCS
			final String intervalEndParam = request.queryParams("intervalEnd"); // NOCS
			if (intervalStartParam == null || intervalEndParam == null) {
				return this.hourOfWeekRepository.get(sensorId);
			} else {
				final Interval interval = Interval.of(Instant.parse(intervalStartParam),
						Instant.parse(intervalEndParam));
				return this.hourOfWeekRepository.get(sensorId, interval);
			}
		}, this.gson::toJson);

		this.webService.get("/interval/day-of-week", (request, response) -> {
			return this.dayOfWeekRepository.getIntervals();
		}, this.gson::toJson);

		this.webService.get("/interval/hour-of-day", (request, response) -> {
			return this.hourOfDayRepository.getIntervals();
		}, this.gson::toJson);

		this.webService.get("/interval/hour-of-week", (request, response) -> {
			return this.hourOfWeekRepository.getIntervals();
		}, this.gson::toJson);

		this.webService.after((request, response) -> {
			response.type("application/json");
		});
	}

}
