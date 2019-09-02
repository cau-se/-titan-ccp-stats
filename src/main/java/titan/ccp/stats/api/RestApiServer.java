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
import titan.ccp.stats.api.util.Interval;

/**
 * Contains a web server for accessing the stats via a REST interface.
 */
public class RestApiServer {
	// TODO make a builder that returns this server

	private static final Logger LOGGER = LoggerFactory.getLogger(RestApiServer.class);

	private final Gson gson = new GsonBuilder().create();
	private final StatsRepository<DayOfWeekActivePowerRecord> dayOfWeekRepository;
	private final StatsRepository<HourOfDayActivePowerRecord> hourOfDayRepository;
	private final Service webService;
	private final boolean enableCors; // NOPMD

	/**
	 * Creates a new API server using the passed parameters.
	 */
	public RestApiServer(final Session cassandraSession, final int port, final boolean enableCors) {
		this.dayOfWeekRepository = new StatsRepository<>(cassandraSession, DayOfWeekMapping.create());
		this.hourOfDayRepository = new StatsRepository<>(cassandraSession, HourOfDayMapping.create());
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
			return this.dayOfWeekRepository.get(sensorId, null);
		}, this.gson::toJson);

		this.webService.get("/sensor/:sensorId/hour-of-day", (request, response) -> {
			final String sensorId = request.params("sensorId"); // NOCS

			try {
				final Instant start = Instant.ofEpochMilli(Long.parseLong(request.queryParamOrDefault("start", null)));
				final Instant end = Instant.ofEpochMilli(Long.parseLong(request.queryParamOrDefault("end", null)));
				return this.hourOfDayRepository.get(sensorId, Interval.of(start, end));
			} catch (final NumberFormatException e) {
				return this.hourOfDayRepository.get(sensorId);
			}
		}, this.gson::toJson);

		this.webService.get("/interval/hour-of-day", (request, response) -> {
			return this.hourOfDayRepository.getIntervals();
		}, this.gson::toJson);

		this.webService.get("/interval/day-of-week", (request, response) -> {
			return this.dayOfWeekRepository.getIntervals();
		}, this.gson::toJson);

		this.webService.after((request, response) -> {
			response.type("application/json");
		});
	}

}
