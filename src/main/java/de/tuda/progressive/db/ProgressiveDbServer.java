package de.tuda.progressive.db;

import de.tuda.progressive.db.buffer.DataBufferFactory;
import de.tuda.progressive.db.buffer.impl.JdbcDataBufferFactory;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.driver.DbDriverFactory;
import de.tuda.progressive.db.meta.MetaData;
import de.tuda.progressive.db.meta.jdbc.JdbcMetaData;
import de.tuda.progressive.db.statement.context.impl.BaseContextFactory;
import de.tuda.progressive.db.statement.context.impl.jdbc.JdbcContextFactory;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ProgressiveDbServer {

	private static final Logger log = LoggerFactory.getLogger(ProgressiveDbServer.class);

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			log.error("pass path of configuration");
			return;
		}

		File configurationFile = new File(args[0]);
		if (!configurationFile.exists()) {
			log.error("configuration does not exist: {}", configurationFile);
			return;
		}

		Configurations configs = new Configurations();
		Configuration config = configs.properties(configurationFile);

		ProgressiveDbServer server = new Builder()
				.source(
						config.getString("source.url"),
						config.getString("source.user", null),
						config.getString("source.password", null)
				)
				.meta(
						config.getString("meta.url"),
						config.getString("meta.user", null),
						config.getString("meta.password", null)
				)
				.tmp(
						config.getString("tmp.url"),
						config.getString("tmp.user", null),
						config.getString("tmp.password", null)
				)
				.port(config.getInt("port", 9000))
				.build();

		server.start();
	}

	public static class Builder {

		private String sourceUrl;
		private Properties sourceProperties;

		private String metaUrl;
		private Properties metaProperties;

		private String tmpUrl;
		private Properties tmpProperties;

		private int port;

		public Builder source(String url) {
			source(url, null, null);
			return this;
		}

		public Builder source(String url, String user, String password) {
			source(url, createProperties(user, password));
			return this;
		}

		public Builder source(String url, Properties properties) {
			this.sourceUrl = url;
			this.sourceProperties = properties;
			return this;
		}

		public Builder meta(String url) {
			meta(url, null, null);
			return this;
		}

		public Builder meta(String url, String user, String password) {
			meta(url, createProperties(user, password));
			return this;
		}

		public Builder meta(String url, Properties properties) {
			this.metaUrl = url;
			this.metaProperties = properties;
			return this;
		}

		public Builder tmp(String url) {
			tmp(url, null, null);
			return this;
		}

		public Builder tmp(String url, String user, String password) {
			tmp(url, createProperties(user, password));
			return this;
		}

		public Builder tmp(String url, Properties properties) {
			this.tmpUrl = url;
			this.tmpProperties = properties;
			return this;
		}

		public Builder port(int port) {
			this.port = port;
			return this;
		}

		private Properties createProperties(String user, String password) {
			Properties properties = new Properties();
			if (user != null) {
				properties.setProperty("user", user);
			}
			if (password != null) {
				properties.setProperty("password", password);
			}
			return properties;
		}

		public ProgressiveDbServer build() {
			ProgressiveDbServer server = new ProgressiveDbServer();
			server.sourceUrl = sourceUrl;
			server.sourceProperties = sourceProperties;
			server.metaUrl = metaUrl;
			server.metaProperties = metaProperties;
			server.tmpUrl = tmpUrl;
			server.tmpProperties = tmpProperties;
			server.port = port;
			return server;
		}
	}

	private String sourceUrl;
	private Properties sourceProperties;

	private String metaUrl;
	private Properties metaProperties;

	private String tmpUrl;
	private Properties tmpProperties;

	private int port;

	private HttpServer server;

	private ProgressiveDbServer() {
	}

	public synchronized void start() throws SQLException {
		if (server == null) {
			log.info("starting");

			final DbDriver sourceDriver = DbDriverFactory.create(sourceUrl);
			final DbDriver bufferDriver = DbDriverFactory.create(tmpUrl);
			final BaseContextFactory contextFactory = createContextFactory(sourceDriver, bufferDriver);
			final DataBufferFactory dataBufferFactory = createDataBufferFactory(tmpUrl, tmpProperties);
			final MetaData metaData = new JdbcMetaData(metaUrl, metaProperties);

			final ProgressiveHandler progressiveHandler = new ProgressiveHandler(
					DbDriverFactory.create(sourceUrl),
					metaData,
					contextFactory,
					dataBufferFactory
			);

			Meta meta = new ProgressiveMeta(sourceUrl, sourceProperties, progressiveHandler);
			Service service = new PService(meta);

			server = new HttpServer.Builder()
					.withHandler(service, Driver.Serialization.JSON)
					.withPort(port)
					.build();
			server.start();

			Runtime.getRuntime().addShutdownHook(
					new Thread(this::stop)
			);

			new Thread(() -> {
				try {
					server.join();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}).start();
		}
	}

	private BaseContextFactory createContextFactory(DbDriver sourceDriver, DbDriver bufferDriver) {
		return new JdbcContextFactory(sourceDriver, bufferDriver);
	}

	private DataBufferFactory createDataBufferFactory(String url, Properties properties) {
		return new JdbcDataBufferFactory(url, properties);
	}

	public synchronized void stop() {
		if (server != null) {
			log.info("shutting down");
			server.stop();
			server = null;
		}
	}
}
