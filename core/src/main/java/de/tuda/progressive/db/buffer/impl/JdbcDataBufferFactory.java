package de.tuda.progressive.db.buffer.impl;

import de.tuda.progressive.db.buffer.DataBuffer;
import de.tuda.progressive.db.buffer.DataBufferFactory;
import de.tuda.progressive.db.buffer.SelectDataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.driver.DbDriverFactory;
import de.tuda.progressive.db.exception.ProgressiveException;
import de.tuda.progressive.db.statement.context.impl.JdbcSourceContext;
import de.tuda.progressive.db.statement.context.impl.jdbc.JdbcSelectContext;
import de.tuda.progressive.db.util.SqlUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class JdbcDataBufferFactory
    implements DataBufferFactory<JdbcSelectContext, JdbcSourceContext> {

  private final String url;

  private final Properties properties;

  private final DataSource dataSource;

  public JdbcDataBufferFactory(String url) {
    this(url, null);
  }

  public JdbcDataBufferFactory(String url, Properties properties) {
    this(url, properties, null);
  }

  public JdbcDataBufferFactory(DataSource dataSource) {
    this(null, null, dataSource);
  }

  private JdbcDataBufferFactory(String url, Properties properties, DataSource dataSource) {
    if (url == null && dataSource == null) {
      throw new IllegalArgumentException("set either url or dataSource");
    }

    this.url = url;
    this.properties = properties;
    this.dataSource = dataSource;
  }

  @Override
  public DataBuffer create(JdbcSelectContext context) {
    Connection connection = null;
    DbDriver driver;

    try {
      connection = getConnection();
      driver = getDriver(connection);

      return create(driver, connection, context);
    } catch (Throwable t) {
      SqlUtils.closeSafe(connection);

      throw t;
    }
  }

  @Override
  public SelectDataBuffer create(DataBuffer dataBuffer, JdbcSourceContext context) {
    final Connection connection = ((JdbcSelectDataBuffer) dataBuffer).getConnection();
    final DbDriver driver = getDriver(connection);
    return new JdbcSelectDataBuffer(driver, connection, context, context.getSelectSource());
  }

  private DataBuffer create(DbDriver driver, Connection connection, JdbcSelectContext context) {
    return new JdbcDataBuffer(driver, connection, context);
  }

  private Connection getConnection() {
    try {
      if (url != null) {
        return DriverManager.getConnection(url, properties);
      }
      if (dataSource != null) {
        return dataSource.getConnection();
      }
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }

    // should never happen
    throw new IllegalStateException("url and dataSource are null");
  }

  private DbDriver getDriver(Connection connection) {
    String connectionUrl = url;

    if (connectionUrl == null) {
      try {
        connectionUrl = connection.getMetaData().getURL();
      } catch (SQLException e) {
        throw new ProgressiveException(e);
      }
    }

    return DbDriverFactory.create(connectionUrl);
  }
}
