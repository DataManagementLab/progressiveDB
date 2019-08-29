package de.tuda.progressive.db.driver;

import de.tuda.progressive.db.driver.impl.MySQLDriver;
import de.tuda.progressive.db.driver.impl.PostgreSQLDriver;
import de.tuda.progressive.db.driver.impl.SQLiteDriver;

public class DbDriverFactory {

  private static final int PREFIX_LEN = "jdbc:".length();

  public static DbDriver create(String url) {
    return create(url, -1);
  }

  public static DbDriver create(String url, int partitionSize) {
    final String driver = url.substring(PREFIX_LEN, url.indexOf(":", PREFIX_LEN));
    AbstractDriver.Builder builder;

    switch (driver.toUpperCase()) {
      case "POSTGRESQL":
        builder = new PostgreSQLDriver.Builder();
        break;
      case "SQLITE":
        builder = new SQLiteDriver.Builder();
        break;
      case "MYSQL":
        builder = new MySQLDriver.Builder();
        break;
      default:
        throw new IllegalArgumentException("driver not supported: " + driver);
    }

    if (partitionSize >= 0) {
      builder.partitionSize(partitionSize);
    }

    return builder.build();
  }
}
