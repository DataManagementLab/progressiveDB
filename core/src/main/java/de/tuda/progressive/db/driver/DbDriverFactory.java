package de.tuda.progressive.db.driver;

import de.tuda.progressive.db.driver.impl.MySQLDriver;
import de.tuda.progressive.db.driver.impl.PostgreSQLDriver;
import de.tuda.progressive.db.driver.impl.SQLiteDriver;

public class DbDriverFactory {

  private static final int PREFIX_LEN = "jdbc:".length();

  public static DbDriver create(String url) {
    final String driver = url.substring(PREFIX_LEN, url.indexOf(":", PREFIX_LEN));
    switch (driver.toUpperCase()) {
      case "POSTGRESQL":
        return new PostgreSQLDriver.Builder().build();
      case "SQLITE":
        return new SQLiteDriver.Builder().build();
      case "MYSQL":
        return new MySQLDriver.Builder().build();
    }

    throw new IllegalArgumentException("driver not supported: " + driver);
  }
}
