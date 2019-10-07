package de.tuda.progressive.db.driver;

import de.tuda.progressive.db.driver.impl.MySQLDriver;
import de.tuda.progressive.db.driver.impl.PostgreSQLDriver;
import de.tuda.progressive.db.driver.impl.SQLiteDriver;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class DbDriverFactory {

  private static final int PREFIX_LEN = "jdbc:".length();

  private static final Map<String, Supplier<AbstractDriver.Builder>> drivers = new HashMap<>();

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
        final Supplier<AbstractDriver.Builder> supplier = drivers.get(driver.toUpperCase());
        if (supplier == null || (builder = supplier.get()) == null) {
          throw new IllegalArgumentException("driver not supported: " + driver);
        }
    }

    if (partitionSize >= 0) {
      builder.partitionSize(partitionSize);
    }

    return builder.build();
  }

  public static void register(String driver, Supplier<AbstractDriver.Builder> supplier) {
    drivers.put(driver.toUpperCase(), supplier);
  }
}
