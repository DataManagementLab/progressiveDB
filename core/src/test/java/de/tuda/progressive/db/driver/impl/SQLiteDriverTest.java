package de.tuda.progressive.db.driver.impl;

import de.tuda.progressive.db.driver.AbstractDriver;
import de.tuda.progressive.db.driver.AbstractDriverTest;
import de.tuda.progressive.db.exception.ProgressiveException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeAll;

import java.sql.DriverManager;
import java.sql.SQLException;

class SQLiteDriverTest extends AbstractDriverTest {
//  private static final String URL = "jdbc:sqlite::memory:";
  private static final String URL = "jdbc:sqlite:/tmp/progressivedb-test.sqlite";

  @BeforeAll
  static void init() throws SQLException {
    connection = DriverManager.getConnection(URL);

    try (Statement statement = connection.createStatement()) {
      statement.execute("PRAGMA foreign_keys = ON");
    }
  }

  @Override
  protected AbstractDriver.Builder getDriver() {
    return new SQLiteDriver.Builder();
  }
}
