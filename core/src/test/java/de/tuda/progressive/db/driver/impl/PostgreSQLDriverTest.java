package de.tuda.progressive.db.driver.impl;

import de.tuda.progressive.db.driver.PartitionDriver;
import de.tuda.progressive.db.driver.PartitionDriverTest;
import org.junit.jupiter.api.BeforeAll;

import java.sql.DriverManager;
import java.sql.SQLException;

class PostgreSQLDriverTest extends PartitionDriverTest {
  private static final String URL = "jdbc:postgresql://localhost:5432/progressive";
  private static final String USER = "postgres";
  private static final String PASSWORD = "postgres";

  @BeforeAll
  static void init() throws SQLException {
    connection = DriverManager.getConnection(URL, USER, PASSWORD);
  }

  @Override
  protected PartitionDriver.Builder getDriver() {
    return new PostgreSQLDriver.Builder().hasPartitions(false);
  }
}
