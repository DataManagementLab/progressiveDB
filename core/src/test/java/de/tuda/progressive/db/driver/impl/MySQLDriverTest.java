package de.tuda.progressive.db.driver.impl;

import de.tuda.progressive.db.driver.PartitionDriver;
import de.tuda.progressive.db.driver.PartitionDriverTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.MySQLContainer;

import java.sql.DriverManager;

class MySQLDriverTest extends PartitionDriverTest {

  private static MySQLContainer mySQLContainer = new MySQLContainer("mysql:8")
          .withDatabaseName("progressive")
          .withUsername("root")
          .withPassword("");

  @BeforeAll
  static void beforeAll() throws Exception {
    mySQLContainer.start();

    // MySQL needs some time to start in container
    Thread.sleep(1000);

    connection = DriverManager.getConnection(
            mySQLContainer.getJdbcUrl(),
            mySQLContainer.getUsername(),
            mySQLContainer.getPassword()
    );
  }

  @AfterAll
  static void afterAll() throws Exception {
    connection.close();
    mySQLContainer.stop();
  }

  @Override
  protected PartitionDriver.Builder getDriver() {
    return new MySQLDriver.Builder().hasPartitions(false);
  }
}
