package de.tuda.progressive.db.driver.impl;

import de.tuda.progressive.db.driver.PartitionDriver;
import de.tuda.progressive.db.driver.PartitionDriverTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.DriverManager;

class PostgreSQLDriverTest extends PartitionDriverTest {

  private static PostgreSQLContainer postgreSQLContainer = new PostgreSQLContainer("postgres:11")
          .withDatabaseName("progressive")
          .withUsername("postgres")
          .withPassword("postgres");

  @BeforeAll
  static void beforeAll() throws Exception {
    postgreSQLContainer.start();

    // PostgreSQL needs some time to start in container
    Thread.sleep(1000);

    connection = DriverManager.getConnection(
            postgreSQLContainer.getJdbcUrl(),
            postgreSQLContainer.getUsername(),
            postgreSQLContainer.getPassword()
    );
  }

  @AfterAll
  static void afterAll() throws Exception {
    connection.close();
    postgreSQLContainer.stop();
  }

  @Override
  protected PartitionDriver.Builder getDriver() {
    return new PostgreSQLDriver.Builder().hasPartitions(false);
  }
}
