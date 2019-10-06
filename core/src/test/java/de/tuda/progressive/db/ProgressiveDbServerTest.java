package de.tuda.progressive.db;

import de.tuda.progressive.db.util.SqlUtils;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

class ProgressiveDbServerTest {

  private static PostgreSQLContainer postgreSQLContainer = new PostgreSQLContainer("postgres:11")
          .withDatabaseName("progressive")
          .withUsername("postgres")
          .withPassword("postgres");

  private static ProgressiveDbServer server;

  private Connection connection;

  @BeforeAll
  static void beforeAll() throws Exception {
    postgreSQLContainer.start();

    // PostgreSQL needs some time to start in container
    Thread.sleep(1000);

    try (Connection connection = DriverManager.getConnection(
            postgreSQLContainer.getJdbcUrl(),
            postgreSQLContainer.getUsername(),
            postgreSQLContainer.getPassword()
    )) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE IF EXISTS t");
        statement.execute("CREATE TABLE t (a INTEGER, b INTEGER, c VARCHAR(100), d VARCHAR(100))");
        statement.execute("INSERT INTO t VALUES (1, 2, 'a', 'a')");
        statement.execute("INSERT INTO t VALUES (3, 4, 'b', 'a')");
        statement.execute("INSERT INTO t VALUES (5, 6, 'a', 'b')");
        statement.execute("INSERT INTO t VALUES (7, 8, 'b', 'b')");
      }
    }

    final String metaFileName = System.getProperty("user.home") + "/progressivedb-test.sqlite";
    final File metaFile = new File(metaFileName);
    if (metaFile.exists() && !metaFile.delete()) {
      throw new IllegalStateException("could not delete meta db");
    }

    server = new ProgressiveDbServer.Builder()
        .source(postgreSQLContainer.getJdbcUrl(), postgreSQLContainer.getUsername(), postgreSQLContainer.getPassword())
        .tmp("jdbc:sqlite::memory:")
        .meta("jdbc:sqlite:" + metaFile)
        .port(9001)
        .chunkSize(2)
        .build();

    server.start();

    try (Connection connection = DriverManager
        .getConnection("jdbc:avatica:remote:url=http://localhost:9001")) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("PREPARE TABLE t");
      }
    }
  }

  @BeforeEach
  private void beforeEach() throws Exception {
    connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:9001");
  }

  @AfterEach
  private void afterEach() throws Exception {
    SqlUtils.closeSafe(connection);
  }

  @AfterAll
  static void afterAll() {
    server.stop();
    postgreSQLContainer.stop();
  }

  private void test(String sql, Object[][][] expected) throws Exception {
    try (Statement statement = connection.createStatement()) {
      try (ResultSet result = statement.executeQuery(sql)) {
        for (int i = 0; i < expected.length; i++) {
          final Object[][] expectedPartition = expected[i];

          for (int j = 0; j < expectedPartition.length; j++) {
            final Object[] expectedRow = expectedPartition[j];
            Assertions.assertTrue(result.next());

            for (int k = 0; k < expectedRow.length; k++) {
              Assertions.assertEquals(expectedRow[k], result.getObject(k + 1));
            }
          }
        }
        Assertions.assertFalse(result.next());
      }
    }
  }

  @Test
  void testAvg() throws Exception {
    test("SELECT PROGRESSIVE AVG(a) FROM t", new Object[][][]{
        {{5.0}},
        {{4.0}}
    });
  }

  @Test
  void testSum() throws Exception {
    test("SELECT PROGRESSIVE SUM(a) FROM t", new Object[][][]{
        {{20.0}},
        {{16.0}}
    });
  }

  @Test
  void testCount() throws Exception {
    test("SELECT PROGRESSIVE COUNT(a) FROM t", new Object[][][]{
        {{4.0}},
        {{4.0}}
    });
  }

  @Test
  void testFuncPartition() throws Exception {
    test("SELECT PROGRESSIVE AVG(a), PROGRESSIVE_PARTITION() FROM t", new Object[][][]{
        {{5.0, 1}},
        {{4.0, 2}}
    });
  }

  @Test
  void testFuncProgress() throws Exception {
    test("SELECT PROGRESSIVE AVG(a), PROGRESSIVE_PROGRESS() FROM t", new Object[][][]{
        {{5.0, 0.5}},
        {{4.0, 1.0}}
    });
  }

  @Test
  void testFuncConfidence() throws Exception {
    test("SELECT PROGRESSIVE AVG(a), PROGRESSIVE_CONFIDENCE(a) FROM t", new Object[][][]{
        {{5.0, 5.761936747919524}},
        {{4.0, 4.074304547221858}}
    });
  }

  @Test
  void testGroupBy() throws Exception {
    test("SELECT PROGRESSIVE AVG(a), c FROM t GROUP BY c", new Object[][][]{{
        {5.0, "b"},
    }, {
        {3.0, "a"},
        {5.0, "b"},
    }});
  }

  @Test
  void testOrderByAsc() throws Exception {
    test("SELECT PROGRESSIVE AVG(a), c FROM t GROUP BY c ORDER BY c", new Object[][][]{{
        {5.0, "b"},
    }, {
        {3.0, "a"},
        {5.0, "b"},
    }});
  }

  @Test
  void testOrderByDesc() throws Exception {
    test("SELECT PROGRESSIVE AVG(a), c FROM t GROUP BY c ORDER BY c DESC", new Object[][][]{{
        {5.0, "b"},
    }, {
        {5.0, "b"},
        {3.0, "a"},
    }});
  }

  @Test
  void testHaving() throws Exception {
    Assertions.assertThrows(
        SQLException.class,
        () -> test("SELECT PROGRESSIVE AVG(a), d FROM t GROUP BY d HAVING AVG(a)",
            new Object[][][]{})
    );
  }
}