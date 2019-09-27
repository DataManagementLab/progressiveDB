package de.tuda.progressive.db.buffer.impl;

import de.tuda.progressive.db.Utils;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.driver.impl.SQLiteDriver;
import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.statement.context.impl.jdbc.JdbcSelectContext;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JdbcDataBufferTest {

  private static final DbDriver driver = SQLiteDriver.INSTANCE;

  private static Connection connection;

  @BeforeAll
  static void beforeAll() throws SQLException {
    connection = DriverManager.getConnection("jdbc:sqlite::memory:");
  }

  @BeforeEach
  void beforeEach() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("drop table if exists t");
      statement.execute("drop table if exists b");
      statement.execute("create table t (a integer, b integer, c varchar(100), d varchar(100))");
      statement.execute("insert into t values (1, 2, 'a', 'a')");
      statement.execute("insert into t values (3, 4, 'b', 'b')");
      statement.execute("insert into t values (5, 6, 'a', 'a')");
      statement.execute("insert into t values (7, 8, 'b', 'b')");
    }
  }

  @AfterAll
  static void afterAll() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T parse(String sql) {
    try {
      return (T) SqlParser.create(sql).parseStmt();
    } catch (SqlParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private void test(JdbcSelectContext context, List<List<Object[]>> expected) throws SQLException {
    final JdbcDataBuffer buffer = new JdbcDataBuffer(driver, connection, context);

    try (Statement statement = connection.createStatement()) {
      try (ResultSet result = statement.executeQuery(driver.toSql(context.getSelectSource()))) {
        buffer.add(result);

        for (int i = 0; i < expected.size(); i++) {
          final List<Object[]> expectedPartition = expected.get(i);
          final List<Object[]> actualPartition =
              buffer.get(i, (double) (i + 1) / (double) expected.size());

          assertEquals(expectedPartition.size(), actualPartition.size());
          for (int j = 0; j < expectedPartition.size(); j++) {
            final Object[] expectedRow = expectedPartition.get(j);
            final Object[] actualRow = actualPartition.get(j);

            assertEquals(expectedRow.length, actualRow.length);

            for (int k = 0; k < expectedRow.length; k++) {
              if (expectedRow[k] instanceof Double) {
                assertEquals((double) expectedRow[k], (double) actualRow[k], 0.001);
              } else {
                assertEquals(expectedRow[k], actualRow[k]);
              }
            }
          }
        }
      }
    }
  }

  private JdbcSelectContext.Builder builder() {
    return new JdbcSelectContext.Builder()
        .selectSource(parse("select a, b, c, d from t"))
        .createBuffer(Utils.createTable("b", 4, 2))
        .insertBuffer(Utils.createUpsert("b", 4, 2));
  }

  @Test
  void testSum() throws Throwable {
    final JdbcSelectContext context =
        builder()
            .metaFields(Arrays.asList(MetaField.SUM, MetaField.SUM, MetaField.NONE, MetaField.NONE))
            .selectBuffer(parse("select cast(a as float) / ?, cast(b as float) / ?, c, d from b"))
            .build();

    test(
        context,
        Arrays.asList(
            Arrays.asList(new Object[] {6.0, 8.0, "a", "a"}, new Object[] {10.0, 12.0, "b", "b"})));
  }

  @Test
  void testCount() throws Throwable {
    final JdbcSelectContext context =
        builder()
            .metaFields(
                Arrays.asList(MetaField.COUNT, MetaField.COUNT, MetaField.NONE, MetaField.NONE))
            .selectBuffer(parse("select cast(a as float) / ?, cast(b as float) / ?, c, d from b"))
            .build();

    test(
        context,
        Arrays.asList(
            Arrays.asList(new Object[] {6.0, 8.0, "a", "a"}, new Object[] {10.0, 12.0, "b", "b"})));
  }

  @Test
  void testAvg() throws Throwable {
    final JdbcSelectContext context =
        builder()
            .metaFields(Arrays.asList(MetaField.AVG, MetaField.NONE, MetaField.NONE))
            .selectBuffer(parse("select cast(a as float) / cast(b as float), c, d from b"))
            .build();

    test(
        context,
        Arrays.asList(
            Arrays.asList(new Object[] {0.75, "a", "a"}, new Object[] {0.833, "b", "b"})));
  }

  @Test
  void testSumProgress() throws Throwable {
    final JdbcSelectContext context =
        builder()
            .metaFields(
                Arrays.asList(
                    MetaField.PROGRESS,
                    MetaField.SUM,
                    MetaField.SUM,
                    MetaField.NONE,
                    MetaField.NONE))
            .selectBuffer(
                parse("select ?, cast(a as float) / ?, cast(b as float) / ?, c, d from b"))
            .build();

    test(
        context,
        Arrays.asList(
            Arrays.asList(
                new Object[] {0.5, 12.0, 16.0, "a", "a"}, new Object[] {0.5, 20.0, 24.0, "b", "b"}),
            Arrays.asList(
                new Object[] {1.0, 6.0, 8.0, "a", "a"}, new Object[] {1.0, 10.0, 12.0, "b", "b"})));
  }

  @Test
  void testCountProgress() throws Throwable {
    final JdbcSelectContext context =
        builder()
            .metaFields(
                Arrays.asList(
                    MetaField.PROGRESS,
                    MetaField.COUNT,
                    MetaField.COUNT,
                    MetaField.NONE,
                    MetaField.NONE))
            .selectBuffer(
                parse("select ?, cast(a as float) / ?, cast(b as float) / ?, c, d from b"))
            .build();

    test(
        context,
        Arrays.asList(
            Arrays.asList(
                new Object[] {0.5, 12.0, 16.0, "a", "a"}, new Object[] {0.5, 20.0, 24.0, "b", "b"}),
            Arrays.asList(
                new Object[] {1.0, 6.0, 8.0, "a", "a"}, new Object[] {1.0, 10.0, 12.0, "b", "b"})));
  }

  @Test
  void testAvgProgress() throws Throwable {
    final JdbcSelectContext context =
        builder()
            .metaFields(
                Arrays.asList(MetaField.PROGRESS, MetaField.AVG, MetaField.NONE, MetaField.NONE))
            .selectBuffer(parse("select ?, cast(a as float) / cast(b as float), c, d from b"))
            .build();

    test(
        context,
        Arrays.asList(
            Arrays.asList(new Object[] {0.5, 0.75, "a", "a"}, new Object[] {0.5, 0.833, "b", "b"}),
            Arrays.asList(
                new Object[] {1.0, 0.75, "a", "a"}, new Object[] {1.0, 0.833, "b", "b"})));
  }

  @Test
  void testPartition() throws Throwable {
    final JdbcSelectContext context =
        builder()
            .metaFields(Arrays.asList(MetaField.PARTITION, MetaField.NONE, MetaField.NONE))
            .selectBuffer(parse("select ?, c, d from b"))
            .build();

    test(
        context,
        Arrays.asList(
            Arrays.asList(new Object[] {0, "a", "a"}, new Object[] {0, "b", "b"}),
            Arrays.asList(new Object[] {1, "a", "a"}, new Object[] {1, "b", "b"})));
  }

  @Test
  void testMultipleMeta() throws Throwable {
    final JdbcSelectContext context =
        builder()
            .metaFields(
                Arrays.asList(
                    MetaField.PARTITION,
                    MetaField.PARTITION,
                    MetaField.PROGRESS,
                    MetaField.PROGRESS,
                    MetaField.NONE))
            .selectBuffer(parse("select ?, ?, ?, ?, c from b"))
            .build();

    test(
        context,
        Arrays.asList(
            Arrays.asList(new Object[] {0, 0, 0.5, 0.5, "a"}, new Object[] {0, 0, 0.5, 0.5, "b"}),
            Arrays.asList(new Object[] {1, 1, 1.0, 1.0, "a"}, new Object[] {1, 1, 1.0, 1.0, "b"})));
  }
}
