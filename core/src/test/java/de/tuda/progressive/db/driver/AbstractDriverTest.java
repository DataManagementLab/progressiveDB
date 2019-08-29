package de.tuda.progressive.db.driver;

import de.tuda.progressive.db.meta.MemoryMetaData;
import de.tuda.progressive.db.meta.MetaData;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public abstract class AbstractDriverTest<
    D extends AbstractDriver, B extends AbstractDriver.Builder<D, B>> {

  private static final String TABLE_NAME = "f";
  private static final String FOREIGN_TABLE_NAME = "d";
  private static final char JOIN_COLUMN_NAME = 'b';

  private static final int ENTRY_COUNT = 3;

  protected static Connection connection;

  @BeforeEach
  void beforeEach() throws SQLException {
    final AbstractDriver driver = getDriver().build();

    try (Statement statement = connection.createStatement()) {
      for (int i = 0; i < ENTRY_COUNT; i++) {
        SqlDropTable dropTable = SqlUtils.dropTable(driver.getPartitionTable(TABLE_NAME, i));
        statement.execute(driver.toSql(dropTable));
      }

      statement.execute(driver.toSql(SqlUtils.dropTable(TABLE_NAME)));
      statement.execute(
          String.format(
              "create table %s (a integer, %c varchar(100))", TABLE_NAME, JOIN_COLUMN_NAME));
      for (int i = 0; i < ENTRY_COUNT; i++) {
        statement.execute(
            String.format("insert into %s values (%d, '%c')", TABLE_NAME, i + 1, 'a' + i));
      }
    }
  }

  @AfterAll
  static void afterAll() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }

  protected abstract B getDriver();

  private void assertColumn(
      MetaData metaData, String tableName, String columnName, Column expected) {
    final Column column = metaData.getColumn(tableName, columnName);

    if (expected == null) {
      assertNull(column);
    } else {
      assertNotNull(column);
      assertEquals(expected.getMin(), column.getMin());
      assertEquals(expected.getMax(), column.getMax());
    }
  }

  private String createForeignTable(int num) {
    final DbDriver driver = getDriver().build();

    try (Statement statement = connection.createStatement()) {
      final String table = String.format("%s_%d", FOREIGN_TABLE_NAME, num);

      statement.execute(driver.toSql(SqlUtils.dropTable(table)));
      for (int i = 0; i < ENTRY_COUNT; i++) {
        statement.execute(driver.toSql(SqlUtils.dropTable(driver.getPartitionTable(table, i))));
      }

      final String sqlCreateTable =
          String.format(
              "create table %s (%s varchar(100), %c integer, primary key (%s))",
              table, JOIN_COLUMN_NAME, JOIN_COLUMN_NAME + 1, JOIN_COLUMN_NAME);
      statement.execute(sqlCreateTable);

      try (PreparedStatement insert =
          connection.prepareStatement(String.format("insert into %s values (?, ?)", table))) {
        final String sql = String.format("select %s from %s", JOIN_COLUMN_NAME, TABLE_NAME);
        try (ResultSet result = statement.executeQuery(sql)) {
          int counter = 1;
          while (result.next()) {
            insert.setString(1, result.getString(1));
            insert.setInt(2, counter++);
            assertEquals(1, insert.executeUpdate());
          }
        }
      }

      final String sqlAlterTable =
          String.format(
              "alter table %s add foreign key (%s) references %s(%s)",
              TABLE_NAME, JOIN_COLUMN_NAME, table, JOIN_COLUMN_NAME);
      statement.execute(sqlAlterTable);

      return table;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private List<String> createForeignTables(int count) {
    return IntStream.range(0, count)
        .mapToObj(this::createForeignTable)
        .collect(Collectors.toList());
  }

  void testPrepare(AbstractDriver.Builder<D, B> builder, int partitionSize, int joinTables) {
    final DbDriver driver = builder.partitionSize(partitionSize).build();
    final List<String> foreignTables = createForeignTables(joinTables);

    final MetaData metaData = new MemoryMetaData();
    driver.prepareTable(connection, TABLE_NAME, metaData);

    testPrepare(metaData, TABLE_NAME, "a", partitionSize);
    foreignTables.forEach(
        table ->
            testPrepare(
                metaData, table, String.valueOf((char) (JOIN_COLUMN_NAME + 1)), partitionSize));
  }

  private void testPrepare(MetaData metaData, String table, String column, int partitionSize) {
    final List<Partition> partitions = metaData.getPartitions(table);
    final int partitionCount = (int) Math.ceil((double) ENTRY_COUNT / (double) partitionSize);

    assertEquals(partitionCount, partitions.size());
    if (partitionSize > 1) {
      partitions.sort(Comparator.comparingInt(p -> (int) p.getEntries()));
      assertEquals(1, partitions.get(0).getEntries());
      assertEquals(2, partitions.get(1).getEntries());
    } else {
      partitions.forEach(p -> assertEquals(1, p.getEntries()));
    }

    assertColumn(metaData, table, column, new Column(table, column, 1, 3));
    assertColumn(metaData, table, String.valueOf(JOIN_COLUMN_NAME), null);
  }

  @Test
  void testGetCount() {
    final AbstractDriver driver = getDriver().build();

    assertEquals(3, driver.getCount(connection, TABLE_NAME));
  }

  private void testPrepare(int partitionSize, int joinTable) {
    testPrepare(getDriver(), partitionSize, joinTable);
  }

  @Test
  void testPrepare1() {
    testPrepare(1, 0);
  }

  @Test
  void testPrepare2() {
    testPrepare(2, 0);
  }

  @Test
  void testPrepare1Join1() {
    testPrepare(1, 1);
  }

  @Test
  void testPrepare2Join1() {
    testPrepare(2, 1);
  }

  @Test
  void testPrepare1Join2() {
    testPrepare(1, 2);
  }

  @Test
  void testPrepare2Join2() {
    testPrepare(2, 2);
  }
}
