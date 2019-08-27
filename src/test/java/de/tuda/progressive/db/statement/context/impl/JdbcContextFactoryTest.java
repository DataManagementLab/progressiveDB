package de.tuda.progressive.db.statement.context.impl;

import de.tuda.progressive.db.buffer.impl.JdbcDataBuffer;
import de.tuda.progressive.db.buffer.impl.JdbcSelectDataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.driver.impl.SQLiteDriver;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlParserImpl;
import de.tuda.progressive.db.sql.parser.SqlSelectProgressive;
import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.statement.context.impl.jdbc.JdbcContextFactory;
import de.tuda.progressive.db.statement.context.impl.jdbc.JdbcSelectContext;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("unchecked")
class JdbcContextFactoryTest {

  private static final DbDriver driver = SQLiteDriver.INSTANCE;

  private static Connection sourceConnection;

  private static SqlParser.Config config;

  private static JdbcContextFactory contextFactory;

  private Connection bufferConnection;

  @BeforeAll
  static void beforeAll() throws SQLException {
    sourceConnection = DriverManager.getConnection("jdbc:sqlite::memory:");
    contextFactory = new JdbcContextFactory(SQLiteDriver.INSTANCE, SQLiteDriver.INSTANCE);
    config = SqlParser.configBuilder().setParserFactory(SqlParserImpl.FACTORY).build();

    try (Statement statement = sourceConnection.createStatement()) {
      statement.execute(driver.toSql(SqlUtils.dropTable("t")));
      statement.execute("create table t (a integer, b integer, c varchar(100))");
      statement.execute("create table t_partition_0 (a integer, b integer, c varchar(100))");
      statement.execute("create table t_partition_1 (a integer, b integer, c varchar(100))");
      statement.execute("insert into t_partition_0 values (1, 2, 'a')");
      statement.execute("insert into t_partition_0 values (3, 4, 'b')");
      statement.execute("insert into t_partition_1 values (5, 6, 'a')");
      statement.execute("insert into t_partition_1 values (7, 8, 'b')");
      statement.execute("insert into t_partition_1 values (9, 10, 'c')");
    }
  }

  @BeforeEach
  void beforeEach() throws SQLException {
    bufferConnection = DriverManager.getConnection("jdbc:sqlite::memory:");
  }

  @AfterEach
  void afterEach() {
    SqlUtils.closeSafe(bufferConnection);
  }

  @AfterAll
  static void afterAll() {
    SqlUtils.closeSafe(sourceConnection);
  }

  private void assertContextNotNull(JdbcSelectContext context) {
    assertNotNull(context);
    assertNotNull(context.getMetaFields());
    assertNotNull(context.getSelectSource());
    assertNotNull(context.getCreateBuffer());
    assertNotNull(context.getInsertBuffer());
    assertNotNull(context.getSelectSource());
  }

  private void testAggregation(String sql, List<List<Object[]>> expectedValues) throws Exception {
    final SqlSelectProgressive select =
        (SqlSelectProgressive) SqlParser.create(sql, config).parseQuery();
    final JdbcSelectContext context = contextFactory.create(sourceConnection, select, null);

    assertContextNotNull(context);

    try (Statement statement = bufferConnection.createStatement()) {
      statement.execute(driver.toSql(context.getCreateBuffer()));

      try (Statement sourceSelectStatement = sourceConnection.createStatement()) {
        final String updateBuffer =
            context.getUpdateBuffer() == null ? null : driver.toSql(context.getUpdateBuffer());

        try (PreparedStatement updateBufferStatement =
            updateBuffer == null ? null : bufferConnection.prepareStatement(updateBuffer)) {
          final String insertBuffer = driver.toSql(context.getInsertBuffer());

          try (PreparedStatement insertBufferStatement =
              bufferConnection.prepareStatement(insertBuffer)) {
            final String selectBuffer = driver.toSql(context.getSelectBuffer());

            try (PreparedStatement selectBufferStatement =
                bufferConnection.prepareStatement(selectBuffer)) {
              for (int i = 0; i < expectedValues.size(); i++) {
                try (ResultSet result =
                    sourceSelectStatement.executeQuery(
                        getSelectSource(context.getSelectSource(), i))) {
                  while (result.next()) {
                    final int columnCount = result.getMetaData().getColumnCount();

                    if (updateBufferStatement != null) {
                      for (int j = 1; j <= columnCount; j++) {
                        updateBufferStatement.setObject(j, result.getObject(j));
                      }
                    }

                    if (updateBufferStatement == null
                        || updateBufferStatement.executeUpdate() == 0) {
                      for (int j = 1; j <= columnCount; j++) {
                        insertBufferStatement.setObject(j, result.getObject(j));

                        if (select.getGroup() != null) {
                          insertBufferStatement.setObject(j + columnCount, result.getObject(j));
                        }
                      }

                      insertBufferStatement.execute();
                    }
                  }
                }

                final List<MetaField> metaFields = context.getMetaFields();
                for (int j = 0; j < metaFields.size(); j++) {
                  switch (metaFields.get(j)) {
                    case COUNT:
                    case SUM:
                      selectBufferStatement.setDouble(
                          j + 1, (double) (i + 1) / (double) expectedValues.size());
                      break;
                  }
                }

                final int partition = i;
                SqlUtils.setMetaFields(
                    selectBufferStatement,
                    context::getFunctionMetaFieldPos,
                    new HashMap<MetaField, Object>() {
                      {
                        put(MetaField.PARTITION, partition);
                        put(
                            MetaField.PROGRESS,
                            (double) (partition + 1) / (double) expectedValues.size());
                      }
                    });

                try (ResultSet result = selectBufferStatement.executeQuery()) {
                  for (Object[] expectedRow : expectedValues.get(i)) {
                    assertTrue(result.next());
                    assertEquals(expectedRow.length, result.getMetaData().getColumnCount());

                    for (int j = 0; j < metaFields.size(); j++) {
                      assertEquals(expectedRow[j], result.getObject(j + 1));
                    }
                  }

                  assertFalse(result.next());
                }
              }
            }
          }
        }
      }
    }
  }

  private String getSelectSource(SqlSelect selectSource, int partition) {
    final SqlSelect select = (SqlSelect) selectSource.clone(SqlParserPos.ZERO);
    final SqlIdentifier from = (SqlIdentifier) select.getFrom();
    select.setFrom(SqlUtils.getIdentifier(driver.getPartitionTable(from.getSimple(), partition)));

    return driver.toSql(select);
  }

  private Object[] valuesRow(Object... values) {
    return values;
  }

  private List<Object[]> singleValueRowPartition(Object value) {
    return valuesPartition(new Object[] {value});
  }

  private List<Object[]> valuesPartition(Object[]... valueRows) {
    return Arrays.asList(valueRows);
  }

  private List<List<Object[]>> singleValueRowsPartitions(Object... values) {
    return Arrays.stream(values).map(this::singleValueRowPartition).collect(Collectors.toList());
  }

  @Test
  void testAvg() throws Exception {
    final String sql = "select progressive avg(a) from t";

    testAggregation(sql, singleValueRowsPartitions(2.0, 5.0));
  }

  @Test
  void testCount() throws Exception {
    final String sql = "select progressive count(a) from t";

    testAggregation(sql, singleValueRowsPartitions(4.0, 5.0));
  }

  @Test
  void testSum() throws Exception {
    final String sql = "select progressive sum(a) from t";

    testAggregation(sql, singleValueRowsPartitions(8.0, 25.0));
  }

  @Test
  void testAvgWhere() throws Exception {
    final String sql = "select progressive avg(a) from t where c = 'a'";

    testAggregation(sql, singleValueRowsPartitions(1.0, 3.0));
  }

  @Test
  void testCountWhere() throws Exception {
    final String sql = "select progressive count(a) from t where c = 'a'";

    testAggregation(sql, singleValueRowsPartitions(2.0, 2.0));
  }

  @Test
  void testSumWhere() throws Exception {
    final String sql = "select progressive sum(a) from t where c = 'a'";

    testAggregation(sql, singleValueRowsPartitions(2.0, 6.0));
  }

  @Test
  void testPartition() throws Exception {
    final String sql = "select progressive count(a), progressive_partition() from t";

    testAggregation(
        sql, Arrays.asList(valuesPartition(valuesRow(4.0, 0)), valuesPartition(valuesRow(5.0, 1))));
  }

  @Test
  void testProgress() throws Exception {
    final String sql = "select progressive avg(a), progressive_progress() from t";

    testAggregation(
        sql,
        Arrays.asList(valuesPartition(valuesRow(2.0, 0.5)), valuesPartition(valuesRow(5.0, 1.0))));
  }

  @Test
  void testOrder() throws Exception {
    final String sql =
        "select progressive progressive_partition(), progressive_progress(), count(a) from t";

    testAggregation(
        sql,
        Arrays.asList(
            valuesPartition(valuesRow(0, 0.5, 4.0)), valuesPartition(valuesRow(1, 1.0, 5.0))));
  }

  @Test
  void testGroupByAvg() throws Exception {
    final String sql = "select progressive avg(a), c from t group by c";

    testAggregation(
        sql,
        Arrays.asList(
            valuesPartition(valuesRow(1.0, "a"), valuesRow(3.0, "b")),
            valuesPartition(valuesRow(3.0, "a"), valuesRow(5.0, "b"), valuesRow(9.0, "c"))));
  }

  @Test
  void testGroupByCount() throws Exception {
    final String sql = "select progressive count(a), c from t group by c";

    testAggregation(
        sql,
        Arrays.asList(
            valuesPartition(valuesRow(2.0, "a"), valuesRow(2.0, "b")),
            valuesPartition(valuesRow(2.0, "a"), valuesRow(2.0, "b"), valuesRow(1.0, "c"))));
  }

  @Test
  void testGroupBySum() throws Exception {
    final String sql = "select progressive sum(a), c from t group by c";

    testAggregation(
        sql,
        Arrays.asList(
            valuesPartition(valuesRow(2.0, "a"), valuesRow(6.0, "b")),
            valuesPartition(valuesRow(6.0, "a"), valuesRow(10.0, "b"), valuesRow(9.0, "c"))));
  }

  @Test
  void testColumnAlias() throws Exception {
    final String sql = "select progressive count(a) a from t where c = 'a'";

    testAggregation(sql, singleValueRowsPartitions(2.0, 2.0));
  }

  @Test
  void testFutureGroupByInvalid() throws Exception {
    final String createViewSql =
        "create progressive view pv as select count(a) cnt_a, c future from t group by c future";
    final SqlCreateProgressiveView view =
        (SqlCreateProgressiveView) SqlParser.create(createViewSql, config).parseStmt();
    final JdbcSelectContext viewContext = contextFactory.create(sourceConnection, view, null);

    try (JdbcDataBuffer viewDataBuffer =
        new JdbcDataBuffer(SQLiteDriver.INSTANCE, bufferConnection, viewContext)) {

      final String selectViewSql = "select progressive cnt_a, c from pv group by c";
      final SqlSelectProgressive select =
          (SqlSelectProgressive) SqlParser.create(selectViewSql, config).parseQuery();

      final JdbcSourceContext selectContext = contextFactory.create(viewDataBuffer, select, null);

      try (Statement statement = bufferConnection.createStatement()) {
        assertThrows(
            SQLException.class,
            () -> statement.execute(driver.toSql(selectContext.getSelectSource())));
      }
    }
  }

  @Test
  void testFutureGroupByDefault() throws Exception {
    testFuture(
        "create progressive view pv as select count(a), c future from t group by c future",
        "select progressive * from pv",
        valuesPartition(valuesRow(5.0)));
  }

  @Test
  void testFutureGroupByOne() throws Exception {
    testFuture(
        "create progressive view pv as select count(a), c future from t group by c future",
        "select progressive * from pv with future group by c",
        valuesPartition(valuesRow(2.0, "a"), valuesRow(2.0, "b"), valuesRow(1.0, "c")));
  }

  @Test
  void testFutureWhereDefault() throws Exception {
    testFuture(
        "create progressive view pv as select count(a) from t where (c = 'a') future",
        "select progressive * from pv",
        valuesPartition(valuesRow(5.0)));
  }

  private void testFuture(String viewSql, String selectSql, List<Object[]> expected)
      throws Exception {
    final SqlCreateProgressiveView view =
        (SqlCreateProgressiveView) SqlParser.create(viewSql, config).parseStmt();
    final JdbcSelectContext viewContext = contextFactory.create(sourceConnection, view, null);
    final JdbcDataBuffer viewDataBuffer =
        new JdbcDataBuffer(SQLiteDriver.INSTANCE, bufferConnection, viewContext);

    try (Statement statement = sourceConnection.createStatement()) {
      for (int i = 0; i < 2; i++) {
        try (ResultSet resultSet =
            statement.executeQuery(getSelectSource(viewContext.getSelectSource(), i))) {
          viewDataBuffer.add(resultSet);
        }
      }
    }

    final SqlSelectProgressive select =
        (SqlSelectProgressive) SqlParser.create(selectSql, config).parseQuery();
    final JdbcSourceContext selectContext = contextFactory.create(viewDataBuffer, select, null);
    final JdbcSelectDataBuffer selectDataBuffer =
        new JdbcSelectDataBuffer(
            SQLiteDriver.INSTANCE,
            bufferConnection,
            selectContext,
            selectContext.getSelectSource());

    List<Object[]> result = selectDataBuffer.get(2, 1.0);
    assertEquals(expected.size(), result.size());

    for (int i = 0; i < expected.size(); i++) {
      assertArrayEquals(expected.get(i), result.get(i));
    }
  }

  @Test
  void testFutureWhereOne() throws Exception {
    testFuture(
        "create progressive view pv as select count(a) from t where (c = 'a') future",
        "select progressive * from pv with future where c = 'a'",
        valuesPartition(valuesRow(2.0)));
  }

  @Test
  void testFutureWhereTwo() throws Exception {
    testFuture(
        "create progressive view pv as select count(a) from t where (c = 'a') future or (c = 'b') future",
        "select progressive * from pv with future where c = 'a', c = 'b'",
        valuesPartition(valuesRow(4.0)));
  }

  @Test
  void testFutureWhereTwoSecond() throws Exception {
    testFuture(
        "create progressive view pv as select count(a) from t where (c = 'a') future or (c = 'c') future",
        "select progressive * from pv with future where c = 'c'",
        valuesPartition(valuesRow(1.0)));
  }

  @Test
  void testFutureWhereMixedAndDefault() throws Exception {
    testFuture(
        "create progressive view pv as select count(a) from t where (c = 'a') future and b > 5",
        "select progressive * from pv",
        valuesPartition(valuesRow(3.0)));
  }

  @Test
  void testFutureWhereMixedAndOne() throws Exception {
    testFuture(
        "create progressive view pv as select count(a) from t where (c = 'a') future and b > 5",
        "select progressive * from pv with future where c = 'a'",
        valuesPartition(valuesRow(1.0)));
  }

  @Test
  void testFutureWhereMixedOrDefault() throws Exception {
    testFuture(
        "create progressive view pv as select count(a) from t where (c = 'a') future or c = 'b'",
        "select progressive * from pv",
        valuesPartition(valuesRow(2.0)));
  }

  @Test
  void testFutureWhereMixedOrOne() throws Exception {
    testFuture(
        "create progressive view pv as select count(a) from t where (c = 'a') future or c = 'b'",
        "select progressive * from pv with future where c = 'a'",
        valuesPartition(valuesRow(4.0)));
  }

  @Test
  void testFutureWhereGroupDefault() throws Exception {
    testFuture(
        "create progressive view pv as select count(a), c from t where (c = 'a') future group by c",
        "select progressive * from pv",
        valuesPartition(valuesRow(2.0, "a"), valuesRow(2.0, "b"), valuesRow(1.0, "c")));
  }

  @Test
  void testFutureWhereGroupOne() throws Exception {
    testFuture(
        "create progressive view pv as select count(a), c from t where (c = 'a') future group by c",
        "select progressive * from pv with future where c = 'a'",
        valuesPartition(valuesRow(2.0, "a")));
  }
}
