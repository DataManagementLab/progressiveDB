package de.tuda.progressive.db.statement.context.impl;

import de.tuda.progressive.db.buffer.impl.JdbcDataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.driver.impl.SQLiteDriver;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlFutureNode;
import de.tuda.progressive.db.sql.parser.SqlParserImpl;
import de.tuda.progressive.db.sql.parser.SqlSelectProgressive;
import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BaseContextFactoryTest {

  private static Connection connection;

  private static Factory factory;

  private static SqlParser.Config config =
      SqlParser.configBuilder().setParserFactory(SqlParserImpl.FACTORY).build();

  @BeforeAll
  static void beforeAll() throws SQLException {
    connection = DriverManager.getConnection("jdbc:sqlite::memory:");
    factory = new Factory(SQLiteDriver.INSTANCE);
    config =
        SqlParser.configBuilder()
            .setParserFactory(SqlParserImpl.FACTORY)
            .setUnquotedCasing(Casing.UNCHANGED)
            .build();
  }

  @AfterAll
  static void afterAll() {
    SqlUtils.closeSafe(connection);
  }

  private void test(String futureWhere, String sourceWhere, List<String> columns) throws Exception {
    test(futureWhere, sourceWhere, columns, false);
  }

  private void test(String futureWhere, String sourceWhere) throws Exception {
    test(futureWhere, sourceWhere, null, false);
  }

  private void test(
      String futureWhere, String sourceWhere, List<String> columns, boolean aggregation)
      throws Exception {
    final SqlCreateProgressiveView view =
        (SqlCreateProgressiveView)
            SqlParser.create(
                    String.format(
                        "create progressive view pv as select %s from v where %s",
                        aggregation ? "sum(z)" : "z", futureWhere),
                    config)
                .parseStmt();

    final JdbcSourceContext context = factory.create(connection, view, null);

    final SqlIdentifier zColumn = SqlUtils.getIdentifier("z");
    final SqlNodeList selectList =
        SqlNodeList.of(aggregation ? SqlUtils.createSumAggregation(zColumn) : zColumn);
    final List<SqlNode> futureColumns =
        columns == null
            ? getFutureColumns(((SqlSelect) view.getQuery()).getWhere())
            : getFutureColumns(columns);
    futureColumns.forEach(selectList::add);

    final SqlSelect select =
        new SqlSelect(
            SqlParserPos.ZERO,
            null,
            selectList,
            SqlUtils.getIdentifier("v"),
            getWhere(sourceWhere),
            getGroupBy(aggregation, futureColumns.size()),
            null,
            null,
            null,
            null,
            null);

    assertTrue(select.equalsDeep(context.getSelectSource(), Litmus.THROW));
  }

  private List<SqlNode> getFutureColumns(List<String> selectList) throws Exception {
    final String sql = String.format("select %s from t", String.join(", ", selectList));
    final SqlSelect select = (SqlSelect) SqlParser.create(sql, config).parseQuery();
    final List<SqlNode> columns = new ArrayList<>();
    for (int i = 0; i < select.getSelectList().size(); i++) {
      columns.add(createField(i + 1, select.getSelectList().get(i)));
    }
    return columns;
  }

  private List<SqlNode> getFutureColumns(SqlNode where) {
    final List<SqlNode> futures = getFutures(where);
    final List<SqlNode> columns = new ArrayList<>(futures.size());
    for (int i = 0; i < futures.size(); i++) {
      columns.add(createField(i + 1, futures.get(i)));
    }
    return columns;
  }

  private SqlNode getWhere(String sourceWhere) throws Exception {
    if (sourceWhere == null) {
      return null;
    }
    final SqlSelect select =
        (SqlSelect) SqlParser.create("select * from t where " + sourceWhere, config).parseQuery();
    return select.getWhere();
  }

  private SqlNodeList getGroupBy(boolean aggregation, int futures) {
    if (!aggregation) {
      return null;
    }
    final SqlNodeList groupBy = new SqlNodeList(SqlParserPos.ZERO);
    IntStream.range(1, futures + 1)
        .mapToObj(BaseContextFactoryTest::getFutureColumnName)
        .forEach(groupBy::add);
    return groupBy;
  }

  private List<SqlNode> getFutures(SqlNode node) {
    final List<SqlNode> futures = new ArrayList<>();
    if (node instanceof SqlFutureNode) {
      futures.add(((SqlFutureNode) node).getNode());
    } else if (node instanceof SqlBasicCall) {
      final SqlBasicCall call = (SqlBasicCall) node;
      futures.addAll(getFutures(call.getOperands()[0]));
      futures.addAll(getFutures(call.getOperands()[1]));
    }
    return futures;
  }

  private SqlNode createField(int index, SqlNode node) {
    return SqlUtils.getAlias(
        SqlUtils.createCast(node, SqlTypeName.INTEGER), getFutureColumnName(index));
  }

  private static SqlIdentifier getFutureColumnName(int index) {
    return SqlUtils.getIdentifier("z" + index);
  }

  @Test
  void testFutureWhereOne() throws Exception {
    test("(a = 1) future", null);
  }

  @Test
  void testFutureWhereTwoAnd() throws Exception {
    test("(a = 1) future and (b = 1) future", null);
  }

  @Test
  void testFutureWhereTwoOr() throws Exception {
    test("(a = 1) future or (b = 1) future", null);
  }

  @Test
  void testFutureWhereOneMixedAnd() throws Exception {
    test("(a = 1) future and b = 1", "b = 1");
  }

  @Test
  void testFutureWhereOneMixedOr() throws Exception {
    test("(a = 1) future or b = 1", "a = 1 or b = 1", Arrays.asList("a = 1", "b = 1"));
  }

  @Test
  void testFutureWhereTwoMixed() throws Exception {
    test("((a = 1) future or (a = 2) future) and b = 1", "b = 1");
  }

  @Test
  void testFutureWhereTwoMixedReverse() throws Exception {
    test(
        "((a = 1) future and (a = 2) future) or b = 1",
        "a = 1 or a = 2 or b = 1",
        Arrays.asList("a = 1", "a = 2", "b = 1"));
  }

  @Test
  void testFutureWhereOneMultiple() throws Exception {
    test("(a = 1) future and (a = 2) future", null);
  }

  @Test
  void testFutureWhereTwoMultiple() throws Exception {
    test("((a = 1) future or (a = 2) future) and ((b = 1) future or (b = 2) future)", null);
  }

  @Test
  void testFutureWhereMultipleMixed() throws Exception {
    test("(a = 1) future and b = 1 and (c = 1) future", "b = 1");
  }

  @Test
  void testFutureWhereTwoMultipleMixed() throws Exception {
    test(
        "((a = 1) future or (a = 2) future) and b = 1 and ((c = 1) future or (c = 2) future)",
        "b = 1");
  }

  @Test
  void testFutureWhereOrMixed() throws Exception {
    test(
        "((a = 1) future or (a = 2)) and b = 1",
        "(a = 1 or a = 2) and b = 1",
        Arrays.asList("a = 1", "a = 2"));
  }

  @Test
  void testFutureWhereDeepOrMixed() throws Exception {
    test(
        "((a = 1) future or (a = 2) future or (a = 3)) and b = 1",
        "(a = 1 or a = 2 or a = 3) and b = 1",
        Arrays.asList("a = 1", "a = 2", "a = 3"));
  }

  @Test
  void testFutureWhereAggregation() throws Exception {
    test("(a = 1) future", null, null, true);
  }

  @Test
  void testFutureWhereNested() {
    assertThrows(
        IllegalArgumentException.class, () -> test("((a = 1) future or (a = 2)) future", null));
  }

  static class Factory
      extends BaseContextFactory<JdbcSourceContext, JdbcSourceContext, JdbcDataBuffer> {

    public Factory(DbDriver sourceDriver) {
      super(sourceDriver);
    }

    @Override
    protected JdbcSourceContext create(
        Connection connection,
        SqlSelectProgressive select,
        Function<SqlIdentifier, Column> columnMapper,
        List<MetaField> metaFields,
        SqlSelect selectSource) {
      return null;
    }

    @Override
    protected JdbcSourceContext create(
        Connection connection,
        SqlCreateProgressiveView view,
        Function<SqlIdentifier, Column> columnMapper,
        List<MetaField> metaFields,
        SqlSelect selectSource) {
      final SqlSelect select = (SqlSelect) view.getQuery();
      return new JdbcSourceContext(metaFields, null, selectSource, getTables(select.getFrom()));
    }

    @Override
    public JdbcSourceContext create(
        JdbcDataBuffer dataBuffer,
        SqlSelectProgressive select,
        Function<SqlIdentifier, Column> columnMapper) {
      return null;
    }

    @Override
    protected String getMetaFieldName(int index, MetaField metaField) {
      return getFutureColumnName(index).getSimple();
    }
  }
}
