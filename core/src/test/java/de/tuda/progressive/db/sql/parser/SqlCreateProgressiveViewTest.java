package de.tuda.progressive.db.sql.parser;

import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("unchecked")
class SqlCreateProgressiveViewTest {

  private SqlParser.Config config;

  @BeforeEach
  void init() {
    config =
        SqlParser.configBuilder()
            .setUnquotedCasing(Casing.UNCHANGED)
            .setParserFactory(SqlParserImpl.FACTORY)
            .build();
  }

  private void testSimple(boolean replace) throws Exception {
    final String name = "a";
    final String table = "b";

    SqlParser parser =
        SqlParser.create(
            String.format(
                "create %s progressive view %s as select * from %s",
                replace ? "or replace" : "", name, table),
            config);
    SqlNode node = parser.parseStmt();

    assertEquals(SqlCreateProgressiveView.class, node.getClass());

    SqlCreateProgressiveView createProgressiveView = (SqlCreateProgressiveView) node;

    assertEquals(name, createProgressiveView.getName().getSimple());
    assertEquals(replace, createProgressiveView.getReplace());
    assertEquals(SqlSelect.class, createProgressiveView.getQuery().getClass());

    SqlSelect select = (SqlSelect) createProgressiveView.getQuery();

    assertEquals(SqlIdentifier.class, select.getFrom().getClass());

    SqlIdentifier from = (SqlIdentifier) select.getFrom();

    assertEquals(table, from.getSimple());
  }

  @Test
  void simpleCreate() throws Exception {
    testSimple(false);
  }

  @Test
  void simpleCreateOrReplace() throws Exception {
    testSimple(true);
  }

  private String buildGroup(Triple<String, Boolean, String> column) {
    String result = column.getLeft();
    if (column.getMiddle()) {
      result += " future";
    }
    return result;
  }

  private String buildSelect(Triple<String, Boolean, String> column) {
    String result = buildGroup(column);
    final String alias = column.getRight();
    if (alias != null) {
      result += " as " + alias;
    }
    return result;
  }

  private void testFutureGroupBy(Triple<String, Boolean, String>... columns) throws Exception {
    final String name = "a";
    final String table = "b";
    final List<String> selects =
        Arrays.stream(columns).map(this::buildSelect).collect(Collectors.toList());
    final List<String> groupBy =
        Arrays.stream(columns).map(this::buildGroup).collect(Collectors.toList());

    final String sql =
        String.format(
            "create progressive view %s as select %s from %s group by %s",
            name, "x, " + String.join(", ", selects), table, String.join(", ", groupBy));

    SqlParser parser = SqlParser.create(sql, config);
    SqlNode node = parser.parseStmt();

    assertEquals(SqlCreateProgressiveView.class, node.getClass());

    SqlCreateProgressiveView createProgressiveView = (SqlCreateProgressiveView) node;

    assertEquals(name, createProgressiveView.getName().getSimple());
    assertEquals(SqlSelect.class, createProgressiveView.getQuery().getClass());

    SqlSelect select = (SqlSelect) createProgressiveView.getQuery();
    SqlNodeList selectList = select.getSelectList();
    SqlNodeList groupByList = select.getGroup();

    assertEquals(columns.length, groupByList.size());
    for (int i = 0; i < columns.length; i++) {
      SqlIdentifier groupIdentifier;

      if (columns[i].getMiddle()) {
        final String alias = columns[i].getRight();
        if (alias != null) {
          assertAlias(columns[i], selectList.get(i + 1));
        }

        assertEquals(SqlFutureNode.class, groupByList.get(i).getClass());
        groupIdentifier = (SqlIdentifier) ((SqlFutureNode) groupByList.get(i)).getNode();
      } else {
        assertEquals(SqlIdentifier.class, groupByList.get(i).getClass());
        groupIdentifier = (SqlIdentifier) groupByList.get(i);
      }

      assertEquals(columns[i].getLeft(), groupIdentifier.getSimple());
    }
  }

  private void assertAlias(Triple<String, Boolean, String> column, SqlNode alias) {
    assertEquals(SqlBasicCall.class, alias.getClass());

    final SqlBasicCall selectCall = (SqlBasicCall) alias;
    assertEquals(SqlAsOperator.class, selectCall.getOperator().getClass());

    SqlNode[] operands = selectCall.getOperands();

    assertEquals(SqlFutureNode.class, operands[0].getClass());
    final SqlNode node = ((SqlFutureNode) operands[0]).getNode();

    assertEquals(SqlIdentifier.class, node.getClass());
    assertEquals(column.getLeft(), ((SqlIdentifier) node).getSimple());

    assertEquals(SqlIdentifier.class, operands[1].getClass());
    assertEquals(column.getRight(), ((SqlIdentifier) operands[1]).getSimple());
  }

  @Test
  void groupBy() throws Exception {
    testFutureGroupBy(new ImmutableTriple<>("a", false, null));
  }

  @Test
  void groupByFuture() throws Exception {
    testFutureGroupBy(new ImmutableTriple<>("a", true, null));
  }

  @Test
  void groupByMultiple() throws Exception {
    testFutureGroupBy(
        new ImmutableTriple<>("a", false, null), new ImmutableTriple<>("b", false, null));
  }

  @Test
  void groupByMultipleMixed() throws Exception {
    testFutureGroupBy(
        new ImmutableTriple<>("a", false, null), new ImmutableTriple<>("b", true, null));
  }

  @Test
  void groupByMultipleFuture() throws Exception {
    testFutureGroupBy(
        new ImmutableTriple<>("a", true, null), new ImmutableTriple<>("b", true, null));
  }

  @Test
  void groupByFutureAlias() throws Exception {
    testFutureGroupBy(new ImmutableTriple<>("a", true, "foo"));
  }

  private String createEqualsString(Triple<String, String, Boolean> condition) {
    String equals = String.format("%s = '%s'", condition.getLeft(), condition.getMiddle());
    if (condition.getRight()) {
      equals = "(" + equals + ") future";
    }
    return equals;
  }

  private String createFutureWhere(Triple<String, String, Boolean>... conditions) {
    final List<String> whereList = new ArrayList<>();

    final List<String> currentList = new ArrayList<>();
    boolean future = conditions[0].getRight();

    for (Triple<String, String, Boolean> condition : conditions) {
      if (future != condition.getRight()) {
        String where = String.join(" or ", currentList);
        if (future && currentList.size() > 1) {
          where = "(" + where + ")";
        }
        whereList.add(where);
        currentList.clear();
        future = condition.getRight();
      }
      currentList.add(createEqualsString(condition));
    }
    String where = String.join(" or ", currentList);
    if (future && currentList.size() > 1) {
      where = "(" + where + ")";
    }
    whereList.add(where);

    return String.join(" and ", whereList);
  }

  private SqlNode createEqualsNode(Triple<String, String, Boolean> condition) {
    SqlNode equals =
        new SqlBasicCall(
            SqlStdOperatorTable.EQUALS,
            new SqlNode[] {
              SqlUtils.getIdentifier(condition.getLeft()),
              SqlLiteral.createCharString(condition.getMiddle(), SqlParserPos.ZERO)
            },
            SqlParserPos.ZERO);

    if (condition.getRight()) {
      equals = new SqlFutureNode(equals, SqlParserPos.ZERO);
    }
    return equals;
  }

  private SqlNode join(List<SqlNode> nodes, SqlOperator operator) {
    SqlNode node = nodes.get(0);
    for (int i = 1; i < nodes.size(); i++) {
      node = new SqlBasicCall(operator, new SqlNode[] {node, nodes.get(i)}, SqlParserPos.ZERO);
    }
    return node;
  }

  private SqlNode createWhereNode(Triple<String, String, Boolean>... conditions) {
    final List<SqlNode> whereList = new ArrayList<>();

    final List<SqlNode> currentList = new ArrayList<>();
    boolean future = conditions[0].getRight();

    for (Triple<String, String, Boolean> condition : conditions) {
      if (future != condition.getRight()) {
        whereList.add(join(currentList, SqlStdOperatorTable.OR));
        currentList.clear();
        future = condition.getRight();
      }
      currentList.add(createEqualsNode(condition));
    }
    whereList.add(join(currentList, SqlStdOperatorTable.OR));

    return join(whereList, SqlStdOperatorTable.AND);
  }

  private void testFutureWhere(Triple<String, String, Boolean>... conditions) throws Exception {
    final String name = "a";
    final String table = "b";

    final String futureWhere = createFutureWhere(conditions);

    final String sql =
        String.format(
            "create progressive view %s as select * from %s where %s", name, table, futureWhere);

    final SqlParser parser = SqlParser.create(sql, config);
    final SqlNode node = parser.parseStmt();

    assertEquals(SqlCreateProgressiveView.class, node.getClass());

    final SqlCreateProgressiveView createProgressiveView = (SqlCreateProgressiveView) node;

    assertEquals(name, createProgressiveView.getName().getSimple());
    assertEquals(SqlSelect.class, createProgressiveView.getQuery().getClass());

    final SqlSelect select = (SqlSelect) createProgressiveView.getQuery();
    assertTrue(
        SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO))
            .equalsDeep(select.getSelectList(), Litmus.THROW));

    final SqlNode where = select.getWhere();
    assertNotNull(where);

    final SqlNode expectedWhere = createWhereNode(conditions);
    assertTrue(expectedWhere.equalsDeep(where, Litmus.THROW));
  }

  @Test
  void futureWhereSingle() throws Exception {
    testFutureWhere(ImmutableTriple.of("a", "1", true));
  }

  @Test
  void futureWhereMultiple() throws Exception {
    testFutureWhere(ImmutableTriple.of("a", "1", true), ImmutableTriple.of("a", "2", true));
  }

  @Test
  void futureWherePre() throws Exception {
    testFutureWhere(
        ImmutableTriple.of("a", "1", true),
        ImmutableTriple.of("a", "2", true),
        ImmutableTriple.of("b", "1", false));
  }

  @Test
  void futureWherePreReverse() throws Exception {
    testFutureWhere(
        ImmutableTriple.of("b", "1", false),
        ImmutableTriple.of("a", "1", true),
        ImmutableTriple.of("a", "2", true));
  }

  @Test
  void futureWhereMixed() throws Exception {
    testFutureWhere(
        ImmutableTriple.of("a", "1", true),
        ImmutableTriple.of("b", "2", false),
        ImmutableTriple.of("a", "2", true),
        ImmutableTriple.of("b", "2", false));
  }
}
