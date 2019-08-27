package de.tuda.progressive.db.sql.parser;

import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SqlSelectProgressiveTest {

  private SqlParser.Config config;

  @BeforeEach
  void init() {
    config =
        SqlParser.configBuilder()
            .setUnquotedCasing(Casing.UNCHANGED)
            .setParserFactory(SqlParserImpl.FACTORY)
            .build();
  }

  private void test(
      String sql,
      SqlNodeList selectList,
      SqlNode from,
      SqlNodeList withFutureWhere,
      SqlNodeList withFutureGroupBy)
      throws Exception {
    final SqlNode node = SqlParser.create(sql, config).parseQuery();

    assertEquals(SqlSelectProgressive.class, node.getClass());

    final SqlSelectProgressive select = (SqlSelectProgressive) node;

    assertTrue(selectList.equalsDeep(select.getSelectList(), Litmus.THROW));
    assertTrue(from.equalsDeep(select.getFrom(), Litmus.THROW));

    if (withFutureWhere == null) {
      assertNull(select.getWithFutureWhere());
    } else {
      assertTrue(withFutureWhere.equalsDeep(select.getWithFutureWhere(), Litmus.THROW));
    }

    if (withFutureGroupBy == null) {
      assertNull(select.getWithFutureGroupBy());
    } else {
      assertTrue(withFutureGroupBy.equalsDeep(select.getWithFutureGroupBy(), Litmus.THROW));
    }
  }

  private SqlNode createEquals(String name, String value) {
    return new SqlBasicCall(
        SqlStdOperatorTable.EQUALS,
        new SqlNode[] {
          SqlUtils.getIdentifier(name), SqlLiteral.createCharString(value, SqlParserPos.ZERO)
        },
        SqlParserPos.ZERO);
  }

  @Test
  void testProgressiveKeyword() throws Exception {
    test(
        "select progressive * from t",
        SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO)),
        SqlUtils.getIdentifier("t"),
        null,
        null);
  }

  @Test
  void testWithFutureWhere() throws Exception {
    test(
        "select progressive * from t with future where a = '1'",
        SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO)),
        SqlUtils.getIdentifier("t"),
        SqlNodeList.of(createEquals("a", "1")),
        null);
  }

  @Test
  void testWithFutureWhereMultiple() throws Exception {
    test(
        "select progressive * from t with future where a = '1', b = '2'",
        SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO)),
        SqlUtils.getIdentifier("t"),
        SqlNodeList.of(createEquals("a", "1"), createEquals("b", "2")),
        null);
  }

  @Test
  void testWithFutureGroup() throws Exception {
    test(
        "select progressive * from t with future group by a",
        SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO)),
        SqlUtils.getIdentifier("t"),
        null,
        SqlNodeList.of(SqlUtils.getIdentifier("a")));
  }

  @Test
  void testWithFutureGroupMultiple() throws Exception {
    test(
        "select progressive * from t with future group by a, b",
        SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO)),
        SqlUtils.getIdentifier("t"),
        null,
        SqlNodeList.of(SqlUtils.getIdentifier("a"), SqlUtils.getIdentifier("b")));
  }

  @Test
  void testWithFutureWhereAndFutureGroup() throws Exception {
    test(
        "select progressive * from t with future where a = '1' with future group by a",
        SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO)),
        SqlUtils.getIdentifier("t"),
        SqlNodeList.of(createEquals("a", "1")),
        SqlNodeList.of(SqlUtils.getIdentifier("a")));
  }
}
