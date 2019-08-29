package de.tuda.progressive.db.sql.parser;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlSelectProgressive extends SqlSelect {

  private SqlNodeList withFutureWhere;

  private SqlNodeList withFutureGroupBy;

  public SqlSelectProgressive(
      SqlParserPos pos,
      SqlNodeList keywordList,
      SqlNodeList selectList,
      SqlNode from,
      SqlNodeList withFutureWhere,
      SqlNode where,
      SqlNodeList withFutureGroupBy,
      SqlNodeList groupBy,
      SqlNode having,
      SqlNodeList windowDecls,
      SqlNodeList orderBy,
      SqlNode offset,
      SqlNode fetch) {
    super(
        pos,
        keywordList,
        selectList,
        from,
        where,
        groupBy,
        having,
        windowDecls,
        orderBy,
        offset,
        fetch);

    this.withFutureWhere = withFutureWhere;
    this.withFutureGroupBy = withFutureGroupBy;
  }

  @Override
  public SqlOperator getOperator() {
    return SqlSelectProgressiveOperator.INSTANCE;
  }

  public SqlNodeList getWithFutureWhere() {
    return withFutureWhere;
  }

  public SqlNodeList getWithFutureGroupBy() {
    return withFutureGroupBy;
  }
}
