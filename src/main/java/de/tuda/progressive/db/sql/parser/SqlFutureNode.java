package de.tuda.progressive.db.sql.parser;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;

public class SqlFutureNode extends SqlNodeList {

  private final boolean hideParens;

  public SqlFutureNode(SqlNode node, SqlParserPos pos) {
    this(node, true, pos);
  }

  public SqlFutureNode(SqlNode node, boolean hideParens, SqlParserPos pos) {
    super(Collections.singleton(node), pos);
    this.hideParens = hideParens;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (hideParens) {
      super.unparse(writer, 0, 0);
    } else {
      super.unparse(writer, leftPrec, rightPrec);
    }
    writer.keyword("FUTURE");
  }

  public SqlNode getNode() {
    return getList().get(0);
  }

  @Override
  public SqlNodeList clone(SqlParserPos pos) {
    return new SqlFutureNode(getList().get(0), hideParens, pos);
  }
}
