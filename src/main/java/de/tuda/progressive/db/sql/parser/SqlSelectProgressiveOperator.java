package de.tuda.progressive.db.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;

import java.util.ArrayList;
import java.util.List;

public class SqlSelectProgressiveOperator extends SqlOperator {
  public static final SqlSelectProgressiveOperator INSTANCE = new SqlSelectProgressiveOperator();

  private SqlSelectProgressiveOperator() {
    super("SELECT PROGRESSIVE", SqlKind.SELECT, 2, true, ReturnTypes.SCOPE, null, null);
  }

  @Override
  public SqlSyntax getSyntax() {
    return SqlSyntax.SPECIAL;
  }

  @Override
  public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    SqlSelectProgressive select = (SqlSelectProgressive) call;
    final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
    writer.sep("SELECT PROGRESSIVE");

    SqlNodeList keywordList = (SqlNodeList) select.getOperandList().get(0);
    for (int i = 0; i < keywordList.size(); i++) {
      final SqlNode keyword = keywordList.get(i);
      keyword.unparse(writer, 0, 0);
    }
    SqlNode selectClause = select.getSelectList();
    if (selectClause == null) {
      selectClause = SqlIdentifier.star(SqlParserPos.ZERO);
    }
    final SqlWriter.Frame selectListFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT_LIST);
    unparseListClause(writer, selectClause);
    writer.endList(selectListFrame);

    writer.sep("FROM");
    final SqlWriter.Frame fromFrame = writer.startList(SqlWriter.FrameTypeEnum.FROM_LIST);
    select
        .getFrom()
        .unparse(writer, SqlJoin.OPERATOR.getLeftPrec() - 1, SqlJoin.OPERATOR.getRightPrec() - 1);
    writer.endList(fromFrame);

    if (select.getWithFutureGroupBy() != null) {
      writer.sep("WITH FUTURE WHERE");
      final SqlWriter.Frame groupFrame = writer.startList(SqlWriter.FrameTypeEnum.WHERE_LIST);
      unparseListClause(writer, select.getWithFutureWhere());
      writer.endList(groupFrame);
    }

    if (select.getWhere() != null) {
      writer.sep("WHERE");

      if (!writer.isAlwaysUseParentheses()) {
        SqlNode node = select.getWhere();

        // decide whether to split on ORs or ANDs
        SqlKind whereSepKind = SqlKind.AND;
        if ((node instanceof SqlCall) && node.getKind() == SqlKind.OR) {
          whereSepKind = SqlKind.OR;
        }

        // unroll whereClause
        final List<SqlNode> list = new ArrayList<>(0);
        while (node.getKind() == whereSepKind) {
          assert node instanceof SqlCall;
          final SqlCall call1 = (SqlCall) node;
          list.add(0, call1.operand(1));
          node = call1.operand(0);
        }
        list.add(0, node);

        // unparse in a WhereList frame
        final SqlWriter.Frame whereFrame = writer.startList(SqlWriter.FrameTypeEnum.WHERE_LIST);
        unparseListClause(
            writer, new SqlNodeList(list, select.getWhere().getParserPosition()), whereSepKind);
        writer.endList(whereFrame);
      } else {
        select.getWhere().unparse(writer, 0, 0);
      }
    }
    if (select.getWithFutureGroupBy() != null) {
      writer.sep("WITH FUTURE GROUP BY");
      final SqlWriter.Frame groupFrame = writer.startList(SqlWriter.FrameTypeEnum.GROUP_BY_LIST);
      if (select.getWithFutureGroupBy().getList().isEmpty()) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE, "(", ")");
        writer.endList(frame);
      } else {
        unparseListClause(writer, select.getWithFutureGroupBy());
      }
      writer.endList(groupFrame);
    }
    if (select.getGroup() != null) {
      writer.sep("GROUP BY");
      final SqlWriter.Frame groupFrame = writer.startList(SqlWriter.FrameTypeEnum.GROUP_BY_LIST);
      if (select.getGroup().getList().isEmpty()) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE, "(", ")");
        writer.endList(frame);
      } else {
        unparseListClause(writer, select.getGroup());
      }
      writer.endList(groupFrame);
    }
    if (select.getHaving() != null) {
      writer.sep("HAVING");
      select.getHaving().unparse(writer, 0, 0);
    }
    if (select.getWindowList().size() > 0) {
      writer.sep("WINDOW");
      final SqlWriter.Frame windowFrame =
          writer.startList(SqlWriter.FrameTypeEnum.WINDOW_DECL_LIST);
      for (SqlNode windowDecl : select.getWindowList()) {
        writer.sep(",");
        windowDecl.unparse(writer, 0, 0);
      }
      writer.endList(windowFrame);
    }
    if (select.getOrderList() != null && select.getOrderList().size() > 0) {
      writer.sep("ORDER BY");
      final SqlWriter.Frame orderFrame = writer.startList(SqlWriter.FrameTypeEnum.ORDER_BY_LIST);
      unparseListClause(writer, select.getOrderList());
      writer.endList(orderFrame);
    }
    writer.fetchOffset(select.getFetch(), select.getOffset());
    writer.endList(selectFrame);
  }
}
