package de.tuda.progressive.db.statement.context.impl;

import de.tuda.progressive.db.buffer.DataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlFutureNode;
import de.tuda.progressive.db.sql.parser.SqlSelectProgressive;
import de.tuda.progressive.db.statement.context.ContextFactory;
import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.util.MetaFieldUtils;
import de.tuda.progressive.db.util.SqlUtils;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

public abstract class BaseContextFactory<
    C1 extends JdbcSourceContext, C2 extends BaseContext, D extends DataBuffer>
    implements ContextFactory<C1, C2, D> {

  protected final DbDriver sourceDriver;

  public BaseContextFactory(DbDriver sourceDriver) {
    this.sourceDriver = sourceDriver;
  }

  @Override
  public C1 create(
      Connection connection,
      SqlSelectProgressive select,
      Function<SqlIdentifier, Column> columnMapper) {
    final List<MetaField> metaFields = getMetaFields(select.getSelectList());
    final SqlSelect selectSource = transformSelect(select, metaFields);

    return create(connection, select, columnMapper, metaFields, selectSource);
  }

  @Override
  public C1 create(
      Connection connection,
      SqlCreateProgressiveView view,
      Function<SqlIdentifier, Column> columnMapper) {
    final SqlSelect select = (SqlSelect) view.getQuery();

    final List<MetaField> metaFields = getMetaFields(select.getSelectList());
    final SqlSelect selectSource = transformSelect(select, metaFields);

    return create(connection, view, columnMapper, metaFields, selectSource);
  }

  protected abstract C1 create(
      Connection connection,
      SqlSelectProgressive select,
      Function<SqlIdentifier, Column> columnMapper,
      List<MetaField> metaFields,
      SqlSelect selectSource);

  protected abstract C1 create(
      Connection connection,
      SqlCreateProgressiveView view,
      Function<SqlIdentifier, Column> columnMapper,
      List<MetaField> metaFields,
      SqlSelect selectSource);

  private <T> List<T> get(SqlNodeList columns, Function<SqlNode, T> func) {
    return StreamSupport.stream(columns.spliterator(), false)
        .map(func)
        .collect(Collectors.toList());
  }

  protected final List<MetaField> getMetaFields(List<MetaField> metaFields, List<Integer> indices) {
    return indices.stream().map(metaFields::get).collect(Collectors.toList());
  }

  protected final List<MetaField> getMetaFields(SqlNodeList columns) {
    return get(columns, this::columnToMetaField);
  }

  private MetaField columnToMetaField(SqlNode column) {
    if (column instanceof SqlIdentifier || column instanceof SqlLiteral) {
      return MetaField.NONE;
    }
    if (column instanceof SqlBasicCall) {
      SqlBasicCall call = (SqlBasicCall) column;
      SqlOperator operator = call.getOperator();

      switch (operator.getName().toUpperCase()) {
        case "AVG":
          return MetaField.AVG;
        case "COUNT":
          return MetaField.COUNT;
        case "SUM":
          return MetaField.SUM;
        case "AS":
          return columnToMetaField(call.getOperands()[0]);
        case "PROGRESSIVE_PARTITION":
          return MetaField.PARTITION;
        case "PROGRESSIVE_PROGRESS":
          return MetaField.PROGRESS;
        case "PROGRESSIVE_CONFIDENCE":
          return MetaField.CONFIDENCE_INTERVAL;
      }

      throw new IllegalArgumentException("operation is not supported: " + operator.getName());
    }
    if (column instanceof SqlFutureNode) {
      return MetaField.FUTURE_GROUP;
    }

    throw new IllegalArgumentException("column type is not supported: " + column.getClass());
  }

  protected final SqlSelect transformSelect(SqlSelect select, List<MetaField> metaFields) {
    if (select.getHaving() != null) {
      throw new IllegalArgumentException("having is not supported");
    }

    final SqlNodeList oldSelectList = select.getSelectList();
    final SqlNodeList oldGroups = select.getGroup() == null ? SqlNodeList.EMPTY : select.getGroup();

    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    final SqlNodeList groups = new SqlNodeList(SqlParserPos.ZERO);

    for (int i = 0; i < oldSelectList.size(); i++) {
      SqlBasicCall call;
      switch (metaFields.get(i)) {
        case AVG:
          call = (SqlBasicCall) oldSelectList.get(i);
          SqlBasicCall avg;
          if (call.getKind() == SqlKind.AS) {
            avg = (SqlBasicCall) call.getOperands()[0];
          } else {
            avg = call;
          }

          selectList.add(SqlUtils.createSumAggregation(avg.getOperands()));
          selectList.add(SqlUtils.createCountAggregation(avg.getOperands()));
          break;
        case COUNT:
        case SUM:
        case NONE:
          selectList.add(oldSelectList.get(i));
          break;
        case PARTITION:
        case PROGRESS:
          // don't add anything
          break;
        case FUTURE_GROUP:
          selectList.add(removeFuture(oldSelectList.get(i)));
          break;
        case CONFIDENCE_INTERVAL:
          call = (SqlBasicCall) oldSelectList.get(i);
          selectList.add(SqlUtils.createCountAggregation(call.getOperands()));
          break;
        default:
          throw new IllegalArgumentException("metaField not supported: " + metaFields.get(i));
      }
    }

    for (SqlNode group : oldGroups) {
      if (group instanceof SqlFutureNode) {
        groups.add(((SqlFutureNode) group).getNode());
      } else {
        groups.add(group);
      }
    }

    final SqlNode from = transformFrom(select.getFrom());

    final List<SqlNode> futures = new ArrayList<>();
    final SqlBasicCall where = createWhere(futures, from, select.getWhere());
    final int metaFieldsSize = metaFields.size();

    boolean hasAggregation = MetaFieldUtils.hasAggregation(metaFields);
    for (int i = 0; i < futures.size(); i++) {
      final SqlIdentifier name =
          SqlUtils.getIdentifier(getMetaFieldName(metaFieldsSize + i, MetaField.FUTURE_WHERE));
      metaFields.add(MetaField.FUTURE_WHERE);
      selectList.add(
          SqlUtils.getAlias(SqlUtils.createCast(futures.get(i), SqlTypeName.INTEGER), name));

      if (hasAggregation) {
        groups.add(name);
      }
    }

    return new SqlSelect(
        SqlParserPos.ZERO,
        null,
        selectList,
        from,
        where,
        groups.size() > 0 ? groups : null,
        null,
        select.getWindowList(),
        select.getOrderList(),
        select.getOffset(),
        select.getFetch());
  }

  private SqlNode transformFrom(SqlNode node) {
    if (node instanceof SqlIdentifier) {
      final SqlIdentifier identifier = (SqlIdentifier) node;
      if (sourceDriver.hasPartitions()) {
        return SqlUtils.getIdentifier(sourceDriver.getPartitionTable(identifier.getSimple()));
      } else {
        return identifier;
      }
    } else if (node instanceof SqlJoin) {
      final SqlJoin join = (SqlJoin) node;
      // TODO currently just comma supported
      return new SqlJoin(
          SqlParserPos.ZERO,
          transformFrom(join.getLeft()),
          SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
          JoinType.COMMA.symbol(SqlParserPos.ZERO),
          transformFrom(join.getRight()),
          JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
          null);
    } else {
      throw new IllegalArgumentException("from not supported: " + node);
    }
  }

  protected final SqlNode removeFuture(SqlNode node) {
    if (node instanceof SqlFutureNode) {
      return ((SqlFutureNode) node).getNode();
    }

    if (node instanceof SqlBasicCall) {
      final SqlBasicCall call = (SqlBasicCall) node;
      final SqlNode[] operands = call.getOperands();

      if (call.getOperator() instanceof SqlAsOperator) {
        return new SqlBasicCall(
            SqlStdOperatorTable.AS,
            new SqlNode[]{removeFuture(operands[0]), operands[1]},
            SqlParserPos.ZERO);
      }
    }

    return node;
  }

  private SqlBasicCall createWhere(List<SqlNode> futures, SqlNode from, SqlNode oldWhere) {
    final SqlBasicCall where = transformWhere(futures, oldWhere);
    if (!sourceDriver.hasPartitions()) {
      return where;
    }

    final SqlBasicCall eqPartition = createWhereEqPartition(from);
    if (where == null) {
      return eqPartition;
    }

    return new SqlBasicCall(
        SqlStdOperatorTable.AND, new SqlNode[]{where, eqPartition}, SqlParserPos.ZERO);
  }

  private SqlBasicCall createWhereEqPartition(SqlNode node) {
    if (node instanceof SqlIdentifier) {
      final SqlIdentifier identifier = (SqlIdentifier) node;

      return new SqlBasicCall(
          SqlStdOperatorTable.EQUALS,
          new SqlNode[]{
              new SqlIdentifier(
                  Arrays.asList(identifier.getSimple(), "_partition"), SqlParserPos.ZERO),
              new SqlDynamicParam(0, SqlParserPos.ZERO)
          },
          SqlParserPos.ZERO);
    } else if (node instanceof SqlJoin) {
      final SqlJoin join = (SqlJoin) node;

      return new SqlBasicCall(
          SqlStdOperatorTable.AND,
          new SqlNode[]{
              createWhereEqPartition(join.getLeft()), createWhereEqPartition(join.getRight())
          },
          SqlParserPos.ZERO);
    } else {
      throw new IllegalArgumentException("node not supported: " + node);
    }
  }

  private SqlBasicCall transformWhere(List<SqlNode> futures, SqlNode oldWhere) {
    if (oldWhere == null) {
      return null;
    }

    return resolveWhereFutures(futures, oldWhere, false, false);
  }

  private SqlBasicCall resolveWhereFutures(
      List<SqlNode> futures, SqlNode node, boolean add, boolean inFuture) {
    if (node instanceof SqlFutureNode) {
      if (inFuture) {
        throw new IllegalArgumentException("future nodes must not be nested");
      }

      inFuture = true;
      node = ((SqlFutureNode) node).getNode();
      futures.add(node);
    }

    final SqlBasicCall call = (SqlBasicCall) node;
    switch (call.getOperator().getName()) {
      case "AND": {
        FutureType leftFuture = getFutureType(call.getOperands()[0]);
        FutureType rightFuture = getFutureType(call.getOperands()[1]);
        boolean reverse = add && leftFuture == FutureType.FULL && rightFuture == FutureType.FULL;

        final SqlBasicCall left =
            resolveWhereFutures(futures, call.getOperands()[0], reverse, inFuture);
        final SqlBasicCall right =
            resolveWhereFutures(futures, call.getOperands()[1], reverse, inFuture);

        if (left == null) {
          return right;
        } else if (right == null) {
          return left;
        } else {
          return new SqlBasicCall(
              reverse ? SqlStdOperatorTable.OR : SqlStdOperatorTable.AND,
              new SqlNode[]{left, right},
              SqlParserPos.ZERO);
        }
      }
      case "OR": {
        FutureType leftFuture = getFutureType(call.getOperands()[0]);
        FutureType rightFuture = getFutureType(call.getOperands()[1]);
        boolean newAdd = add || (leftFuture == FutureType.FULL ^ rightFuture == FutureType.FULL);

        final SqlBasicCall left =
            resolveWhereFutures(futures, call.getOperands()[0], newAdd, inFuture);

        if (leftFuture == FutureType.NONE && rightFuture == FutureType.FULL) {
          futures.add(left);
        }

        final SqlBasicCall right =
            resolveWhereFutures(futures, call.getOperands()[1], newAdd, inFuture);

        if (leftFuture == FutureType.FULL && rightFuture == FutureType.NONE) {
          futures.add(right);
        }

        if (left == null) {
          return right;
        } else if (right == null) {
          return left;
        } else {
          return new SqlBasicCall(
              SqlStdOperatorTable.OR, new SqlNode[]{left, right}, SqlParserPos.ZERO);
        }
      }
      default:
        if (!inFuture || add) {
          return call;
        }
    }

    return null;
  }

  protected final FutureType getFutureType(SqlNode node) {
    if (node instanceof SqlFutureNode) {
      return FutureType.FULL;
    } else if (node instanceof SqlBasicCall) {
      final SqlBasicCall call = (SqlBasicCall) node;
      switch (call.getOperator().getName().toUpperCase()) {
        case "AND":
        case "OR":
          final FutureType leftFuture = getFutureType(call.getOperands()[0]);
          final FutureType rightFuture = getFutureType(call.getOperands()[1]);
          if (leftFuture == FutureType.NONE && rightFuture == FutureType.NONE) {
            return FutureType.NONE;
          } else if (leftFuture == FutureType.FULL && rightFuture == FutureType.FULL) {
            return FutureType.FULL;
          } else {
            return FutureType.MIXED;
          }
        default:
          return FutureType.NONE;
      }
    } else {
      throw new IllegalArgumentException("node type not expected: " + node);
    }
  }

  protected String getMetaFieldName(int index, MetaField metaField) {
    if (metaField == MetaField.NONE) {
      return String.format("f%d", index);
    } else {
      return String.format("f%d_%s", index, metaField.name());
    }
  }

  protected enum FutureType {
    NONE,
    MIXED,
    FULL
  }

  protected final List<SqlIdentifier> getTables(SqlNode node) {
    if (node instanceof SqlIdentifier) {
      return Collections.singletonList((SqlIdentifier) node);
    } else if (node instanceof SqlJoin) {
      final SqlJoin join = (SqlJoin) node;
      final List<SqlIdentifier> tables = new ArrayList<>();
      tables.addAll(getTables(join.getLeft()));
      tables.addAll(getTables(join.getRight()));
      return tables;
    } else {
      throw new IllegalArgumentException("node not supported: " + node);
    }
  }
}
