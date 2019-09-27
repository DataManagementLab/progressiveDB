package de.tuda.progressive.db.statement.context.impl.jdbc;

import de.tuda.progressive.db.buffer.impl.JdbcDataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlFutureNode;
import de.tuda.progressive.db.sql.parser.SqlSelectProgressive;
import de.tuda.progressive.db.sql.parser.SqlUpsert;
import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.statement.context.impl.BaseContextFactory;
import de.tuda.progressive.db.statement.context.impl.JdbcSourceContext;
import de.tuda.progressive.db.util.MetaFieldUtils;
import de.tuda.progressive.db.util.SqlUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

public class JdbcContextFactory
    extends BaseContextFactory<JdbcSelectContext, JdbcSourceContext, JdbcDataBuffer> {

  private final DbDriver bufferDriver;

  public JdbcContextFactory(DbDriver sourceDriver, DbDriver bufferDriver) {
    super(sourceDriver);

    if (!bufferDriver.hasUpsert()) {
      throw new IllegalArgumentException("driver does not support upsert");
    }

    this.bufferDriver = bufferDriver;
  }

  @Override
  protected JdbcSelectContext create(
      Connection connection,
      SqlSelectProgressive select,
      Function<SqlIdentifier, Column> columnMapper,
      List<MetaField> metaFields,
      SqlSelect selectSource) {
    final List<SqlIdentifier> fieldNames = getFieldNames(select.getSelectList());
    final List<String> bufferFieldNames = getBufferFieldNames(metaFields);
    final boolean hasAggregation = MetaFieldUtils.hasAggregation(metaFields);
    final SqlNodeList indexColumns = getIndexColumns(metaFields, hasAggregation);
    final Map<Integer, Pair<Integer, Integer>> bounds = getBounds(columnMapper, metaFields, select);
    final String sql = getPrepareSql(selectSource);

    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      final ResultSetMetaData metaData = statement.getMetaData();
      final String bufferTableName = generateBufferTableName();
      final SqlCreateTable createBuffer =
          getCreateBuffer(metaData, bufferFieldNames, bufferTableName, indexColumns);
      final SqlSelect selectBuffer =
          getSelectBuffer(select, bufferFieldNames, bufferTableName, fieldNames, metaFields);

      return builder(bufferFieldNames, bufferTableName, indexColumns)
          .metaFields(metaFields)
          .bounds(bounds)
          .selectSource(selectSource)
          .sourceTables(getTables(select.getFrom()))
          .fieldNames(fieldNames)
          .createBuffer(createBuffer)
          .selectBuffer(selectBuffer)
          .build();
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  @Override
  public JdbcSourceContext create(
      JdbcDataBuffer dataBuffer,
      SqlSelectProgressive select,
      Function<SqlIdentifier, Column> columnMapper) {
    final JdbcSelectContext context = dataBuffer.getContext();
    final Pair<SqlSelect, List<Integer>> transformed = transformSelect(context, select);

    final List<MetaField> metaFields =
        getMetaFields(context.getMetaFields(), transformed.getRight());
    final SqlSelect selectBuffer = transformed.getLeft();
    final Map<Integer, Pair<Integer, Integer>> bounds = dataBuffer.getContext().getBounds();

    return new JdbcSourceContext.Builder()
        .metaFields(metaFields)
        .bounds(bounds)
        .selectSource(selectBuffer)
        .build();
  }

  @Override
  protected JdbcSelectContext create(
      Connection connection,
      SqlCreateProgressiveView view,
      Function<SqlIdentifier, Column> columnMapper,
      List<MetaField> metaFields,
      SqlSelect selectSource) {
    final SqlSelect select = (SqlSelect) view.getQuery();
    final List<SqlIdentifier> fieldNames = getFieldNames(select.getSelectList());
    final List<String> bufferFieldNames = getBufferFieldNames(metaFields);
    final boolean hasAggregation = MetaFieldUtils.hasAggregation(metaFields);
    final SqlNodeList indexColumns = getIndexColumns(metaFields, hasAggregation);
    final Map<Integer, Pair<Integer, Integer>> bounds = getBounds(columnMapper, metaFields, select);
    final String sql = getPrepareSql(selectSource);

    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      final ResultSetMetaData metaData = statement.getMetaData();
      final String bufferTableName = view.getName().getSimple();
      final SqlCreateTable createBuffer =
          getCreateBuffer(metaData, bufferFieldNames, bufferTableName, indexColumns);
      final SqlSelect selectBuffer =
          getSelectBuffer(
              select, bufferFieldNames, bufferTableName, fieldNames, metaFields, select.getWhere(),
              true);

      return builder(bufferFieldNames, bufferTableName, indexColumns)
          .createBuffer(createBuffer)
          .selectSource(selectSource)
          .sourceTables(getTables(select.getFrom()))
          .bounds(bounds)
          .selectBuffer(selectBuffer)
          .metaFields(metaFields)
          .fieldNames(fieldNames)
          .prepareSelect(false)
          .build();
    } catch (SQLException e) {
      // TODO
      throw new RuntimeException(e);
    }
  }

  private String getPrepareSql(SqlSelect select) {
    if (!sourceDriver.hasPartitions()) {
      select = (SqlSelect) select.clone(SqlParserPos.ZERO);
      select.setFrom(transformFromPrepare(select.getFrom()));
    }
    return sourceDriver.toSql(select);
  }

  private SqlNode transformFromPrepare(SqlNode node) {
    if (node instanceof SqlIdentifier) {
      final SqlIdentifier identifier = (SqlIdentifier) node;
      return SqlUtils.getIdentifier(sourceDriver.getPartitionTable(identifier.getSimple(), 0));
    } else {
      final SqlJoin join = (SqlJoin) node;
      // TODO currently just comma supported
      return new SqlJoin(
          SqlParserPos.ZERO,
          transformFromPrepare(join.getLeft()),
          SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
          JoinType.COMMA.symbol(SqlParserPos.ZERO),
          transformFromPrepare(join.getRight()),
          JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
          null);
    }
  }

  private Map<Integer, Pair<Integer, Integer>> getBounds(
      Function<SqlIdentifier, Integer> fieldMapper,
      List<MetaField> metaFields,
      Function<Integer, Pair<Integer, Integer>> boundMapper,
      SqlNodeList selectList) {
    final Map<Integer, Pair<Integer, Integer>> bounds = new HashMap<>();

    for (int i = 0; i < metaFields.size(); i++) {
      if (metaFields.get(i) == MetaField.CONFIDENCE_INTERVAL) {
        SqlNode node = selectList.get(i);
        SqlIdentifier fieldName = null;

        if (node instanceof SqlBasicCall) {
          final SqlBasicCall call = (SqlBasicCall) node;

          if (SqlStdOperatorTable.AS.equals(call.getOperator())) {
            node = call.operand(1);
          }
        }

        if (node instanceof SqlIdentifier) {
          fieldName = (SqlIdentifier) node;
        }

        final int index = fieldMapper.apply(fieldName);
        bounds.put(i, boundMapper.apply(index));
      }
    }

    return bounds;
  }

  private Map<Integer, Pair<Integer, Integer>> getBounds(
      Function<SqlIdentifier, Column> columnMapper, List<MetaField> metaFields, SqlSelect select) {
    final SqlNodeList selectList = select.getSelectList();
    final List<SqlIdentifier> tables = getTables(select.getFrom());
    final Map<Integer, Pair<Integer, Integer>> bounds = new HashMap<>();

    for (int i = 0; i < metaFields.size(); i++) {
      if (metaFields.get(i) == MetaField.CONFIDENCE_INTERVAL) {
        SqlBasicCall node = (SqlBasicCall) selectList.get(i);
        if (SqlStdOperatorTable.AS.equals(node.getOperator())) {
          node = node.operand(0);
        }

        final SqlIdentifier columnIdentifier = node.operand(0);
        Column column;
        if (columnIdentifier.names.size() == 1) {
          column = tables.stream()
              .map(t -> SqlUtils.getIdentifier(t.getSimple(), columnIdentifier.getSimple()))
              .map(columnMapper).filter(Objects::nonNull).findAny().get();
        } else {
          column = columnMapper.apply(columnIdentifier);
        }

        if (column == null) {
          throw new IllegalArgumentException("column not found: " + columnIdentifier);
        }
        bounds.put(i, ImmutablePair.of((int) column.getMin(), (int) column.getMax()));
      }
    }

    return bounds;
  }

  private Pair<SqlSelect, List<Integer>> transformSelect(
      JdbcBufferContext context, SqlSelectProgressive select) {
    final SqlSelect selectBuffer = context.getSelectBuffer();
    final List<Integer> indices = new ArrayList<>();
    final List<Integer> futureGroupIndices =
        getFutureGroupIndices(
            context::getFieldIndex, context.getMetaFields(), select.getWithFutureGroupBy());

    final SqlNodeList selectList = SqlNodeList.clone(selectBuffer.getSelectList());
    final SqlNodeList groups =
        selectBuffer.getGroup() == null
            ? new SqlNodeList(SqlParserPos.ZERO)
            : SqlNodeList.clone(selectBuffer.getGroup());

    final List<MetaField> metaFields = context.getMetaFields();
    for (int i = 0; i < metaFields.size(); i++) {
      switch (metaFields.get(i)) {
        case NONE:
        case AVG:
        case COUNT:
        case SUM:
        case CONFIDENCE_INTERVAL:
        case PARTITION:
        case PROGRESS:
          indices.add(i);
          break;
        case FUTURE_GROUP:
          if (futureGroupIndices.contains(i)) {
            indices.add(i);
            selectList.add(getBufferField(context, i, true));
          }
          break;
        case FUTURE_WHERE:
          // ignore
          break;
        default:
          throw new IllegalArgumentException("metaField not supported: " + metaFields.get(i));
      }
    }

    if (select.getWithFutureGroupBy() != null) {
      for (SqlNode group : select.getWithFutureGroupBy()) {
        groups.add(getBufferField(context, (SqlIdentifier) group, false));
      }
    }

    final SqlSelect viewSelect =
        new SqlSelect(
            SqlParserPos.ZERO,
            null,
            selectList,
            select.getFrom(),
            activateFutures(metaFields, select.getWithFutureWhere(), selectBuffer.getWhere()),
            groups.size() > 0 ? groups : null,
            null,
            null,
            null,
            null,
            null);

    return ImmutablePair.of(
        new SqlSelect(
            SqlParserPos.ZERO,
            null,
            select.getSelectList(),
            SqlUtils.getAlias(viewSelect, (SqlIdentifier) select.getFrom()),
            select.getWhere(),
            select.getGroup(),
            select.getHaving(),
            null,
            select.getOrderList(),
            select.getOffset(),
            select.getFetch()),
        indices);
  }

  private SqlNode activateFutures(List<MetaField> metaFields, SqlNodeList futures, SqlNode node) {
    if (node == null) {
      return null;
    }
    if (futures == null) {
      futures = SqlNodeList.EMPTY;
    }

    int index;
    for (index = 0; index < metaFields.size(); index++) {
      if (metaFields.get(index) == MetaField.FUTURE_WHERE) {
        break;
      }
    }

    final Pair<SqlNode, Integer> result = activateFutures(futures, node, index);
    return result.getLeft();
  }

  private Pair<SqlNode, Integer> activateFutures(SqlNodeList futures, SqlNode node, int index) {
    if (node instanceof SqlFutureNode) {
      final SqlNode innerNode = ((SqlFutureNode) node).getNode();
      if (SqlUtils.contains(futures, innerNode)) {
        return activateFutures(futures, innerNode, index);
      } else {
        return ImmutablePair.of(null, index + 1);
      }
    } else if (node instanceof SqlBasicCall) {
      final SqlBasicCall call = (SqlBasicCall) node;
      switch (call.getOperator().getName().toUpperCase()) {
        case "AND":
        case "OR":
          final Pair<SqlNode, Integer> left =
              activateFutures(futures, call.getOperands()[0], index);
          final Pair<SqlNode, Integer> right =
              activateFutures(futures, call.getOperands()[1], left.getRight());
          final int newIndex = Math.max(left.getRight(), right.getRight());

          if (left.getLeft() == null) {
            return ImmutablePair.of(right.getLeft(), newIndex);
          } else if (right.getLeft() == null) {
            return ImmutablePair.of(left.getLeft(), newIndex);
          } else {
            return ImmutablePair.of(
                new SqlBasicCall(
                    call.getOperator(),
                    new SqlNode[]{left.getLeft(), right.getLeft()},
                    SqlParserPos.ZERO),
                newIndex);
          }
        default:
          return ImmutablePair.of(
              new SqlBasicCall(
                  SqlStdOperatorTable.EQUALS,
                  new SqlNode[]{
                      SqlUtils.getIdentifier(getMetaFieldName(index, MetaField.FUTURE_WHERE)),
                      SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)
                  },
                  SqlParserPos.ZERO),
              index + 1);
      }
    } else {
      throw new IllegalArgumentException("type not supported: " + node);
    }
  }

  private List<Integer> getFutureGroupIndices(
      Function<SqlIdentifier, Integer> fieldMapper,
      List<MetaField> metaFields,
      SqlNodeList withFutureGroupBy) {
    final List<Integer> indices = new ArrayList<>();

    if (withFutureGroupBy != null) {
      for (SqlNode group : withFutureGroupBy) {
        final int index =
            getField(fieldMapper, group, metaFields, EnumSet.of(MetaField.FUTURE_GROUP))
                .getMiddle();
        indices.add(index);
      }
    }

    return indices;
  }

  private SqlNode getBufferField(JdbcBufferContext context, SqlIdentifier field, boolean addAlias) {
    final int index = context.getFieldIndex(field);
    return getBufferField(context, index, addAlias);
  }

  private SqlNode getBufferField(JdbcBufferContext context, int index, boolean addAlias) {
    final String fieldName = getMetaFieldName(index, context.getMetaFields().get(index));
    SqlNode node = SqlUtils.getIdentifier(fieldName);
    if (addAlias) {
      node = SqlUtils.getAlias(node, context.getFieldName(index));
    }
    return node;
  }

  private Triple<SqlIdentifier, Integer, MetaField> getField(
      Function<SqlIdentifier, Integer> fieldMapper,
      SqlNode node,
      List<MetaField> metaFields,
      EnumSet<MetaField> valid) {
    if (!(node instanceof SqlIdentifier)) {
      throw new IllegalArgumentException("field is not an identifier: " + node);
    }

    final int index = fieldMapper.apply((SqlIdentifier) node);
    if (index < 0) {
      throw new IllegalArgumentException("field not found: " + node);
    }

    final MetaField metaField = metaFields.get(index);
    if (valid != null) {
      if (!valid.contains(metaField)) {
        throw new IllegalArgumentException(
            "field is invalid: " + metaField + " - must be one of: " + valid);
      }
    }

    return ImmutableTriple.of((SqlIdentifier) node, index, metaField);
  }

  private List<SqlIdentifier> getFieldNames(SqlNodeList selectList) {
    final List<SqlIdentifier> fieldNames = new ArrayList<>(selectList.size());
    for (SqlNode select : selectList) {
      SqlIdentifier identifier = null;
      if (select instanceof SqlBasicCall) {
        SqlBasicCall call = (SqlBasicCall) select;

        if (SqlStdOperatorTable.AS.equals(call.getOperator())) {
          select = call.operands[1];
        } else {
          identifier = SqlUtils.getIdentifier(call.getOperator().getName());
        }
      } else if (select instanceof SqlFutureNode) {
        SqlFutureNode futureNode = (SqlFutureNode) select;
        select = futureNode.getNode();
      }

      if (identifier == null) {
        if (!(select instanceof SqlIdentifier)) {
          throw new IllegalArgumentException("future in select must contain an identifier");
        }
        identifier = (SqlIdentifier) select;
      }
      fieldNames.add(identifier);
    }
    return fieldNames;
  }

  private List<String> getBufferFieldNames(List<MetaField> metaFields) {
    final List<String> fieldNames = new ArrayList<>();
    int i = 0;

    for (MetaField metaField : metaFields) {
      switch (metaField) {
        case AVG:
          fieldNames.add(getMetaFieldName(i, MetaField.SUM));
          fieldNames.add(getMetaFieldName(i++, MetaField.COUNT));
          break;
        case NONE:
        case COUNT:
        case SUM:
        case FUTURE_GROUP:
        case FUTURE_WHERE:
          fieldNames.add(getMetaFieldName(i++, metaField));
          break;
        case PARTITION:
        case PROGRESS:
          i++;
          break;
        case CONFIDENCE_INTERVAL:
          fieldNames.add(getMetaFieldName(i++, metaField));
          break;
        default:
          throw new IllegalArgumentException("metaField not handled: " + metaField);
      }
    }

    return fieldNames;
  }

  private String generateBufferTableName() {
    return "progressive_buffer_" + UUID.randomUUID().toString().replaceAll("-", "_");
  }

  private SqlCreateTable getCreateBuffer(
      ResultSetMetaData metaData,
      List<String> bufferFieldNames,
      String bufferTableName,
      SqlNodeList columnIndexes) {
    SqlNode[] additionalColumns;

    if (columnIndexes.size() > 0) {
      additionalColumns =
          new SqlNode[]{
              SqlDdlNodes.primary(
                  SqlParserPos.ZERO,
                  new SqlIdentifier("pk_" + bufferTableName, SqlParserPos.ZERO),
                  columnIndexes)
          };
    } else {
      additionalColumns = new SqlNode[0];
    }

    return SqlUtils.createTable(
        bufferDriver, metaData, bufferFieldNames, bufferTableName, additionalColumns);
  }

  private SqlSelect getSelectBuffer(
      SqlSelect originalSelect,
      List<String> bufferFieldNames,
      String bufferTableName,
      List<SqlIdentifier> fieldNames,
      List<MetaField> metaFields) {
    return getSelectBuffer(
        originalSelect, bufferFieldNames, bufferTableName, fieldNames, metaFields, null, false);
  }

  private SqlSelect getSelectBuffer(
      SqlSelect originalSelect,
      List<String> bufferFieldNames,
      String bufferTableName,
      List<SqlIdentifier> fieldNames,
      List<MetaField> metaFields,
      SqlNode where,
      boolean view) {
    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    final SqlNodeList groupBy = new SqlNodeList(SqlParserPos.ZERO);

    int i = 0;
    int index = 0;
    for (int j = 0; j < fieldNames.size(); j++) {
      final SqlIdentifier alias = fieldNames.get(j);
      final MetaField metaField = metaFields.get(j);

      SqlNode newColumn = null;

      switch (metaField) {
        case NONE:
          final SqlIdentifier identifier = SqlUtils.getIdentifier(bufferFieldNames.get(i++));
          newColumn = identifier;
          groupBy.add(identifier);
          break;
        case AVG:
          final SqlIdentifier id1 = SqlUtils.getIdentifier(bufferFieldNames.get(i));
          final SqlIdentifier id2 = SqlUtils.getIdentifier(bufferFieldNames.get(i + 1));
          newColumn =
              SqlUtils.createAvgAggregation(
                  view ? SqlUtils.createSumAggregation(id1) : id1,
                  view ? SqlUtils.createSumAggregation(id2) : id2);
          i += 2;
          break;
        case COUNT:
        case SUM:
          newColumn =
              SqlUtils.createSumPercentAggregation(
                  index++, SqlUtils.getIdentifier(bufferFieldNames.get(i++)));
          break;
        case PARTITION:
          newColumn = SqlUtils.createFunctionMetaField(index++, SqlTypeName.INTEGER);
          break;
        case PROGRESS:
          newColumn = SqlUtils.createFunctionMetaField(index++, SqlTypeName.FLOAT);
          break;
        case FUTURE_GROUP:
          // ignore
          i++;
          break;
        case CONFIDENCE_INTERVAL:
          final SqlIdentifier id = SqlUtils.getIdentifier(bufferFieldNames.get(i++));
          newColumn = SqlUtils.createCast(view ? SqlUtils.createSumAggregation(id) : id, SqlTypeName.FLOAT);
          break;
        default:
          throw new IllegalArgumentException("metaField not handled: " + metaField);
      }

      if (newColumn != null) {
        selectList.add(SqlUtils.getAlias(newColumn, alias));
      }
    }

    return new SqlSelect(
        SqlParserPos.ZERO,
        null,
        selectList,
        new SqlIdentifier(bufferTableName, SqlParserPos.ZERO),
        transformWhere(where),
        groupBy.size() == 0 ? null : groupBy,
        originalSelect.getHaving(),
        null,
        originalSelect.getOrderList(),
        null,
        null);
  }

  private SqlNodeList getIndexColumns(List<MetaField> metaFields, boolean hasAggregation) {
    return getIndexColumns(
        metaFields,
        IntStream.range(0, metaFields.size()).boxed().collect(Collectors.toList()),
        hasAggregation);
  }

  private SqlNodeList getIndexColumns(
      List<MetaField> metaFields, List<Integer> indices, boolean hasAggregation) {
    final SqlNodeList indexColumns = new SqlNodeList(SqlParserPos.ZERO);
    for (Integer index : indices) {
      final MetaField metaField = metaFields.get(index);
      if (MetaFieldUtils.isIndex(metaField, hasAggregation)) {
        indexColumns.add(SqlUtils.getIdentifier(getMetaFieldName(index, metaField)));
      }
    }
    return indexColumns;
  }

  private JdbcSelectContext.Builder builder(
      List<String> bufferFieldNames, String bufferTableName, SqlNodeList indexColumns) {
    return new JdbcSelectContext.Builder()
        .insertBuffer(getInsertBuffer(bufferFieldNames, bufferTableName, indexColumns))
        .updateBuffer(getUpdateBuffer(bufferFieldNames, bufferTableName, indexColumns));
  }

  private SqlInsert getInsertBuffer(
      List<String> bufferFieldNames, String bufferTableName, SqlNodeList indexColumns) {
    final int count = bufferFieldNames.size();

    final SqlIdentifier targetTable = new SqlIdentifier(bufferTableName, SqlParserPos.ZERO);
    final SqlNodeList columns = new SqlNodeList(SqlParserPos.ZERO);
    final SqlNode[] insertValues = new SqlNode[count];
    final SqlNodeList updateValues = new SqlNodeList(SqlParserPos.ZERO);

    for (int i = 0; i < count; i++) {
      final SqlNode param = new SqlDynamicParam(count + i, SqlParserPos.ZERO);
      final SqlIdentifier column = new SqlIdentifier(bufferFieldNames.get(i), SqlParserPos.ZERO);

      columns.add(column);
      insertValues[i] = new SqlDynamicParam(i, SqlParserPos.ZERO);

      if (isKey(bufferFieldNames.get(i), indexColumns)) {
        updateValues.add(param);
      } else {
        updateValues.add(
            new SqlBasicCall(
                SqlStdOperatorTable.PLUS, new SqlNode[]{column, param}, SqlParserPos.ZERO));
      }
    }

    if (indexColumns.size() > 0) {
      return new SqlUpsert(
          SqlParserPos.ZERO,
          targetTable,
          columns,
          insertValues,
          indexColumns,
          new SqlUpdate(
              SqlParserPos.ZERO,
              new SqlIdentifier(Collections.emptyList(), SqlParserPos.ZERO),
              columns,
              updateValues,
              null,
              null,
              null));
    } else {
      return new SqlInsert(
          SqlParserPos.ZERO,
          SqlNodeList.EMPTY,
          targetTable,
          SqlUtils.getValues(insertValues),
          columns);
    }
  }

  private boolean isKey(String column, SqlNodeList groups) {
    if (groups == null) {
      return false;
    }

    for (SqlNode group : groups) {
      if (group instanceof SqlIdentifier) {
        if (column.equalsIgnoreCase(((SqlIdentifier) group).getSimple())) {
          return true;
        }
      }
    }
    return false;
  }

  private SqlUpdate getUpdateBuffer(
      List<String> bufferFieldNames, String bufferTableName, SqlNodeList columnIndexes) {
    if (bufferDriver.hasUpsert() && columnIndexes.size() > 0) {
      return null;
    }

    final SqlNodeList columns = new SqlNodeList(SqlParserPos.ZERO);
    final SqlNodeList values = new SqlNodeList(SqlParserPos.ZERO);

    if (columnIndexes.size() != 0) {
      // TODO implement if driver does not support upsert
      throw new IllegalStateException("driver does not support upsert");
    }

    for (int i = 0; i < bufferFieldNames.size(); i++) {
      final SqlNode param = new SqlDynamicParam(i, SqlParserPos.ZERO);
      final SqlIdentifier column = new SqlIdentifier(bufferFieldNames.get(i), SqlParserPos.ZERO);
      columns.add(column);

      if (isKey(bufferFieldNames.get(i), columnIndexes)) {
        values.add(param);
      } else {
        values.add(
            new SqlBasicCall(
                SqlStdOperatorTable.PLUS, new SqlNode[]{column, param}, SqlParserPos.ZERO));
      }
    }

    return new SqlUpdate(
        SqlParserPos.ZERO,
        new SqlIdentifier(bufferTableName, SqlParserPos.ZERO),
        columns,
        values,
        null,
        null,
        null);
  }

  private SqlNode transformWhere(SqlNode oldWhere) {
    if (oldWhere == null) {
      return null;
    }

    return resolveWhereFutures(oldWhere, false);
  }

  private SqlNode resolveWhereFutures(SqlNode node, boolean add) {
    boolean isFuture = false;
    if (node instanceof SqlFutureNode) {
      isFuture = true;
      add = true;

      node = ((SqlFutureNode) node).getNode();
    }

    final SqlBasicCall call = (SqlBasicCall) node;
    switch (call.getOperator().getName()) {
      case "AND": {
        FutureType leftFuture = getFutureType(call.getOperands()[0]);
        FutureType rightFuture = getFutureType(call.getOperands()[1]);

        final SqlNode left =
            resolveWhereFutures(call.getOperands()[0], leftFuture == FutureType.FULL);
        final SqlNode right =
            resolveWhereFutures(call.getOperands()[1], rightFuture == FutureType.FULL);

        if (left == null) {
          return right;
        } else if (right == null) {
          return left;
        } else {
          return new SqlBasicCall(
              SqlStdOperatorTable.AND, new SqlNode[]{left, right}, SqlParserPos.ZERO);
        }
      }
      case "OR": {
        FutureType leftFuture = getFutureType(call.getOperands()[0]);
        FutureType rightFuture = getFutureType(call.getOperands()[1]);
        boolean newAdd = leftFuture == FutureType.FULL || rightFuture == FutureType.FULL;

        final SqlNode left = resolveWhereFutures(call.getOperands()[0], newAdd);
        final SqlNode right = resolveWhereFutures(call.getOperands()[1], newAdd);

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
        if (add) {
          if (isFuture) {
            return new SqlFutureNode(node, SqlParserPos.ZERO);
          } else {
            return call;
          }
        }
    }

    return null;
  }
}
