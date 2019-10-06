package de.tuda.progressive.db.driver;

import de.tuda.progressive.db.exception.ProgressiveException;
import de.tuda.progressive.db.meta.MetaData;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.model.ColumnMeta;
import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.util.SqlUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDriver implements DbDriver {

  private static final Logger log = LoggerFactory.getLogger(AbstractDriver.class);

  protected static final String PART_COLUMN_NAME = "_partition";

  private static final String INSERT_PART_TPL =
      "insert into %s select %s from (%s) t where _row_number = %d";

  private SqlDialect dialect;

  private int partitionSize;

  @Override
  public String toSql(SqlNode node) {
    return node.toSqlString(dialect).getSql();
  }

  @Override
  public SqlTypeName toSqlType(int jdbcType) {
    return null;
  }

  @Override
  public void prepareTable(Connection connection, String table, MetaData metaData) {
    final List<Partition> partitions = split(connection, table);
    final List<Column> columns = getColumns(connection, table);

    metaData.add(partitions, columns);
  }

  private List<Partition> split(Connection connection, String table) {
    final List<Partition> partitions = new ArrayList<>();

    for (Map.Entry<String, Long> entry : getPartitionSizes(connection, table).entrySet()) {
      final String currentTable = entry.getKey();
      final long partitionCount = entry.getValue();

      log.info("create {} partitions for table {}", partitionCount, currentTable);
      createPartitions(connection, currentTable, partitionCount);

      log.info("insert data into table {}", currentTable);
      insertData(connection, currentTable, partitionCount);

      log.info("read meta data of table {}", currentTable);
      partitions.addAll(
          getPartitions(connection, currentTable, partitionCount, table.equals(currentTable)));
    }

    return partitions;
  }

  private SortedMap<String, Long> getPartitionSizes(Connection connection, String baseTable) {
    final Set<String> foreignTables = getForeignTables(connection, baseTable);
    final SortedMap<String, Long> partitionSizes = new TreeMap<>();

    long size =
        partitionSize > 0 ? partitionSize : getPartitionSize(connection, baseTable, foreignTables);

    partitionSizes.put(baseTable, getPartitionCount(connection, baseTable, size));
    for (String foreignTable : foreignTables) {
      partitionSizes.put(foreignTable, getPartitionCount(connection, foreignTable, size));
    }

    return partitionSizes;
  }

  protected Set<String> getForeignTables(Connection connection, String baseTable) {
    final Set<String> foreignTables = new HashSet<>();

    try (ResultSet result = connection.getMetaData().getImportedKeys(null, null, baseTable)) {
      while (result.next()) {
        foreignTables.add(result.getString("PKTABLE_NAME"));
      }
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
    return foreignTables;
  }

  protected List<SqlBasicCall> getJoins(Connection connection, String baseTable) {
    final List<SqlBasicCall> joins = new ArrayList<>();

    try (ResultSet result = connection.getMetaData().getImportedKeys(null, null, baseTable)) {
      while (result.next()) {
        final SqlIdentifier fk =
            SqlUtils.getIdentifier(
                result.getString("FKTABLE_NAME"), result.getString("FKCOLUMN_NAME"));
        final SqlIdentifier pk =
            SqlUtils.getIdentifier(
                result.getString("PKTABLE_NAME"), result.getString("PKCOLUMN_NAME"));
        joins.add(
            new SqlBasicCall(
                SqlStdOperatorTable.EQUALS, new SqlNode[]{fk, pk}, SqlParserPos.ZERO));
      }
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
    return joins;
  }

  protected void createPartitions(Connection connection, String table, long partitions) {
    final List<ColumnMeta> columnMetas = getColumnMetas(connection, table);

    try (Statement destStatement = connection.createStatement()) {
      for (int i = 0; i < partitions; i++) {
        final String partitionTable = getPartitionTable(table, i);
        final SqlCreateTable createTable =
            SqlUtils.createTable(this, columnMetas, partitionTable);

        dropTable(connection, partitionTable);
        destStatement.execute(toSql(createTable));
      }
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
  }

  @Override
  public String getPartitionTable(String table, long partition) {
    return String.format("%s_%d", getPartitionTable(table), partition);
  }

  protected void insertData(Connection connection, String table, long partitions) {
    final String template = String.format(getSelectTemplate(), partitions, table);
    insertData(connection, template, table, partitions);
  }

  private void insertData(Connection connection, String template, String table, long partitions) {
    final SqlNodeList columns = getSelectColumns(connection, table);

    try (Statement statement = connection.createStatement()) {
      for (int i = 0; i < partitions; i++) {
        final String sql =
            String.format(
                INSERT_PART_TPL, getPartitionTable(table, i), toSql(columns), template, i);
        statement.execute(sql);
      }
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
  }

  protected abstract String getSelectTemplate();

  private long getPartitionCount(Connection connection, String table, long partitionSize) {
    log.info("get count of partitions of table {} with size {}", table, partitionSize);
    final long count = getCount(connection, table);
    return (long) Math.ceil(((double) count / (double) partitionSize));
  }

  private List<Column> getColumns(Connection connection, String baseTable) {
    final List<Column> columns = new ArrayList<>(getColumnsOfTable(connection, baseTable));

    for (String foreignTable : getForeignTables(connection, baseTable)) {
      columns.addAll(getColumnsOfTable(connection, foreignTable));
    }

    return columns;
  }

  private List<Column> getColumnsOfTable(Connection connection, String table) {
    final List<String> columnNames = getNumericColumnNames(connection, table);

    try (PreparedStatement statement =
        connection.prepareStatement(toSql(getSelectMinMax(table, columnNames)))) {
      try (ResultSet result = statement.executeQuery()) {
        final List<Column> columns = new ArrayList<>();

        result.next();

        for (int i = 0; i < columnNames.size(); i++) {
          final Column column = new Column();
          final int pos = i * 2 + 1;

          column.setTable(table);
          column.setName(columnNames.get(i));
          column.setMin(result.getLong(pos));
          column.setMax(result.getLong(pos + 1));
          columns.add(column);
        }

        return columns;
      }
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
  }

  private SqlNodeList getSelectColumns(Connection connection, String table) {
    final List<SqlIdentifier> identifiers = getColumnMetas(connection, table).stream()
        .map(c -> SqlUtils.getIdentifier(c.getName())).collect(Collectors.toList());
    return new SqlNodeList(identifiers, SqlParserPos.ZERO);
  }

  private List<String> getNumericColumnNames(Connection connection, String table) {
    return getColumnMetas(connection, table).stream().filter(c -> {
      switch (c.getSqlType()) {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
        case Types.FLOAT:
        case Types.DOUBLE:
        case Types.REAL:
          return true;
        default:
          return false;
      }
    }).map(ColumnMeta::getName).collect(Collectors.toList());
  }

  protected List<ColumnMeta> getColumnMetas(Connection connection, String table) {
    try (PreparedStatement statement =
        connection.prepareStatement(toSql(getSelectAll(table)))) {
      final List<ColumnMeta> columnMetas = new ArrayList<>();
      final ResultSetMetaData metaData = statement.getMetaData();
      try {
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
          columnMetas.add(new ColumnMeta(metaData.getColumnName(i), metaData.getColumnType(i),
              metaData.getScale(i), metaData.getPrecision(i)));
        }
      } catch (SQLException e) {
        throw new ProgressiveException(e);
      }
      return columnMetas;
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
  }

  private SqlSelect getSelectMinMax(String table, List<String> columnNames) {
    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    for (String columnName : columnNames) {
      selectList.add(createAggregator(SqlStdOperatorTable.MIN, columnName));
      selectList.add(createAggregator(SqlStdOperatorTable.MAX, columnName));
    }

    return getSelect(selectList, table);
  }

  private SqlBasicCall createAggregator(SqlAggFunction func, String columnName) {
    return new SqlBasicCall(
        func, new SqlNode[]{new SqlIdentifier(columnName, SqlParserPos.ZERO)}, SqlParserPos.ZERO);
  }

  protected final void dropTable(Connection connection, String table) {
    try (Statement statement = connection.createStatement()) {
      final String sql = toSql(SqlUtils.dropTable(table));
      statement.execute(sql);
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
  }

  private List<Partition> getPartitions(
      Connection connection, String table, long partitionCount, boolean isFact) {
    final List<Partition> partitions = new ArrayList<>();
    for (int i = 0; i < partitionCount; i++) {
      final String partitionName = getPartitionTable(table, i);
      final Partition partition = new Partition();
      partition.setSrcTable(table);
      partition.setTableName(partitionName);
      partition.setId(i);
      partition.setEntries(getPartitionEntries(connection, table, i));
      partition.setFact(isFact);
      partitions.add(partition);
    }
    return partitions;
  }

  protected long getPartitionEntries(Connection connection, String table, int partition) {
    return getCount(connection, getPartitionTable(table, partition), null);
  }

  protected final long getCount(Connection connection, String table) {
    return getCount(connection, table, null);
  }

  protected final long getCount(Connection connection, String table, SqlNode where) {
    return getCount(connection, getSelectCount(table, where));
  }

  protected final long getCount(Connection connection, SqlSelect select) {
    try (Statement statement = connection.createStatement()) {
      try (ResultSet result = statement.executeQuery(toSql(select))) {
        result.next();
        return result.getLong(1);
      }
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
  }

  protected final SqlSelect getSelectAll(String table) {
    final SqlNode selectAll = new SqlIdentifier("*", SqlParserPos.ZERO);
    return getSelect(selectAll, table);
  }

  private SqlSelect getSelectCount(String table, SqlNode where) {
    final SqlNode selectCount = createAggregator(SqlStdOperatorTable.COUNT, "*");
    return getSelect(SqlNodeList.of(selectCount), table, where);
  }

  private SqlSelect getSelect(SqlNode singleSelect, String table) {
    return getSelect(SqlNodeList.of(singleSelect), table);
  }

  private SqlSelect getSelect(SqlNodeList selectList, String table) {
    return getSelect(selectList, table, null);
  }

  private SqlSelect getSelect(SqlNodeList selectList, String table, SqlNode where) {
    return new SqlSelect(
        SqlParserPos.ZERO,
        new SqlNodeList(SqlParserPos.ZERO),
        selectList,
        new SqlIdentifier(table, SqlParserPos.ZERO),
        where,
        null,
        null,
        null,
        new SqlNodeList(SqlParserPos.ZERO),
        null,
        null);
  }

  private long getPartitionSize(Connection connection, String baseTable, Set<String> foreignTables) {
    final String aggregationColumn = getAggregationColumn(connection, baseTable);
    final List<SqlIdentifier> groups = new ArrayList<>();
    final int groupLimit = Math.max(5, foreignTables.size());
    SqlNode from = null;
    SqlNode where = null;

    if (foreignTables.size() > 0) {
      for (String table : foreignTables) {
        groups.addAll(getGroupColumnsIdentifiers(connection, table, 1, false));
      }

      int i = 0;
      final List<String> tables = new ArrayList<>(foreignTables);
      while (groups.size() < groupLimit) {
        groups.addAll(getGroupColumnsIdentifiers(connection, tables.get(i), 1, false));
        i %= tables.size();
      }

      for (SqlBasicCall join : getJoins(connection, baseTable)) {
        where =
            where == null
                ? join
                : new SqlBasicCall(
                    SqlStdOperatorTable.AND, new SqlNode[]{where, join}, SqlParserPos.ZERO);
      }
    } else {
      groups.addAll(getGroupColumnsIdentifiers(connection, baseTable, groupLimit, true));
    }

    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    selectList.add(
        SqlStdOperatorTable.AVG.createCall(
            SqlParserPos.ZERO, SqlUtils.getIdentifier(aggregationColumn)));

    long size = 1000000;
    int targetTime = 400;

    for (; ; ) {
      final SqlNode limit =
          SqlNumericLiteral.createExactNumeric(String.valueOf(size), SqlParserPos.ZERO);

      SqlNode source = getSelectAll(baseTable);
      ((SqlSelect) source).setFetch(limit);

      source = SqlUtils.getAlias(source, baseTable);

      for (String table : foreignTables) {
        SqlSelect joinSource = getSelectAll(table);
        joinSource.setFetch(limit);

        source =
            (new SqlJoin(
                SqlParserPos.ZERO,
                source,
                SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                JoinType.COMMA.symbol(SqlParserPos.ZERO),
                SqlUtils.getAlias(joinSource, table),
                JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
                null));
      }

      final SqlSelect select =
          new SqlSelect(
              SqlParserPos.ZERO,
              new SqlNodeList(SqlParserPos.ZERO),
              selectList,
              source,
              where,
              new SqlNodeList(groups, SqlParserPos.ZERO),
              null,
              null,
              new SqlNodeList(SqlParserPos.ZERO),
              null,
              null);

      final String sql = toSql(select);

      int RUNS = 5;
      int time = 0;
      for (int i = 0; i < RUNS; i++) {
        final long start = System.nanoTime();
        try (Statement statement = connection.createStatement()) {
          try (ResultSet result = statement.executeQuery(sql)) {
            result.next();
            final long end = System.nanoTime();
            time += (end - start) / 1000000;
          }
        } catch (SQLException e) {
          throw new ProgressiveException(e);
        }
      }

      time /= RUNS;

      long newSize = (int) (size * ((double) targetTime / (double) time));
      long deviation = newSize / 10;
      long tail = (int) Math.floor(Math.log10(newSize) - 1);
      long leading = (int) (newSize / Math.pow(10, tail));
      newSize = leading * (int) Math.pow(10, tail);

      if (size >= newSize - deviation && size <= newSize + deviation) {
        break;
      }

      size = newSize;
    }

    return size;
  }

  private String getAggregationColumn(Connection connection, String table) {
    final Optional<String> column = getNumericColumnNames(connection, table).stream().findAny();
    if (!column.isPresent()) {
      throw new IllegalArgumentException("no aggregation column found: " + table);
    }
    return column.get();
  }

  private List<SqlIdentifier> getGroupColumnsIdentifiers(
      Connection connection, String table, int limit, boolean numbers) {
    return getGroupColumns(connection, table, limit, numbers).stream()
        .map(SqlUtils::getIdentifier)
        .collect(Collectors.toList());
  }

  // TODO Use getColumnMetas method
  private List<String> getGroupColumns(
      Connection connection, String table, int limit, boolean numbers) {
    try (PreparedStatement statement = connection.prepareStatement(toSql(getSelectAll(table)))) {
      final List<String> columns = new ArrayList<>();
      final ResultSetMetaData metaData = statement.getMetaData();
      if (metaData.getColumnCount() < limit) {
        limit = metaData.getColumnCount();
      }

      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        switch (metaData.getColumnType(i)) {
          case Types.VARCHAR:
          case Types.CHAR:
          case Types.DATE:
          case Types.NCHAR:
          case Types.TIME:
          case Types.TIMESTAMP:
          case Types.LONGNVARCHAR:
          case Types.LONGVARCHAR:
          case Types.NVARCHAR:
          case Types.TIME_WITH_TIMEZONE:
          case Types.TIMESTAMP_WITH_TIMEZONE:
            columns.add(metaData.getColumnName(i));

            if (columns.size() == limit) {
              return columns;
            }
        }
      }

      if (numbers) {
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
          switch (metaData.getColumnType(i)) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.REAL:
              columns.add(metaData.getColumnName(i));

              if (columns.size() == limit) {
                return columns;
              }
          }
        }
      }

      return columns;
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
  }

  public abstract static class Builder<D extends AbstractDriver, B extends Builder<D, B>> {

    private final SqlDialect dialect;

    private int partitionSize = -1;

    public Builder(SqlDialect dialect) {
      this.dialect = dialect;
    }

    public B partitionSize(int partitionSize) {
      this.partitionSize = partitionSize;
      return (B) this;
    }

    public abstract D build();

    protected D build(D driver) {
      ((AbstractDriver) driver).dialect = dialect;
      ((AbstractDriver) driver).partitionSize = partitionSize;
      return driver;
    }
  }
}
