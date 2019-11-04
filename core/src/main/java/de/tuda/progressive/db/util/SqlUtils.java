package de.tuda.progressive.db.util;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.exception.ProgressiveException;
import de.tuda.progressive.db.model.ColumnMeta;
import de.tuda.progressive.db.statement.context.MetaField;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Litmus;

import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class SqlUtils {

  private static final RelDataTypeFactory typeFactory =
      new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  private SqlUtils() {
  }

  public static SqlCreateTable createTable(DbDriver driver, List<ColumnMeta> columnMetas,
      String cacheTableName,
      SqlNode... additionalColumns) {
    final SqlNodeList columns = new SqlNodeList(SqlParserPos.ZERO);
    for (ColumnMeta meta : columnMetas) {
      columns.add(getColumnOfType(driver, meta.getName(), meta.getSqlType(), meta.getPrecision(),
          meta.getScale()));
    }
    return createTable(columns, cacheTableName, additionalColumns);
  }

  /**
   * Create the statement to create a table based of the structure of an existing table
   * @param driver The database driver
   * @param metaData MetaData of the original table
   * @param fieldNames Names of the columns, if null is passed as parameter, the original name will be taken
   * @param tableName New table name
   * @param additionalColumns Additional columns that can be added
   * @return The statement to create a table
   */
  public static SqlCreateTable createTable(
      DbDriver driver,
      ResultSetMetaData metaData,
      List<String> fieldNames,
      String tableName,
      SqlNode... additionalColumns) {
    final SqlNodeList columns = new SqlNodeList(SqlParserPos.ZERO);

    try {
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        final String name = fieldNames == null ? metaData.getColumnName(i) : fieldNames.get(i - 1);
        final int columnType = metaData.getColumnType(i);
        final int precision = metaData.getPrecision(i);
        final int scale = metaData.getScale(i);

        columns.add(getColumnOfType(driver, name, columnType, precision, scale));
      }
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }

    return createTable(columns, tableName, additionalColumns);
  }

  public static SqlCreateTable createTable(SqlNodeList columns, String cacheTableName,
      SqlNode... additionalColumns) {
    for (SqlNode column : additionalColumns) {
      columns.add(column);
    }

    return SqlDdlNodes.createTable(
        SqlParserPos.ZERO,
        false,
        false,
        new SqlIdentifier(cacheTableName, SqlParserPos.ZERO),
        columns,
        null);
  }

  public static SqlNodeList getColumns(ResultSetMetaData metaData) {
    final SqlNodeList columns = new SqlNodeList(SqlParserPos.ZERO);
    try {
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        columns.add(getColumnIdentifier(metaData, i));
      }
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
    return columns;
  }

  private static SqlNode getColumnOfType(
      DbDriver driver, String name, int type, int precision, int scale) {
    SqlTypeName sqlType;

    switch (type) {
      case Types.DECIMAL:
        sqlType = SqlTypeName.DECIMAL;
        break;
      case Types.INTEGER:
        sqlType = SqlTypeName.INTEGER;
        break;
      case Types.BIGINT:
        sqlType = SqlTypeName.BIGINT;
        break;
      case Types.VARCHAR:
        sqlType = SqlTypeName.VARCHAR;
        break;
      default:
        sqlType = driver.toSqlType(type);
        if (sqlType == null) {
          throw new IllegalArgumentException("type not supported: " + type);
        }
    }

    return createColumn(name, sqlType, precision, scale);
  }

  public static SqlNode createColumn(String name, SqlTypeName type, int precision, int scale) {
    return SqlDdlNodes.column(
        SqlParserPos.ZERO,
        new SqlIdentifier(name, SqlParserPos.ZERO),
        new SqlDataTypeSpec(
            new SqlIdentifier(type.name(), SqlParserPos.ZERO),
            precision,
            scale > 0 ? scale : -1,
            null,
            null,
            SqlParserPos.ZERO),
        null,
        null);
  }

  public static SqlDropTable dropTable(String table) {
    return dropTable(table, true);
  }

  public static SqlDropTable dropTable(String table, boolean ifExists) {
    return SqlDdlNodes.dropTable(
        SqlParserPos.ZERO, ifExists, new SqlIdentifier(table, SqlParserPos.ZERO));
  }

  public static void closeSafe(AutoCloseable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (Exception e) {
        // do nothing
      }
    }
  }

  public static <T> Consumer<T> consumer(SqlConsumer<T> consumer) {
    return value -> {
      try {
        consumer.accept(value);
      } catch (SQLException e) {
        throw new ProgressiveException(e);
      }
    };
  }

  public static SqlDataTypeSpec getDataType(SqlTypeName typeName) {
    RelDataType dataType = typeFactory.createSqlType(typeName);
    return SqlTypeUtil.convertTypeToSpec(dataType);
  }

  public static void setScale(
      PreparedStatement statement, List<MetaField> metaFields, double progress) {
    try {
      int pos = 1;
      for (MetaField metaField : metaFields) {
        switch (metaField) {
          case COUNT:
          case SUM:
            statement.setDouble(pos++, progress);
            break;
          case NONE:
          case FUTURE_GROUP:
          case FUTURE_WHERE:
          case CONFIDENCE_INTERVAL:
            // do nothing
            break;
          case AVG:
          case PROGRESS:
          case PARTITION:
            pos++;
            break;
          default:
            throw new IllegalStateException("metaField not supported: " + metaField);
        }
      }
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
  }

  public static void setMetaFields(
      PreparedStatement statement,
      Function2<MetaField, Boolean, List<Integer>> posFunction,
      Map<MetaField, Object> values) {
    posFunction
        .apply(MetaField.PARTITION, true)
        .forEach(
            SqlUtils.consumer(
                pos -> {
                  statement.setInt(pos + 1, (int) values.get(MetaField.PARTITION));
                }));

    posFunction
        .apply(MetaField.PROGRESS, true)
        .forEach(
            SqlUtils.consumer(
                pos -> {
                  statement.setDouble(pos + 1, (double) values.get(MetaField.PROGRESS));
                }));
  }

  public static SqlBasicCall createFunctionMetaField(int index, SqlTypeName typeName) {
    return createCast(new SqlDynamicParam(index, SqlParserPos.ZERO), typeName);
  }

  public static SqlBasicCall createCast(SqlNode node, SqlTypeName typeName) {
    return new SqlBasicCall(
        SqlStdOperatorTable.CAST,
        new SqlNode[]{node, SqlUtils.getDataType(typeName)},
        SqlParserPos.ZERO);
  }

  public static SqlBasicCall createAvgAggregation(SqlNode op1, SqlNode op2) {
    return getDivide(createCast(op1, SqlTypeName.FLOAT), createCast(op2, SqlTypeName.FLOAT));
  }

  public static SqlBasicCall createCountAggregation(SqlNode... operands) {
    return new SqlBasicCall(new SqlCountAggFunction("COUNT"), operands, SqlParserPos.ZERO);
  }

  public static SqlBasicCall createCountPercentAggregation(int index, SqlNode operand) {
    return createPercentAggregation(index, createSumAggregation(operand));
  }

  public static SqlBasicCall createSumAggregation(SqlNode... operands) {
    return new SqlBasicCall(new SqlSumAggFunction(null), operands, SqlParserPos.ZERO);
  }

  public static SqlBasicCall createSumPercentAggregation(int index, SqlNode operand) {
    return createPercentAggregation(index, createSumAggregation(operand));
  }

  public static SqlBasicCall createPercentAggregation(int index, SqlNode aggregation) {
    return getDivide(aggregation, new SqlDynamicParam(index, SqlParserPos.ZERO));
  }

  private static SqlBasicCall getDivide(SqlNode op1, SqlNode op2) {
    return new SqlBasicCall(
        SqlStdOperatorTable.DIVIDE, new SqlNode[]{op1, op2}, SqlParserPos.ZERO);
  }

  public static SqlIdentifier getIdentifier(String name) {
    return new SqlIdentifier(name, SqlParserPos.ZERO);
  }

  public static SqlIdentifier getIdentifier(String table, String column) {
    return new SqlIdentifier(Arrays.asList(table, column), SqlParserPos.ZERO);
  }

  public static SqlIdentifier getColumnIdentifier(ResultSetMetaData metaData, int pos) {
    try {
      return new SqlIdentifier(metaData.getColumnName(pos), SqlParserPos.ZERO);
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
  }

  public static SqlBasicCall getValues(SqlNode[] values) {
    return new SqlBasicCall(
        SqlStdOperatorTable.VALUES,
        new SqlNode[]{new SqlBasicCall(new SqlRowOperator(" "), values, SqlParserPos.ZERO)},
        SqlParserPos.ZERO);
  }

  public static SqlNode getAlias(String node, String alias) {
    return getAlias(getIdentifier(node), getIdentifier(alias));
  }

  public static SqlNode getAlias(String node, SqlIdentifier alias) {
    return getAlias(getIdentifier(node), alias);
  }

  public static SqlNode getAlias(SqlNode node, String alias) {
    return getAlias(node, getIdentifier(alias));
  }

  public static SqlNode getAlias(SqlNode node, SqlIdentifier alias) {
    return new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[]{node, alias}, SqlParserPos.ZERO);
  }

  public static boolean contains(SqlNodeList list, SqlNode node) {
    for (SqlNode element : list) {
      if (node.equalsDeep(element, Litmus.IGNORE)) {
        return true;
      }
    }
    return false;
  }
}
