package de.tuda.progressive.db.driver.impl;

import de.tuda.progressive.db.driver.PartitionDriver;
import de.tuda.progressive.db.exception.ProgressiveException;
import de.tuda.progressive.db.util.SqlUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;

public class MySQLDriver extends PartitionDriver {

  private static SqlDialect SQL_DIALECT =
      new MysqlSqlDialect(
          MysqlSqlDialect.EMPTY_CONTEXT.withDatabaseProduct(SqlDialect.DatabaseProduct.MYSQL));

  private static final int PARTITION_SIZE = 500000;

  private static final String PART_DEF =
      String.format("partition by list(%s) (%%s)", PART_COLUMN_NAME);
  private static final String PART_SINGLE_TPL = "partition %s values in (%d)";

  private static final String SELECT_TPL =
      "select t.*, ((@row_number := @row_number + 1) %% %d) row_number from %s t, (select @row_number := 0) rn";

  private MySQLDriver() {
  }

  @Override
  public String toSql(SqlNode node) {
    final String sql = super.toSql(node);

    return sql.replaceAll("BETWEEN ASYMMETRIC", "BETWEEN");
  }

  @Override
  public String getPartitionTable(String table) {
    return table + PART_COLUMN_NAME;
  }

  @Override
  protected void createPartitionTable(Connection connection, String table, long partitions) {
    try (PreparedStatement srcStatement = connection.prepareStatement(toSql(getSelectAll(table)))) {
      final ResultSetMetaData metaData = srcStatement.getMetaData();
      final SqlCreateTable createTable =
          SqlUtils.createTable(this, metaData, null, getPartitionTable(table), PART_COLUMN);
      final String partitionsDef =
          String.format(
              PART_DEF,
              String.join(
                  ", ",
                  LongStream.range(0, partitions)
                      .mapToObj(i -> String.format(PART_SINGLE_TPL, getPartitionTable(table, i), i))
                      .collect(Collectors.joining())));
      final String createTableSql = String.format("%s %s", toSql(createTable), partitionsDef);

      try (Statement destStatement = connection.createStatement()) {
        destStatement.execute(createTableSql);
      }
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
  }

  @Override
  protected String getSelectTemplate() {
    return SELECT_TPL;
  }

  @Override
  public boolean hasUpsert() {
    return true;
  }

  public static class Builder extends PartitionDriver.Builder<MySQLDriver, Builder> {

    public Builder() {
      this(SQL_DIALECT);
    }

    public Builder(SqlDialect dialect) {
      super(dialect);
      partitionSize(PARTITION_SIZE);
      hasPartitions(true);
    }

    @Override
    public MySQLDriver build() {
      return build(new MySQLDriver());
    }
  }
}
