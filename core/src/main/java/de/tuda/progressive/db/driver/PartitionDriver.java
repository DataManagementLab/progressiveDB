package de.tuda.progressive.db.driver;

import de.tuda.progressive.db.exception.ProgressiveException;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class PartitionDriver extends AbstractDriver {

  protected static final SqlNode PART_COLUMN =
      SqlUtils.createColumn(PART_COLUMN_NAME, SqlTypeName.INTEGER, 8, 0);

  private static final String INSERT_ALL_TPL = "insert into %s %s";

  private boolean hasPartitions;

  @Override
  protected void createPartitions(Connection connection, String table, long partitions) {
    if (hasPartitions()) {
      dropTable(connection, getPartitionTable(table));
      createPartitionTable(connection, table, partitions);
    } else {
      super.createPartitions(connection, table, partitions);
    }
  }

  protected abstract void createPartitionTable(Connection connection, String table, long partitions);

  @Override
  protected long getPartitionEntries(Connection connection, String table, int partition) {
    if (hasPartitions()) {
      return getCount(connection, getPartitionTable(table), partition);
    } else {
      return super.getPartitionEntries(connection, table, partition);
    }
  }

  private long getCount(Connection connection, String table, int partition) {
    final SqlNode where =
        new SqlBasicCall(
            SqlStdOperatorTable.EQUALS,
            new SqlNode[] {
              SqlUtils.getIdentifier(PART_COLUMN_NAME),
              SqlLiteral.createExactNumeric(String.valueOf(partition), SqlParserPos.ZERO)
            },
            SqlParserPos.ZERO);

    return getCount(connection, table, where);
  }

  @Override
  protected void insertData(Connection connection, String table, long partitions) {
    if (hasPartitions()) {
      final String template = String.format(getSelectTemplate(), partitions, table);
      insertData(connection, template, getPartitionTable(table));
    } else {
      super.insertData(connection, table, partitions);
    }
  }

  private void insertData(Connection connection, String template, String targetTable) {
    try (Statement statement = connection.createStatement()) {
      final String sql = String.format(INSERT_ALL_TPL, targetTable, template);

      statement.execute(sql);
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
  }

  @Override
  public boolean hasPartitions() {
    return hasPartitions;
  }

  public abstract static class Builder<D extends PartitionDriver, B extends Builder<D, B>>
      extends AbstractDriver.Builder<D, B> {
    private boolean hasPartitions;

    public Builder(SqlDialect dialect) {
      super(dialect);
    }

    public B hasPartitions(boolean hasPartitions) {
      this.hasPartitions = hasPartitions;
      return (B) this;
    }

    @Override
    protected D build(D driver) {
      ((PartitionDriver) driver).hasPartitions = hasPartitions;
      return super.build(driver);
    }
  }
}
