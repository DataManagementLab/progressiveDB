package de.tuda.progressive.db.driver.impl;

import de.tuda.progressive.db.driver.AbstractDriver;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.exception.ProgressiveException;
import de.tuda.progressive.db.model.ColumnMeta;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;

public class SQLiteDriver extends AbstractDriver {

  public static final DbDriver INSTANCE = new SQLiteDriver();

  public static final SqlDialect SQL_DIALECT =
      new AnsiSqlDialect(SqlDialect.EMPTY_CONTEXT.withIdentifierQuoteString("\""));

  private static final String SELECT_TPL = "select t.*, (rowid %% %d) row_number from %s t";

  private static final String PRAGMA_TPL = "pragma table_info(%s)";

  private static final String INSERT_ALL_TPL = "insert into %s %s";

  private SQLiteDriver() {
  }

  @Override
  public SqlTypeName toSqlType(int jdbcType) {
    switch (jdbcType) {
      case Types.NULL:
      case Types.NUMERIC:
        return SqlTypeName.VARCHAR;
    }
    return null;
  }

  @Override
  protected List<ColumnMeta> getColumnMetas(Connection connection, String table) {
    try (Statement statement = connection.createStatement()) {
      final List<ColumnMeta> columnMetas = new ArrayList<>();
      try (ResultSet result = statement.executeQuery(String.format(PRAGMA_TPL, table))) {
        while (result.next()) {
          columnMetas.add(createColumnMeta(result));
        }
      }
      return columnMetas;
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
  }

  private ColumnMeta createColumnMeta(ResultSet result) throws SQLException {
    final String name = result.getString(2);
    final String type = result.getString(3);
    final Matcher matcher = Pattern.compile("^([^()]+)(\\((.+)\\))?$").matcher(result.getString(3));

    if (!matcher.find()) {
      throw new IllegalArgumentException("cannot handle type: " + type);
    }

    final SqlTypeName sqlType = SqlTypeName.valueOf(matcher.group(1).toUpperCase());
    String innerType = matcher.group(2);
    int precision = 0;
    int scale = 0;

    if (innerType != null) {
      innerType = innerType.substring(1, innerType.length() - 1);
      precision = Integer.parseInt(innerType);
    }

    return new ColumnMeta(name, sqlType.getJdbcOrdinal(), precision, scale);
  }

  @Override
  public String getPartitionTable(String table) {
    return table + PART_COLUMN_NAME;
  }

  @Override
  protected String getSelectTemplate() {
    return SELECT_TPL;
  }

  @Override
  public boolean hasPartitions() {
    return false;
  }

  @Override
  public boolean hasUpsert() {
    return true;
  }

  public static class Builder extends AbstractDriver.Builder<SQLiteDriver, Builder> {

    public Builder() {
      this(SQL_DIALECT);
    }

    public Builder(SqlDialect dialect) {
      super(dialect);
    }

    @Override
    public SQLiteDriver build() {
      return build(new SQLiteDriver());
    }
  }
}
