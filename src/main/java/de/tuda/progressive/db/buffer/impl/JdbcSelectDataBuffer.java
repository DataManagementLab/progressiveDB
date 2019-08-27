package de.tuda.progressive.db.buffer.impl;

import de.tuda.progressive.db.buffer.SelectDataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.exception.ProgressiveException;
import de.tuda.progressive.db.statement.ResultSetMetaDataWrapper;
import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.statement.context.impl.BaseContext;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcSelectDataBuffer<C extends BaseContext> implements SelectDataBuffer<C> {

  protected final DbDriver driver;

  protected final Connection connection;

  protected final C context;

  private final PreparedStatement selectBuffer;

  private final ResultSetMetaData metaData;

  public JdbcSelectDataBuffer(DbDriver driver, Connection connection, C context, SqlSelect select) {
    this.driver = driver;
    this.connection = connection;
    this.context = context;

    init();

    this.selectBuffer = prepare(select);
    this.metaData = getMetaData(selectBuffer);
  }

  protected void init() {
    // do nothing
  }

  protected final PreparedStatement prepare(SqlCall call) {
    if (call == null) {
      return null;
    }

    final String sql = driver.toSql(call);
    try {
      return connection.prepareStatement(sql);
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
  }

  protected final ResultSetMetaData getMetaData(PreparedStatement statement) {
    if (statement == null) {
      return null;
    }

    try {
      return new ResultSetMetaDataWrapper(statement.getMetaData());
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
  }

  @Override
  public final List<Object[]> get(int partition, double progress) {
    SqlUtils.setScale(selectBuffer, context.getMetaFields(), progress);
    SqlUtils.setMetaFields(
        selectBuffer,
        context::getFunctionMetaFieldPos,
        new HashMap<MetaField, Object>() {
          {
            put(MetaField.PARTITION, partition);
            put(MetaField.PROGRESS, progress);
          }
        });

    final List<MetaField> metaFields = context.getMetaFields();
    final Map<Integer, Pair<Integer, Integer>> bounds = context.getBounds();
    final List<Object[]> results = new ArrayList<>();

    try (ResultSet resultSet = selectBuffer.executeQuery()) {
      while (resultSet.next()) {
        Object[] row = new Object[resultSet.getMetaData().getColumnCount()];
        for (int i = 1; i <= row.length; i++) {
          if (metaFields.get(i - 1) == MetaField.CONFIDENCE_INTERVAL) {
            final double count = resultSet.getDouble(i);
            final Pair<Integer, Integer> bound = bounds.get(i - 1);
            row[i - 1] =
                ((double) (bound.getRight() - bound.getLeft()))
                    * Math.sqrt(1.0 / (2.0 * count) * Math.log(2.0 / (1.0 - 0.95)));
          } else {
            row[i - 1] = resultSet.getObject(i);
          }
        }
        results.add(row);
      }
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }

    return results;
  }

  @Override
  public ResultSetMetaData getMetaData() {
    return metaData;
  }

  @Override
  public void close() throws Exception {
    SqlUtils.closeSafe(selectBuffer);
  }

  @Override
  public C getContext() {
    return context;
  }

  public Connection getConnection() {
    return connection;
  }
}
