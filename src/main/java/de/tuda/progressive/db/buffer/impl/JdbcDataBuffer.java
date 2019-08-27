package de.tuda.progressive.db.buffer.impl;

import de.tuda.progressive.db.buffer.DataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.exception.ProgressiveException;
import de.tuda.progressive.db.statement.context.impl.jdbc.JdbcSelectContext;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.ddl.SqlCreateTable;

import java.sql.*;

public class JdbcDataBuffer extends JdbcSelectDataBuffer<JdbcSelectContext>
    implements DataBuffer<JdbcSelectContext> {

  private final PreparedStatement insertBuffer;

  private final PreparedStatement updateBuffer;

  public JdbcDataBuffer(DbDriver driver, Connection connection, JdbcSelectContext context) {
    super(
        driver, connection, context, context.isPrepareSelect() ? context.getSelectBuffer() : null);

    this.insertBuffer = prepare(context.getInsertBuffer());
    this.updateBuffer = prepare(context.getUpdateBuffer());
  }

  @Override
  protected void init() {
    createBuffer(context.getCreateBuffer());
  }

  private void createBuffer(SqlCreateTable create) {
    if (create == null) {
      return;
    }

    final String sql = driver.toSql(create);
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
  }

  @Override
  public final void add(ResultSet result) {
    try {
      addInternal(result);
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
  }

  private void addInternal(ResultSet result) throws SQLException {
    final int internalCount = result.getMetaData().getColumnCount();

    if (updateBuffer != null) {
      while (!result.isClosed() && result.next()) {
        // TODO support where
        for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
          updateBuffer.setObject(i, result.getObject(i));
        }

        if (updateBuffer.executeUpdate() == 0) {
          insert(result, internalCount);
        }
      }
    } else {
      while (!result.isClosed() && result.next()) {
        insert(result, internalCount);
      }
    }
  }

  private void insert(ResultSet result, int internalCount) throws SQLException {
    for (int i = 1; i <= internalCount; i++) {
      insertBuffer.setObject(i, result.getObject(i));

      if (context.hasIndex() && driver.hasUpsert()) {
        insertBuffer.setObject(i + internalCount, result.getObject(i));
      }
    }
    insertBuffer.executeUpdate();
  }

  @Override
  public void close() throws Exception {
    SqlUtils.closeSafe(insertBuffer);
    SqlUtils.closeSafe(updateBuffer);
    super.close();
  }

  public Connection getConnection() {
    return connection;
  }
}
