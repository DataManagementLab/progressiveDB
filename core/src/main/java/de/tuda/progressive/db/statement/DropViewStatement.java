package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.exception.ProgressiveException;
import de.tuda.progressive.db.util.SqlUtils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import org.apache.calcite.sql.ddl.SqlDropTable;

public class DropViewStatement implements ProgressiveStatement {

  private final DbDriver driver;

  private final Connection connection;

  private final String view;

  private final boolean ifExists;

  private boolean finished;

  public DropViewStatement(DbDriver driver, Connection connection, String view, boolean ifExists) {
    this.driver = driver;
    this.connection = connection;
    this.view = view;
    this.ifExists = ifExists;
  }

  @Override
  public ResultSet getResultSet() {
    return new ProgressiveResultSet(null, new LinkedList<>());
  }

  @Override
  public ResultSetMetaData getMetaData() {
    return null;
  }

  @Override
  public boolean isDone() {
    return finished;
  }

  @Override
  public void run() {
    try (Statement statement = connection.createStatement()) {
      final SqlDropTable dropView = SqlUtils.dropTable(view, ifExists);
      final String sql = driver.toSql(dropView);
      statement.execute(sql);
      finished = true;
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public boolean closeWithStatement() {
    return false;
  }
}
