package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.meta.MetaData;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.LinkedList;

public class PrepareStatement implements ProgressiveStatement {

  private final DbDriver driver;

  private final Connection connection;

  private final MetaData metaData;

  private final String table;

  private boolean finished;

  public PrepareStatement(DbDriver driver, Connection connection,
      MetaData metaData, String table) {
    this.driver = driver;
    this.connection = connection;
    this.metaData = metaData;
    this.table = table;
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
  public synchronized void run() {
    driver.prepareTable(connection, table, metaData);
    finished = true;
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
