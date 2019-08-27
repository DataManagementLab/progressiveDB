package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.buffer.DataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.PartitionInfo;
import de.tuda.progressive.db.statement.context.impl.JdbcSourceContext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

public class ProgressiveViewStatement extends ProgressiveBaseStatement {

  private final Set<ProgressiveListener> listeners = new HashSet<>();

  private final ResultSet resultSet;

  public ProgressiveViewStatement(
      DbDriver driver,
      Connection connection,
      JdbcSourceContext context,
      DataBuffer dataBuffer,
      PartitionInfo partitionInfo) {
    super(driver, connection, context, dataBuffer, partitionInfo);

    this.resultSet = new ProgressiveResultSet(metaData, new LinkedList<>());
  }

  @Override
  protected synchronized void queryHandled() {
    listeners.forEach(ProgressiveListener::handle);
  }

  @Override
  public synchronized boolean isDone() {
    return true;
  }

  @Override
  public synchronized ResultSet getResultSet() {
    return resultSet;
  }

  public synchronized void addListener(ProgressiveListener listener) {
    listeners.add(listener);

    if (getProgress() > 0) {
      listener.handle();
    }
  }

  public synchronized void removeListener(ProgressiveListener listener) {
    listeners.remove(listener);
  }

  @Override
  public boolean closeWithStatement() {
    return false;
  }
}
