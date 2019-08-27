package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.buffer.DataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.model.PartitionInfo;
import de.tuda.progressive.db.statement.context.impl.JdbcSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ProgressiveSelectStatement extends ProgressiveBaseStatement {

  private static final Logger log = LoggerFactory.getLogger(ProgressiveSelectStatement.class);

  private List<Object[]> results = new ArrayList<>();

  public ProgressiveSelectStatement(
      DbDriver driver,
      Connection connection,
      JdbcSourceContext context,
      DataBuffer dataBuffer,
      PartitionInfo partitionInfo) {
    super(driver, connection, context, dataBuffer, partitionInfo);
  }

  @Override
  protected synchronized void queryHandled() {
    log.info("run cache query");

    List<Object[]> rows = dataBuffer.get(getReadPartitions(), getProgress());
    results.addAll(rows);

    log.info("cache results received");

    notify();
  }

  @Override
  public synchronized ResultSet getResultSet() {
    if (results.isEmpty() && !isDone()) {
      try {
        wait();
      } catch (InterruptedException e) {
        // do nothing
      }
    }

    ResultSet resultSet = new ProgressiveResultSet(metaData, new LinkedList<>(results));
    results.clear();
    return resultSet;
  }

  @Override
  public synchronized void close() {
    super.close();

    notify();
  }

  @Override
  public boolean closeWithStatement() {
    return true;
  }
}
