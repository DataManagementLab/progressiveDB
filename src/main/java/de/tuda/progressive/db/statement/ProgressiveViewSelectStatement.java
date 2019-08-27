package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.buffer.SelectDataBuffer;
import de.tuda.progressive.db.util.SqlUtils;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ProgressiveViewSelectStatement
    implements ProgressiveStatement, ProgressiveListener {

  private final ProgressiveViewStatement view;

  private final SelectDataBuffer dataBuffer;

  private List<Object[]> results = new ArrayList<>();

  public ProgressiveViewSelectStatement(
      ProgressiveViewStatement view, SelectDataBuffer dataBuffer) {
    this.view = view;
    this.dataBuffer = dataBuffer;
  }

  @Override
  public synchronized void handle() {
    List<Object[]> rows = dataBuffer.get(view.getReadPartitions(), view.getProgress());
    results.addAll(rows);

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

    ResultSet resultSet = new ProgressiveResultSet(getMetaData(), new LinkedList<>(results));
    results.clear();
    return resultSet;
  }

  @Override
  public ResultSetMetaData getMetaData() {
    return dataBuffer.getMetaData();
  }

  @Override
  public boolean isDone() {
    return !view.hasPartitionsToRead();
  }

  @Override
  public void run() {
    view.addListener(this);
  }

  @Override
  public void close() {
    SqlUtils.closeSafe(dataBuffer);
    view.removeListener(this);
  }

  @Override
  public boolean closeWithStatement() {
    return true;
  }
}
