package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.buffer.SelectDataBuffer;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public interface ProgressiveStatement extends AutoCloseable {

  ResultSet getResultSet();

  ResultSetMetaData getMetaData();

  boolean isDone();

  void run();

  void close();

  boolean closeWithStatement();
}
