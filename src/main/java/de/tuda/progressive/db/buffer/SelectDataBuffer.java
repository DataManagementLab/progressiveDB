package de.tuda.progressive.db.buffer;

import de.tuda.progressive.db.statement.context.impl.BaseContext;

import java.sql.ResultSetMetaData;
import java.util.List;

public interface SelectDataBuffer<C extends BaseContext> extends AutoCloseable {
  List<Object[]> get(int partition, double progress);

  ResultSetMetaData getMetaData();

  C getContext();
}
