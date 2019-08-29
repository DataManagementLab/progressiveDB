package de.tuda.progressive.db.buffer;

import de.tuda.progressive.db.statement.context.impl.BaseContext;

import java.sql.ResultSet;

public interface DataBuffer<C extends BaseContext> extends SelectDataBuffer<C> {
  void add(ResultSet result);
}
