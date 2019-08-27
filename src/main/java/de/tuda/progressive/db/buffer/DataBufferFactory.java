package de.tuda.progressive.db.buffer;

import de.tuda.progressive.db.statement.context.impl.BaseContext;

public interface DataBufferFactory<C1 extends BaseContext, C2 extends BaseContext> {
  DataBuffer create(C1 context);

  SelectDataBuffer create(DataBuffer<C1> dataBuffer, C2 context);
}
