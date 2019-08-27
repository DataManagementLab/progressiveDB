package de.tuda.progressive.db.statement.context;

import de.tuda.progressive.db.buffer.DataBuffer;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlSelectProgressive;
import de.tuda.progressive.db.statement.context.impl.BaseContext;
import de.tuda.progressive.db.statement.context.impl.JdbcSourceContext;
import org.apache.calcite.sql.SqlIdentifier;

import java.sql.Connection;
import java.util.function.Function;

public interface ContextFactory<
    C1 extends JdbcSourceContext, C2 extends BaseContext, D extends DataBuffer> {

  C1 create(
      Connection connection,
      SqlSelectProgressive select,
      Function<SqlIdentifier, Column> columnMapper);

  C2 create(
      D dataBuffer, SqlSelectProgressive select, Function<SqlIdentifier, Column> columnMapper);

  C1 create(
      Connection connection,
      SqlCreateProgressiveView view,
      Function<SqlIdentifier, Column> columnMapper);
}
