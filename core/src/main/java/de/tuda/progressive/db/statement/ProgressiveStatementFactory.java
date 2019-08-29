package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlDropProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlPrepareTable;
import de.tuda.progressive.db.sql.parser.SqlSelectProgressive;
import java.sql.Connection;

public interface ProgressiveStatementFactory {

  ProgressiveStatement prepare(Connection connection, SqlSelectProgressive select);

  ProgressiveStatement prepare(
      Connection connection, SqlCreateProgressiveView createProgressiveView);

  ProgressiveStatement prepare(Connection connection, SqlDropProgressiveView dropProgressiveView);

  ProgressiveStatement prepare(Connection connection, SqlPrepareTable prepare);
}
