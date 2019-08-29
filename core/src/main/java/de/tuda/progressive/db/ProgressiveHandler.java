package de.tuda.progressive.db;

import de.tuda.progressive.db.buffer.DataBufferFactory;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.meta.MetaData;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlDropProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlPrepareTable;
import de.tuda.progressive.db.sql.parser.SqlSelectProgressive;
import de.tuda.progressive.db.statement.ProgressiveStatement;
import de.tuda.progressive.db.statement.ProgressiveStatementFactory;
import de.tuda.progressive.db.statement.SimpleStatementFactory;
import de.tuda.progressive.db.statement.context.impl.BaseContextFactory;
import java.sql.Connection;

public class ProgressiveHandler {

  private final ProgressiveStatementFactory statementFactory;

  public ProgressiveHandler(
      DbDriver driver,
      MetaData metaData,
      BaseContextFactory contextFactory,
      DataBufferFactory dataBufferFactory) {
    this.statementFactory =
        new SimpleStatementFactory(driver, metaData, contextFactory, dataBufferFactory);
  }

  public ProgressiveStatement handle(Connection connection, SqlPrepareTable prepareTable) {
    return statementFactory.prepare(connection, prepareTable);
  }

  public ProgressiveStatement handle(Connection connection,
      SqlCreateProgressiveView createProgressiveView) {
    return statementFactory.prepare(connection, createProgressiveView);
  }

  public ProgressiveStatement handle(Connection connection,
      SqlDropProgressiveView dropProgressiveView) {
    return statementFactory.prepare(connection, dropProgressiveView);
  }

  public ProgressiveStatement handle(Connection connection, SqlSelectProgressive select) {
    return statementFactory.prepare(connection, select);
  }
}
