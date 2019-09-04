package de.tuda.progressive.db;

import de.tuda.progressive.db.exception.ProgressiveException;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlDropProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlParserImpl;
import de.tuda.progressive.db.sql.parser.SqlPrepareTable;
import de.tuda.progressive.db.sql.parser.SqlSelectProgressive;
import de.tuda.progressive.db.statement.ProgressiveStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.jdbc.StatementInfo;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProgressiveMeta extends JdbcMeta {

  private static final Logger log = LoggerFactory.getLogger(ProgressiveMeta.class);

  private static final Pattern DDL_PATTERN = Pattern
      .compile("^(create|alter|drop)(?!\\s*progressive)", Pattern.CASE_INSENSITIVE);

  private final ConcurrentMap<Integer, ProgressiveStatement> statements = new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, ProgressiveStatement> durableStatements =
      new ConcurrentHashMap<>();

  private final ProgressiveHandler progressiveHandler;

  private final SqlParser.Config progressiveParserConfig =
      SqlParser.configBuilder()
          .setCaseSensitive(true)
          .setParserFactory(SqlParserImpl.FACTORY)
          .build();

  private final SqlParser.Config ddlParserConfig =
      SqlParser.configBuilder()
          .setCaseSensitive(true)
          .setParserFactory(SqlDdlParserImpl.FACTORY)
          .build();

  public ProgressiveMeta(String url, ProgressiveHandler progressiveHandler) throws SQLException {
    this(url, new Properties(), progressiveHandler);
  }

  public ProgressiveMeta(
      String url, String user, String password, ProgressiveHandler progressiveHandler)
      throws SQLException {
    this(
        url,
        new Properties() {
          {
            put("user", user);
            put("password", password);
          }
        },
        progressiveHandler);
  }

  public ProgressiveMeta(String url, Properties info, ProgressiveHandler progressiveHandler)
      throws SQLException {
    this(url, info, NoopMetricsSystem.getInstance(), progressiveHandler);
  }

  public ProgressiveMeta(
      String url, Properties info, MetricsSystem metrics, ProgressiveHandler progressiveHandler)
      throws SQLException {
    super(url, info, metrics);

    this.progressiveHandler = progressiveHandler;

    init();
  }

  private void init() {
    // warm up parser
    try {
      SqlParser.create("select * from dual", progressiveParserConfig).parseStmt();
      SqlParser.create("select * from dual", ddlParserConfig).parseStmt();
    } catch (SqlParseException e) {
      // do nothing
    }
  }

  @Override
  public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
    return prepareProgressiveStatement(
        ch.id,
        sql,
        statement -> {
          try {
            StatementHandle handle =
                new StatementHandle(
                    ch.id,
                    getStatementIdGenerator().getAndIncrement(),
                    signature(statement.getMetaData()));
            addStatement(handle.id, statement);

            return handle;
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        })
        .orElseGet(() -> super.prepare(ch, sql, maxRowCount));
  }

  @Override
  public StatementHandle createStatement(ConnectionHandle ch) {
    return super.createStatement(ch);
  }

  @Override
  public ExecuteResult prepareAndExecute(
      StatementHandle h,
      String sql,
      long maxRowCount,
      int maxRowsInFirstFrame,
      PrepareCallback callback)
      throws NoSuchStatementException {
    ExecuteResult result = hookPrepareAndExecute(h, sql, maxRowCount, maxRowsInFirstFrame);
    if (result != null) {
      return result;
    }

    return super.prepareAndExecute(h, sql, maxRowCount, maxRowsInFirstFrame, callback);
  }

  @Override
  public ExecuteResult execute(
      StatementHandle h, List<TypedValue> parameterValues, long maxRowCount)
      throws NoSuchStatementException {
    return super.execute(h, parameterValues, maxRowCount);
  }

  @Override
  public ExecuteResult execute(
      StatementHandle h, List<TypedValue> parameterValues, int maxRowsInFirstFrame)
      throws NoSuchStatementException {
    ProgressiveStatement statement = statements.get(h.id);
    if (statement == null) {
      return super.execute(h, parameterValues, maxRowsInFirstFrame);
    }

    return execute(h, statement);
  }

  @Override
  public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount)
      throws NoSuchStatementException, MissingResultsException {
    final ProgressiveStatement statement = statements.get(h.id);
    if (statement == null) {
      return super.fetch(h, offset, fetchMaxRowCount);
    }

    log.info("fetch");

    try {
      return createFrame(offset, statement.isDone(), statement.getResultSet());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void closeStatement(StatementHandle h) {
    final ProgressiveStatement statement = statements.get(h.id);
    if (statement == null) {
      super.closeStatement(h);
    } else {
      statement.close();
      statements.remove(h.id);
    }
  }

  @Override
  public boolean syncResults(StatementHandle h, QueryState state, long offset)
      throws NoSuchStatementException {
    final ProgressiveStatement statement = statements.get(h.id);
    if (statement == null) {
      return super.syncResults(h, state, offset);
    } else {
      return false;
    }
  }

  private void assertStatementExists(StatementHandle h) throws NoSuchStatementException {
    final StatementInfo info = getStatementCache().getIfPresent(h.id);
    if (info == null) {
      throw new NoSuchStatementException(h);
    }
  }

  private void addStatement(int handleId, ProgressiveStatement statement) {
    if (statement.closeWithStatement()) {
      statements.putIfAbsent(handleId, statement);
    } else {
      durableStatements.putIfAbsent(handleId, statement);
    }
  }

  private ExecuteResult hookPrepareAndExecute(
      StatementHandle h, String sql, long maxRowCount, int maxRowsInFirstFrame)
      throws NoSuchStatementException {
    assertStatementExists(h);

    return prepareProgressiveStatement(
        h.connectionId,
        sql,
        statement -> {
          addStatement(h.id, statement);
          return execute(h, statement);
        })
        .orElse(null);
  }

  private ExecuteResult execute(StatementHandle h, ProgressiveStatement statement) {
    statement.run();

    try {
      Frame frame = Frame.create(0, false, Collections.emptyList());

      MetaResultSet result =
          MetaResultSet.create(
              h.connectionId, h.id, false, signature(statement.getMetaData()), frame);
      return new ExecuteResult(Collections.singletonList(result));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private Frame createFrame(long offset, boolean done, ResultSet resultSet) throws SQLException {
    if (resultSet == null) {
      return Frame.create(offset, true, Collections.emptyList());
    }

    final List<Object> rows = new ArrayList<>();

    if (resultSet.getMetaData() != null) {
      final int columnCount = resultSet.getMetaData().getColumnCount();

      while (resultSet.next()) {
        Object[] columns = new Object[columnCount];
        for (int i = 0; i < columnCount; i++) {
          columns[i] = resultSet.getObject(i + 1);
        }
        rows.add(columns);
        offset++;
      }
    }

    log.info("send data back");

    return Frame.create(offset, done, rows);
  }

  private <T> Optional<T> prepareProgressiveStatement(
      String connectionId,
      String sql, Function<ProgressiveStatement, T> success) {
    SqlNode node = parse(sql);
    ProgressiveStatement statement;

    if (node instanceof SqlOrderBy) {
      final SqlOrderBy orderBy = (SqlOrderBy) node;
      if (orderBy.query instanceof SqlSelectProgressive) {
        final SqlSelectProgressive select = (SqlSelectProgressive) orderBy.query;
        select.setOrderBy(orderBy.orderList);
        node = select;
      }
    }

    final Connection connection = getConnectionSafe(connectionId);

    if (node instanceof SqlPrepareTable) {
      statement = progressiveHandler.handle(connection, (SqlPrepareTable) node);
    } else if (node instanceof SqlCreateProgressiveView) {
      statement = progressiveHandler.handle(connection, (SqlCreateProgressiveView) node);
    } else if (node instanceof SqlDropProgressiveView) {
      statement = progressiveHandler.handle(connection, (SqlDropProgressiveView) node);
    } else if (node instanceof SqlSelectProgressive) {
      statement = progressiveHandler.handle(connection, (SqlSelectProgressive) node);
    } else {
      return Optional.empty();
    }

    return Optional.of(success.apply(statement));
  }

  private Connection getConnectionSafe(String connectionId) {
    try {
      return getConnection(connectionId);
    } catch (SQLException e) {
      throw new ProgressiveException(e);
    }
  }

  private SqlNode parse(String sql) {
    final SqlParser.Config parserConfig =
        DDL_PATTERN.matcher(sql).find() ? ddlParserConfig : progressiveParserConfig;

    try {
      return SqlParser.create(sql, parserConfig).parseStmt();
    } catch (SqlParseException e) {
      throw new ProgressiveException(e);
    }
  }
}
