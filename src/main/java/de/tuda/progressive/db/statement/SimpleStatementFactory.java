package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.buffer.DataBuffer;
import de.tuda.progressive.db.buffer.DataBufferFactory;
import de.tuda.progressive.db.buffer.SelectDataBuffer;
import de.tuda.progressive.db.buffer.impl.JdbcDataBuffer;
import de.tuda.progressive.db.driver.DbDriver;
import de.tuda.progressive.db.meta.MetaData;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.model.Partition;
import de.tuda.progressive.db.model.PartitionInfo;
import de.tuda.progressive.db.sql.parser.SqlCreateProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlDropProgressiveView;
import de.tuda.progressive.db.sql.parser.SqlPrepareTable;
import de.tuda.progressive.db.sql.parser.SqlSelectProgressive;
import de.tuda.progressive.db.statement.context.impl.BaseContext;
import de.tuda.progressive.db.statement.context.impl.BaseContextFactory;
import de.tuda.progressive.db.statement.context.impl.JdbcSourceContext;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleStatementFactory implements ProgressiveStatementFactory {

  private static final Logger log = LoggerFactory.getLogger(SimpleStatementFactory.class);

  private final Map<String, ProgressiveViewStatement> viewStatements = new HashMap<>();

  private final DbDriver driver;

  private final MetaData metaData;

  private final Function<SqlIdentifier, Column> columnMapper;

  private final BaseContextFactory contextFactory;

  private final DataBufferFactory dataBufferFactory;

  public SimpleStatementFactory(
      DbDriver driver,
      MetaData metaData,
      BaseContextFactory contextFactory,
      DataBufferFactory dataBufferFactory) {
    this.driver = driver;
    this.metaData = metaData;
    this.columnMapper = metaData::getColumn;
    this.contextFactory = contextFactory;
    this.dataBufferFactory = dataBufferFactory;
  }

  @Override
  public ProgressiveStatement prepare(Connection connection, SqlSelectProgressive select) {
    log.info("prepare select: {}", select);
    assertValid(select);

    final ProgressiveViewStatement viewStatement = getProgressiveView(select);
    final Function<SqlIdentifier, Column> columnMapper = metaData::getColumn;

    if (viewStatement == null) {
      log.info("no view found");

      final PartitionInfo partitionInfo = getJoinInfo(select);
      final JdbcSourceContext context = contextFactory.create(connection, select, columnMapper);
      final DataBuffer dataBuffer = dataBufferFactory.create(context);

      return new ProgressiveSelectStatement(driver, connection, context, dataBuffer, partitionInfo);
    } else {
      log.info("view found");

      final BaseContext context =
          contextFactory.create(viewStatement.getDataBuffer(), select, columnMapper);
      final SelectDataBuffer dataBuffer =
          dataBufferFactory.create(viewStatement.getDataBuffer(), context);
      return new ProgressiveViewSelectStatement(viewStatement, dataBuffer);
    }
  }

  @Override
  public ProgressiveStatement prepare(
      Connection connection, SqlCreateProgressiveView createProgressiveView) {
    log.info("prepare progressive view: {}", createProgressiveView);

    SqlNode query = createProgressiveView.getQuery();

    if (query instanceof SqlOrderBy) {
      final SqlOrderBy orderBy = (SqlOrderBy) query;
      final SqlSelect select = (SqlSelect) orderBy.query;
      select.setOrderBy(orderBy.orderList);

      query = select;
      createProgressiveView =
          new SqlCreateProgressiveView(
              createProgressiveView.getParserPosition(),
              createProgressiveView.getReplace(),
              createProgressiveView.getName(),
              createProgressiveView.getColumnList(),
              query);
    }

    if (!(query instanceof SqlSelect)) {
      throw new IllegalArgumentException("query must be a select: " + query.getClass());
    }

    final SqlSelect select = (SqlSelect) query;
    assertValid(select);

    final SqlIdentifier view = createProgressiveView.getName();
    final String viewName = normalizeViewName(view);
    final JdbcSourceContext context =
        contextFactory.create(connection, createProgressiveView, columnMapper);

    if (viewStatements.containsKey(viewName)) {
      throw new IllegalStateException("view already exists");
    } else {
      log.info("create new view");
      return addViewStatement(connection, context, select, viewName);
    }
  }

  private ProgressiveViewStatement getProgressiveView(SqlSelect select) {
    if (!(select.getFrom() instanceof SqlIdentifier)) {
      return null;
    }

    final SqlIdentifier from = (SqlIdentifier) select.getFrom();
    return viewStatements.get(normalizeViewName(from));
  }

  private PartitionInfo getJoinInfo(SqlSelect select) {
    final Map<String, List<Partition>> partitions = new HashMap<>();
    addPartitions(partitions, select.getFrom());
    return new PartitionInfo(getFactTable(partitions), partitions);
  }

  private void addPartitions(Map<String, List<Partition>> partitions, SqlNode node) {
    if (node instanceof SqlIdentifier) {
      final String name = ((SqlIdentifier) node).getSimple();
      partitions.putIfAbsent(name, metaData.getPartitions(name));
    } else if (node instanceof SqlJoin) {
      final SqlJoin join = (SqlJoin) node;
      addPartitions(partitions, join.getLeft());
      addPartitions(partitions, join.getRight());
    }
  }

  private String getFactTable(Map<String, List<Partition>> partitions) {
    Optional<String> result =
        partitions.entrySet().stream()
            .map(
                entry ->
                    ImmutablePair.of(
                        entry.getKey(), entry.getValue().stream().anyMatch(Partition::isFact)))
            .filter(Pair::getValue)
            .map(Pair::getKey)
            .findFirst();

    if (!result.isPresent()) {
      throw new IllegalArgumentException("no fact table found");
    }

    return result.get();
  }

  private ProgressiveViewStatement addViewStatement(
      Connection connection, JdbcSourceContext context, SqlSelect select, String viewName) {
    final DataBuffer dataBuffer = dataBufferFactory.create(context);
    final PartitionInfo partitionInfo = getJoinInfo(select);
    final ProgressiveViewStatement statement =
        new ProgressiveViewStatement(driver, connection, context, dataBuffer, partitionInfo);

    viewStatements.put(viewName, statement);

    return statement;
  }

  private void assertValid(SqlSelect select) {
    assertValidFrom(select.getFrom());
  }

  private void assertValidFrom(SqlNode from) {
    if (from instanceof SqlSelect) {
      throw new IllegalArgumentException("subqueries are not supported");
    }

    if (from instanceof SqlJoin) {
      final SqlJoin join = (SqlJoin) from;
      switch (join.getJoinType()) {
        case COMMA:
          assertValidFrom(join.getLeft());
          break;
        default:
          throw new IllegalArgumentException("join type not supported: " + join.getJoinType());
      }
    }
  }

  private String normalizeViewName(SqlIdentifier view) {
    return view.getSimple().toUpperCase();
  }

  @Override
  public ProgressiveStatement prepare(Connection connection,
      SqlDropProgressiveView dropProgressiveView) {
    final String viewName = normalizeViewName(dropProgressiveView.getName());

    if (!viewStatements.containsKey(viewName) && !dropProgressiveView.isIfExists()) {
      throw new IllegalArgumentException("view does not exists: " + dropProgressiveView.getName());
    }

    final ProgressiveViewStatement statement = viewStatements.get(viewName);
    if (statement != null) {
      // TODO should not be executed immediately
      statement.close();
      viewStatements.remove(viewName);
    }

    // TODO dont rely on JdbcDataBuffer
    return new DropViewStatement(driver,
        ((JdbcDataBuffer) statement.getDataBuffer()).getConnection(), viewName,
        dropProgressiveView.isIfExists());
  }

  @Override
  public ProgressiveStatement prepare(Connection connection, SqlPrepareTable prepare) {
    return new PrepareStatement(driver, connection, metaData, prepare.getName().getSimple());
  }
}
