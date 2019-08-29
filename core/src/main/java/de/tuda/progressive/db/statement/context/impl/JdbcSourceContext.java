package de.tuda.progressive.db.statement.context.impl;

import de.tuda.progressive.db.statement.context.MetaField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;

public class JdbcSourceContext extends BaseContext {

  private final SqlSelect selectSource;

  private final List<SqlIdentifier> sourceTables;

  public JdbcSourceContext(
      List<MetaField> metaFields,
      Map<Integer, Pair<Integer, Integer>> bounds,
      SqlSelect selectSource,
      List<SqlIdentifier> sourceTables) {
    super(metaFields, bounds);

    this.selectSource = selectSource;
    this.sourceTables = sourceTables;
  }

  public SqlSelect getSelectSource() {
    return selectSource;
  }

  public List<SqlIdentifier> getSourceTables() {
    return sourceTables;
  }

  public abstract static class AbstractBuilder<
          C extends JdbcSourceContext, B extends AbstractBuilder<C, B>>
      extends BaseContext.Builder<C, B> {
    private SqlSelect selectSource;

    private List<SqlIdentifier> sourceTables;

    public B selectSource(SqlSelect selectBuffer) {
      this.selectSource = selectBuffer;
      return (B) this;
    }

    public B sourceTables(List<SqlIdentifier> sourceTables) {
      this.sourceTables = sourceTables;
      return (B) this;
    }

    @Override
    protected final C build(
        List<MetaField> metaFields, Map<Integer, Pair<Integer, Integer>> bounds) {
      return build(metaFields, bounds, selectSource, sourceTables);
    }

    protected abstract C build(
        List<MetaField> metaFields,
        Map<Integer, Pair<Integer, Integer>> bounds,
        SqlSelect selectSource,
        List<SqlIdentifier> tables);
  }

  public static class Builder extends AbstractBuilder<JdbcSourceContext, Builder> {
    @Override
    protected JdbcSourceContext build(
        List<MetaField> metaFields,
        Map<Integer, Pair<Integer, Integer>> bounds,
        SqlSelect selectSource,
        List<SqlIdentifier> sourceTables) {
      return new JdbcSourceContext(metaFields, bounds, selectSource, sourceTables);
    }
  }
}
