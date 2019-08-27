package de.tuda.progressive.db.statement.context.impl.jdbc;

import de.tuda.progressive.db.statement.context.MetaField;
import de.tuda.progressive.db.statement.context.impl.JdbcSourceContext;
import de.tuda.progressive.db.util.ContextUtils;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;

public class JdbcBufferContext extends JdbcSourceContext {

  private final List<SqlIdentifier> fieldNames;

  private final SqlSelect selectBuffer;

  public JdbcBufferContext(
      List<MetaField> metaFields,
      Map<Integer, Pair<Integer, Integer>> bounds,
      SqlSelect selectSource,
      List<SqlIdentifier> fieldNames,
      SqlSelect selectBuffer,
      List<SqlIdentifier> sourceTables) {
    super(metaFields, bounds, selectSource, sourceTables);

    this.fieldNames = fieldNames;
    this.selectBuffer = selectBuffer;
  }

  public List<SqlIdentifier> getFieldNames() {
    return fieldNames;
  }

  public SqlIdentifier getFieldName(int index) {
    return fieldNames.get(index);
  }

  public int getFieldIndex(SqlIdentifier fieldName) {
    return ContextUtils.getFieldIndex(fieldNames, fieldName);
  }

  public SqlSelect getSelectBuffer() {
    return selectBuffer;
  }

  protected abstract static class AbstractBuilder<
          C extends JdbcBufferContext, B extends AbstractBuilder<C, B>>
      extends JdbcSourceContext.AbstractBuilder<C, B> {

    private List<SqlIdentifier> fieldNames;

    private SqlSelect selectBuffer;

    public B fieldNames(List<SqlIdentifier> fieldNames) {
      this.fieldNames = fieldNames;
      return (B) this;
    }

    public B selectBuffer(SqlSelect selectBuffer) {
      this.selectBuffer = selectBuffer;
      return (B) this;
    }

    @Override
    protected final C build(
        List<MetaField> metaFields,
        Map<Integer, Pair<Integer, Integer>> bounds,
        SqlSelect selectSource,
        List<SqlIdentifier> sourceTables) {
      return build(metaFields, bounds, selectSource, fieldNames, selectBuffer, sourceTables);
    }

    protected abstract C build(
        List<MetaField> metaFields,
        Map<Integer, Pair<Integer, Integer>> bounds,
        SqlSelect selectSource,
        List<SqlIdentifier> fieldNames,
        SqlSelect selectBuffer,
        List<SqlIdentifier> sourceTables);
  }

  public static class Builder extends AbstractBuilder<JdbcBufferContext, Builder> {
    @Override
    protected JdbcBufferContext build(
        List<MetaField> metaFields,
        Map<Integer, Pair<Integer, Integer>> bounds,
        SqlSelect selectSource,
        List<SqlIdentifier> fieldNames,
        SqlSelect selectBuffer,
        List<SqlIdentifier> sourceTables) {
      return new JdbcBufferContext(
          metaFields, bounds, selectSource, fieldNames, selectBuffer, sourceTables);
    }
  }
}
