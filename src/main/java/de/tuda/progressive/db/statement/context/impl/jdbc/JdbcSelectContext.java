package de.tuda.progressive.db.statement.context.impl.jdbc;

import de.tuda.progressive.db.statement.context.MetaField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;

public class JdbcSelectContext extends JdbcBufferContext {

  private final SqlCreateTable createBuffer;

  private final SqlInsert insertBuffer;

  private final SqlUpdate updateBuffer;

  private final boolean prepareSelect;

  public JdbcSelectContext(
      List<MetaField> metaFields,
      Map<Integer, Pair<Integer, Integer>> bounds,
      SqlSelect selectSource,
      List<SqlIdentifier> fieldNames,
      SqlCreateTable createBuffer,
      SqlInsert insertBuffer,
      SqlUpdate updateBuffer,
      SqlSelect selectBuffer,
      List<SqlIdentifier> sourceTables,
      boolean prepareSelect) {
    super(metaFields, bounds, selectSource, fieldNames, selectBuffer, sourceTables);

    this.createBuffer = createBuffer;
    this.insertBuffer = insertBuffer;
    this.updateBuffer = updateBuffer;
    this.prepareSelect = prepareSelect;
  }

  public SqlCreateTable getCreateBuffer() {
    return createBuffer;
  }

  public SqlInsert getInsertBuffer() {
    return insertBuffer;
  }

  public SqlUpdate getUpdateBuffer() {
    return updateBuffer;
  }

  public boolean isPrepareSelect() {
    return prepareSelect;
  }

  public static final class Builder
      extends JdbcBufferContext.AbstractBuilder<JdbcSelectContext, Builder> {
    private SqlCreateTable createBuffer;

    private SqlInsert insertBuffer;

    private SqlUpdate updateBuffer;

    private boolean prepareSelect = true;

    public Builder createBuffer(SqlCreateTable createBuffer) {
      this.createBuffer = createBuffer;
      return this;
    }

    public Builder insertBuffer(SqlInsert insertBuffer) {
      this.insertBuffer = insertBuffer;
      return this;
    }

    public Builder updateBuffer(SqlUpdate updateBuffer) {
      this.updateBuffer = updateBuffer;
      return this;
    }

    public Builder prepareSelect(boolean prepareSelect) {
      this.prepareSelect = prepareSelect;
      return this;
    }

    @Override
    protected final JdbcSelectContext build(
        List<MetaField> metaFields,
        Map<Integer, Pair<Integer, Integer>> bounds,
        SqlSelect selectSource,
        List<SqlIdentifier> fieldNames,
        SqlSelect selectBuffer,
        List<SqlIdentifier> sourceTables) {
      return new JdbcSelectContext(
          metaFields,
          bounds,
          selectSource,
          fieldNames,
          createBuffer,
          insertBuffer,
          updateBuffer,
          selectBuffer,
          sourceTables,
          prepareSelect);
    }
  }
}
