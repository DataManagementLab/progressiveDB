package de.tuda.progressive.db.statement.context;

import java.sql.Types;

public enum MetaField {
  NONE(false, false, Types.NULL),
  AVG(false, false, Types.DOUBLE),
  COUNT(false, true, Types.DOUBLE),
  SUM(false, true, Types.DOUBLE),
  FUTURE_GROUP(false, false, Types.NULL),
  FUTURE_WHERE(false, false, Types.NULL),
  PARTITION(true, true, Types.INTEGER),
  PROGRESS(true, true, Types.DOUBLE),
  CONFIDENCE_INTERVAL(true, false, Types.DOUBLE);

  private final boolean function;

  private final boolean substitute;

  private final int sqlType;

  MetaField(boolean function, boolean substitute, int sqlType) {
    this.function = function;
    this.substitute = substitute;
    this.sqlType = sqlType;
  }

  public boolean isFunction() {
    return function;
  }

  public boolean isSubstitute() {
    return substitute;
  }

  public int getSqlType() {
    return sqlType;
  }
}
