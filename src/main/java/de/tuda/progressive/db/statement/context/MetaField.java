package de.tuda.progressive.db.statement.context;

public enum MetaField {
  NONE(false, false),
  AVG(false, false),
  COUNT(false, true),
  SUM(false, true),
  FUTURE_GROUP(false, false),
  FUTURE_WHERE(false, false),
  PARTITION(true, true),
  PROGRESS(true, true),
  CONFIDENCE_INTERVAL(true, false);

  private final boolean function;

  private final boolean substitute;

  MetaField(boolean function, boolean substitute) {
    this.function = function;
    this.substitute = substitute;
  }

  public boolean isFunction() {
    return function;
  }

  public boolean isSubstitute() {
    return substitute;
  }
}
