package de.tuda.progressive.db.model;

public class ColumnMeta {
  final String name;

  final int sqlType;

  final int scale;

  final int precision;

  public ColumnMeta(String name, int sqlType, int scale, int precision) {
    this.name = name;
    this.sqlType = sqlType;
    this.scale = scale;
    this.precision = precision;
  }

  public String getName() {
    return name;
  }

  public int getSqlType() {
    return sqlType;
  }

  public int getScale() {
    return scale;
  }

  public int getPrecision() {
    return precision;
  }
}
