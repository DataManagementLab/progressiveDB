package de.tuda.progressive.db.util;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Litmus;

import java.util.List;

public class ContextUtils {

  private ContextUtils() {}

  public static int getFieldIndex(List<SqlIdentifier> fields, SqlIdentifier field) {
    for (int i = 0; i < fields.size(); i++) {
      if (field.equalsDeep(fields.get(i), Litmus.IGNORE)) {
        return i;
      }
    }
    return -1;
  }
}
