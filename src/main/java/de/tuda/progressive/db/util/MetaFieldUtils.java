package de.tuda.progressive.db.util;

import de.tuda.progressive.db.statement.context.MetaField;

import java.util.List;

public class MetaFieldUtils {

  private MetaFieldUtils() {}

  public static boolean isIndex(MetaField metaField, boolean hasAggregation) {
    switch (metaField) {
      case NONE:
      case FUTURE_GROUP:
        return true;
      case FUTURE_WHERE:
        return hasAggregation;
      default:
        return false;
    }
  }

  public static boolean hasIndex(List<MetaField> metaFields, boolean hasAggregation) {
    for (MetaField metaField : metaFields) {
      if (isIndex(metaField, hasAggregation)) {
        return true;
      }
    }
    return false;
  }

  public static boolean hasAggregation(List<MetaField> metaFields) {
    for (MetaField metaField : metaFields) {
      switch (metaField) {
        case AVG:
        case COUNT:
        case SUM:
        case CONFIDENCE_INTERVAL:
          return true;
        case NONE:
        case PARTITION:
        case PROGRESS:
        case FUTURE_GROUP:
        case FUTURE_WHERE:
          // ignore
          break;
        default:
          throw new IllegalArgumentException("metaField not supported: " + metaField);
      }
    }
    return false;
  }
}
