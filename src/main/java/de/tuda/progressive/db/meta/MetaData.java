package de.tuda.progressive.db.meta;

import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.model.Partition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public interface MetaData {

  void add(List<Partition> partitions, List<Column> columns);

  List<Partition> getPartitions(String table);

  Column getColumn(String table, String column);

  default Column getColumn(Pair<String, String> key) {
    return getColumn(key.getLeft(), key.getRight());
  }

  default Column getColumn(SqlIdentifier identifier) {
    if (identifier.names.size() > 2) {
      throw new IllegalArgumentException("at must 2 names are supported: " + identifier);
    }
    if (identifier.names.size() == 1) {
      return getColumn(null, identifier.names.get(0));
    }
    return getColumn(identifier.names.get(0), identifier.names.get(1));
  }
}
