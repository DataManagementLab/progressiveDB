package de.tuda.progressive.db.driver;

import de.tuda.progressive.db.meta.MetaData;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Connection;

public interface DbDriver {

  String toSql(SqlNode node);

  SqlTypeName toSqlType(int jdbcType);

  void prepareTable(Connection connection, String table, MetaData metaData);

  String getPartitionTable(String table);

  String getPartitionTable(String table, long partition);

  boolean hasUpsert();

  boolean hasPartitions();
}
