package de.tuda.progressive.db.driver;

import de.tuda.progressive.db.meta.MetaData;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Connection;

public interface DbDriver {

  /**
   * Serializes the given node to a driver specific SQL string that can be executed by the database
   * @param node The node to be serialized
   * @return Executable String for the database
   */
  String toSql(SqlNode node);

  /**
   * If ProgessiveDB is not able to map the given jdbcType ({@link java.sql.Types}) to a calcite type ({@link SqlTypeName})
   * then the driver is responsible for the mapping. Usually ProgressiveDB is able to do the mapping on its own, an exception
   * would be SQLite, since it often does not provide relevant information about specific colums via JDBC
   * @param jdbcType See {@link java.sql.Types} for values that are passed as parameter
   * @return The mapped type by the driver
   */
  SqlTypeName toSqlType(int jdbcType);

  /**
   * Prepare the given {@code table} for progressive execution and store meta data about the partition sizes and columns in {@code metaData}
   * @param connection Use the given database connection
   * @param table Table to be prepared to be queried progressively. If table has foreign tables prepare them as well
   * @param metaData
   */
  void prepareTable(Connection connection, String table, MetaData metaData);

  /**
   * Return the name of the prepared table
   * @param table The original name
   * @return The name of the prepared table
   */
  String getPartitionTable(String table);

  /**
   * Return the name of the prepared table for the specific partition
   * @param table The original name
   * @return The name of the prepared table
   */
  String getPartitionTable(String table, long partition);

  /**
   * If database supports {@code INSERT INTO ... ON DUPLICATE KEY UPDATE ...} syntax
   * @return {@code True} if syntax is supported, {@code false} otherwise
   */
  boolean hasUpsert();

  /**
   * Datbase supports table partitioning
   * @return {@code True} if table partitioning is supported, {@code false} otherwise
   */
  boolean hasPartitions();
}
