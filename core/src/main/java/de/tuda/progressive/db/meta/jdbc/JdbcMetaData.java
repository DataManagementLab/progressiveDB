package de.tuda.progressive.db.meta.jdbc;

import de.tuda.progressive.db.meta.MetaData;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.model.Partition;
import org.sql2o.Connection;
import org.sql2o.Sql2o;
import org.sql2o.Sql2oException;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class JdbcMetaData implements MetaData {

  private static final String PARTITIONS_TABLE_NAME = "partitions";
  private static final String COLUMNS_TABLE_NAME = "columns";

  private static final String CREATE_PARTITIONS_TABLE =
      String.format(
          "create table if not exists %s\n"
              + "(\n"
              + "srcTable varchar(1000),\n"
              + "tableName varchar(1000),\n"
              + "id int,\n"
              + "entries int not null,\n"
              + "fact int(1) not null,\n"
              + "primary key (srcTable, tableName, id)\n"
              + ");\n"
              + "\n",
          PARTITIONS_TABLE_NAME);

  private static final String CREATE_COLUMNS_TABLE =
      String.format(
          "create table if not exists %s\n"
              + "(\n"
              + "`table` varchar(100),\n"
              + "`name` varchar(100),\n"
              + "`min` int,\n"
              + "`max` int,\n"
              + "primary key (`table`, `name`)\n"
              + ");\n"
              + "\n",
          COLUMNS_TABLE_NAME);

  private static final String GET_PARTITIONS =
      String.format(
          "select * from %s where srcTable = :srcTable order by id", PARTITIONS_TABLE_NAME);
  private static final String ADD_PARTITION =
      String.format(
          "insert into %s (srcTable, tableName, id, entries, fact) values (:srcTable, :tableName, :id, :entries, :fact)",
          PARTITIONS_TABLE_NAME);
  private static final String ADD_COLUMN =
      String.format(
          "insert into %s (`table`, `name`, `min`, `max`) values (:table, :name, :min, :max)",
          COLUMNS_TABLE_NAME);
  private static final String GET_COLUMN =
      String.format(
          "select * from %s where `table` = :table and `name` like :name", COLUMNS_TABLE_NAME);

  private final Sql2o sql2o;

  public JdbcMetaData(String url) {
    this(url, null, null);
  }

  public JdbcMetaData(String url, String user, String password) {
    sql2o = new Sql2o(url, user, password);

    createTables();
  }

  public JdbcMetaData(String url, Properties properties) {
    this(url, properties.getProperty("user", null), properties.getProperty("password", null));
  }

  public JdbcMetaData(DataSource dataSource) {
    sql2o = new Sql2o(dataSource);

    createTables();
  }

  private void createTables() {
    try (Connection connection = sql2o.open()) {
      connection.createQuery(CREATE_PARTITIONS_TABLE).executeUpdate();
      connection.createQuery(CREATE_COLUMNS_TABLE).executeUpdate();
    }
  }

  @Override
  public void add(List<Partition> partitions, List<Column> columns) {
    try (Connection con = sql2o.open()) {
      con.getJdbcConnection().setAutoCommit(false);

      for (Partition partition : partitions) {
        con.createQuery(ADD_PARTITION)
            .addParameter("srcTable", partition.getSrcTable())
            .addParameter("tableName", partition.getTableName())
            .addParameter("id", partition.getId())
            .addParameter("entries", partition.getEntries())
            .addParameter("fact", partition.isFact())
            .executeUpdate();
      }

      for (Column column : columns) {
        try {
          con.createQuery(ADD_COLUMN)
              .addParameter("table", column.getTable())
              .addParameter("name", column.getName())
              .addParameter("min", column.getMin())
              .addParameter("max", column.getMax())
              .executeUpdate();
        } catch (Sql2oException e) {
          throw e;
        }
      }

      con.commit();
    } catch (SQLException e) {
      throw new Sql2oException(e);
    }
  }

  @Override
  public List<Partition> getPartitions(String table) {
    try (Connection con = sql2o.open()) {
      return con.createQuery(GET_PARTITIONS)
          .addParameter("srcTable", table)
          .executeAndFetch(Partition.class);
    }
  }

  @Override
  public Column getColumn(String table, String column) {
    try (Connection con = sql2o.open()) {
      final List<Column> columns = con.createQuery(GET_COLUMN)
          .addParameter("table", table)
          .addParameter("name", column)
          .executeAndFetch(Column.class);

      if (columns.size() == 1) {
        return columns.get(0);
      } else {
        return columns.stream()
            .filter(c -> c.getName().equals(column))
            .findFirst()
            .orElse(null);
      }
     }
  }
}
