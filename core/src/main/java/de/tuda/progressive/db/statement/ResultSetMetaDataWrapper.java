package de.tuda.progressive.db.statement;

import de.tuda.progressive.db.exception.ProgressiveException;
import de.tuda.progressive.db.util.SqlFunction;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class ResultSetMetaDataWrapper implements ResultSetMetaData {

	private final ResultSetMetaData metaData;

	private final int columnCount;

	private final int[] columnTypes;

	private final String[] columnTypeNames;

	private final String[] columnLabels;

	private final String[] columnNames;

	private final String[] tableNames;

	private final String[] schemaNames;

	private final String[] catalogNames;

	private final String[] columnClassNames;

	public ResultSetMetaDataWrapper(ResultSetMetaData metaData) throws SQLException {
		this.metaData = metaData;
		this.columnCount = metaData.getColumnCount();
		this.columnTypes = getColumnTypes(metaData);
		this.columnTypeNames = get(metaData::getColumnTypeName);
		this.columnLabels = get(metaData::getColumnLabel);
		this.columnNames = get(metaData::getColumnName);
		this.tableNames = get(metaData::getTableName);
		this.schemaNames = get(metaData::getSchemaName);
		this.catalogNames = get(metaData::getCatalogName);
		this.columnClassNames = get(metaData::getColumnClassName);
	}

	private int[] getColumnTypes(ResultSetMetaData metaData) {
		try {
			int[] types = new int[metaData.getColumnCount()];
			for (int i = 0; i < types.length; i++) {
				int type = metaData.getColumnType(i + 1);
				types[i] = type == Types.NULL ? Types.OTHER : type;
			}
			return types;
		} catch (SQLException e) {
			throw new ProgressiveException(e);
		}
	}

	private String[] get(SqlFunction<Integer, String> func) {
		try {
			String[] names = new String[metaData.getColumnCount()];
			for (int i = 0; i < names.length; i++) {
				names[i] = func.get(i + 1);
			}
			return names;
		} catch (SQLException e) {
			throw new ProgressiveException(e);
		}
	}

	@Override
	public int getColumnCount() {
		return columnCount;
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		//TODO
		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		//TODO
		return false;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		//TODO
		return false;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		//TODO
		return false;
	}

	@Override
	public int isNullable(int column) throws SQLException {
		//TODO
		return ResultSetMetaData.columnNoNulls;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		//TODO
		return false;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		return metaData.getColumnDisplaySize(column);
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		return columnLabels[column - 1];
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		return columnNames[column - 1];
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		return schemaNames[column - 1];
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		//TODO
		return 0;
	}

	@Override
	public int getScale(int column) throws SQLException {
		//TODO
		return 0;
	}

	@Override
	public String getTableName(int column) throws SQLException {
		return tableNames[column - 1];
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		return catalogNames[column - 1];
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		return columnTypes[column - 1];
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		return columnTypeNames[column - 1];
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		// TODO
		return false;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		// TODO
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		return metaData.isDefinitelyWritable(column);
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		return columnClassNames[column - 1];
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return metaData.unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return metaData.isWrapperFor(iface);
	}
}
