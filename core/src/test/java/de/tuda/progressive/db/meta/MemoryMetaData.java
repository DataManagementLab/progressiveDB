package de.tuda.progressive.db.meta;

import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.model.Partition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemoryMetaData implements MetaData {

	private Map<String, List<Partition>> partitions = new HashMap<>();

	private Map<String, Map<String, Column>> columns = new HashMap<>();

	@Override
	public void add(List<Partition> partitions, List<Column> columns) {
		addPartitions(partitions);
		addColumns(columns);
	}

	private void addPartitions(List<Partition> newPartitions) {
		for (Partition partition : newPartitions) {
			List<Partition> partitionList = partitions.computeIfAbsent(partition.getSrcTable(), k -> new ArrayList<>());
			partitionList.add(partition);
		}
	}

	private void addColumns(List<Column> newColumns) {
		for (Column column : newColumns) {
			Map<String, Column> columnMap = columns.computeIfAbsent(column.getTable(), k -> new HashMap<>());
			columnMap.put(column.getName(), column);
		}
	}

	@Override
	public List<Partition> getPartitions(String table) {
		return partitions.get(table);
	}

	@Override
	public Column getColumn(String table, String column) {
		return columns.getOrDefault(table, Collections.emptyMap()).get(column);
	}
}
