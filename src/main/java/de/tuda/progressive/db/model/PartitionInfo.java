package de.tuda.progressive.db.model;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class PartitionInfo {

  private final String factTable;

  private final Map<String, List<Partition>> partitions;

  private final int partitionCount;

  public PartitionInfo(String factTable, Map<String, List<Partition>> partitions) {
    this.factTable = factTable;
    this.partitions = partitions;
    this.partitionCount = getPartitionsCount(partitions.values());
  }

  private int getPartitionsCount(Collection<List<Partition>> partitions) {
    return partitions.stream().mapToInt(List::size).reduce((a, b) -> a * b).orElse(0);
  }

  public String getFactTable() {
    return factTable;
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public int getPartitionCount(String table) {
    return partitions.get(table).size();
  }

  public List<Partition> getPartitions(String table) {
    return partitions.get(table);
  }
}
