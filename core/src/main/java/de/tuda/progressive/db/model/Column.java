package de.tuda.progressive.db.model;

public class Column {

	private String table;

	private String name;

	private long min;

	private long max;

	public Column() {
	}

	public Column(String table, String name, long min, long max) {
		this.table = table;
		this.name = name;
		this.min = min;
		this.max = max;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getMin() {
		return min;
	}

	public void setMin(long min) {
		this.min = min;
	}

	public long getMax() {
		return max;
	}

	public void setMax(long max) {
		this.max = max;
	}
}
