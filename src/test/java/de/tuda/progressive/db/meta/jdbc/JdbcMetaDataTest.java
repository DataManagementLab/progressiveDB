package de.tuda.progressive.db.meta.jdbc;

import de.tuda.progressive.db.meta.MetaData;
import de.tuda.progressive.db.model.Column;
import de.tuda.progressive.db.model.Partition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JdbcMetaDataTest {

	private static final String SQLITE_DB_PATH = "mat-test.sqlite";

	private static final String SRC_TABLE_NAME = "test";

	private MetaData metaData;

	@BeforeEach
	void beforeEach() {
		metaData = new JdbcMetaData(String.format("jdbc:sqlite:%s", SQLITE_DB_PATH));
	}

	@AfterEach
	void afterEach() {
		new File(SQLITE_DB_PATH).delete();
	}

	private void assertEqualPartitions(Partition expected, Partition actual) {
		assertEquals(expected.getSrcTable(), actual.getSrcTable());
		assertEquals(expected.getTableName(), actual.getTableName());
		assertEquals(expected.getId(), actual.getId());
		assertEquals(expected.getEntries(), actual.getEntries());
	}

	private void assertEqualColumns(Column expected, Column actual) {
		assertEquals(expected.getTable(), actual.getTable());
		assertEquals(expected.getName(), actual.getName());
		assertEquals(expected.getMin(), actual.getMin());
		assertEquals(expected.getMax(), actual.getMax());
	}

	private List<Partition> createPartitions(long... entries) {
		final List<Partition> partitions = new ArrayList<>();
		for (int i = 0; i < entries.length; i++) {
			Partition partition = new Partition();
			partition.setSrcTable(SRC_TABLE_NAME);
			partition.setTableName(SRC_TABLE_NAME + "_" + i);
			partition.setId(i);
			partition.setEntries(entries[i]);
			partitions.add(partition);
		}
		return partitions;
	}

	private void testAddAndGetPartitions(long... entries) {
		final List<Partition> expectedPartitions = createPartitions(entries);

		metaData.add(expectedPartitions, Collections.emptyList());
		final List<Partition> actualPartitions = metaData.getPartitions(SRC_TABLE_NAME);

		assertEquals(expectedPartitions.size(), actualPartitions.size());
		for (int i = 0; i < expectedPartitions.size(); i++) {
			assertEqualPartitions(expectedPartitions.get(i), actualPartitions.get(i));
		}
	}

	private Column createColumn(String name, long min, long max) {
		return new Column(SRC_TABLE_NAME, name, min, max);
	}

	private void testAddAndGetPartitions(Column... columns) {
		final List<Column> expectedColumns = Arrays.asList(columns);
		metaData.add(Collections.emptyList(), expectedColumns);

		expectedColumns.forEach(expected -> {
			final Column actual = metaData.getColumn(SRC_TABLE_NAME, expected.getName());

			assertEqualColumns(expected, actual);
		});
	}

	@Test
	void addAndGetPartition() {
		testAddAndGetPartitions(0);
	}

	@Test
	void addAndGetPartitions() {
		testAddAndGetPartitions(0, 1, 2);
	}

	@Test
	void addAndGetColumn() {
		final Column column = createColumn("a", 0, 1);

		testAddAndGetPartitions(column);
	}

	@Test
	void addAndGetColumns() {
		final Column column = createColumn("a", 0, 1);
		final Column column2 = createColumn("b", 2, 4);

		testAddAndGetPartitions(column, column2);
	}
}