package de.tuda.progressive.db.sql.parser;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlDropProgressiveViewTest {

	private SqlParser.ConfigBuilder configBuilder;

	@BeforeEach
	void init() {
		configBuilder = SqlParser.configBuilder()
				.setUnquotedCasing(Casing.UNCHANGED)
				.setParserFactory(SqlParserImpl.FACTORY);
	}

	void testSimple(boolean ifExists) throws Exception {
		final String name = "a";

		SqlParser parser = SqlParser.create(String.format(
				"drop progressive view %s %s", ifExists ? "if exists" : "", name
		), configBuilder.build());
		SqlNode node = parser.parseStmt();

		assertEquals(SqlDropProgressiveView.class, node.getClass());

		SqlDropProgressiveView dropProgressiveView = (SqlDropProgressiveView) node;

		assertEquals(name, dropProgressiveView.getName().getSimple());
		assertEquals(ifExists, dropProgressiveView.isIfExists());

		System.out.println(node);
	}

	@Test
	void testSimple() throws Exception {
		testSimple(false);
	}

	@Test
	void testSimpleIfExists() throws Exception {
		testSimple(true);
	}
}