package de.tuda.progressive.db.util;

import java.sql.SQLException;

@FunctionalInterface
public interface SqlFunction<I, O> {
	O get(I value) throws SQLException;
}
