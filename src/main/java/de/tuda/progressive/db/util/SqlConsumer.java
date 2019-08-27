package de.tuda.progressive.db.util;

import java.sql.SQLException;

@FunctionalInterface
public interface SqlConsumer<T> {
	void accept(T value) throws SQLException;
}
