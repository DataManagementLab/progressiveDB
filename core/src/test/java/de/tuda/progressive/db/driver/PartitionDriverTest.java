package de.tuda.progressive.db.driver;

import org.junit.jupiter.api.Test;

public abstract class PartitionDriverTest<
        D extends PartitionDriver, B extends PartitionDriver.Builder<D, B>>
    extends AbstractDriverTest<D, B> {

  private void testPrepare(int partitionSize, int joinTable) {
    testPrepare(getDriver().hasPartitions(true), partitionSize, joinTable);
  }

  @Test
  void testPrepare1Partitions() {
    testPrepare(1, 0);
  }

  @Test
  void testPrepare2Partitions() {
    testPrepare(2, 0);
  }

  @Test
  void testPrepare1PartitionsJoin1() {
    testPrepare(1, 1);
  }

  @Test
  void testPrepare2PartitionsJoin1() {
    testPrepare(2, 1);
  }

  @Test
  void testPrepare1PartitionsJoin2() {
    testPrepare(1, 2);
  }

  @Test
  void testPrepare2PartitionsJoin2() {
    testPrepare(2, 2);
  }
}
