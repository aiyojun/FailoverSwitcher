package com.jpro.failover.impl;

import com.jpro.failover.impl.Tuple3;

public class InnerTravelIterator extends TravelIterator {
    public InnerTravelIterator(IndexerPartition partition, long batchSize, byte[] headers, long size) {
        super(partition, batchSize, headers);
    }

    public boolean hasTheNext() {
        return hasNextBatch();
    }

    public Tuple3<byte[], Integer, Integer> theNext() {
        return nextBatch();
    }
}
