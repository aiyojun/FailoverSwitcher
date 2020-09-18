package com.jpro.failover.implv2.iter;

import com.jqs.bigdata.rs.overall.failover.impl.Pair;

public class RawDataIterator extends ReusableMemoryIterator<Pair<Long, String>> {

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Pair<Long, String> next() {
        return null;
    }
}
