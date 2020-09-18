package com.jpro.failover.implv2;

import com.jqs.bigdata.rs.overall.doris.impl.Tuple3;
import com.jqs.bigdata.rs.overall.failover.implv2.iter.Converter;
import com.jqs.bigdata.rs.overall.failover.implv2.iter.ReusableMemoryIterator;

public class TrimWriteCache implements Converter<Tuple3<byte[], Integer, Integer>> {
    ReusableMemoryIterator iterator;

    @Override
    public Tuple3<byte[], Integer, Integer> convert(Tuple3<Long, Long, Long> every, byte[] readBytes, int start) {
        return null;
    }
}
