package com.jpro.failover.implv2.iter;

import com.jqs.bigdata.rs.overall.doris.impl.Tuple3;

public interface Converter<T> {
    public T convert(Tuple3<Long, Long, Long> every,
                     byte[] readBytes, int start);
}
