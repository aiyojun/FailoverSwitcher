package com.jpro.failover.implv2.iter;

import com.jqs.bigdata.rs.overall.doris.impl.Tuple3;
import com.jqs.bigdata.rs.overall.failover.impl.IndexerPartition;
import lombok.extern.log4j.Log4j2;

import java.util.Iterator;
import java.util.List;

@Log4j2
public class ReusableMemoryIterator<T> implements Iterator<T> {
    protected IndexerPartition partition;
    protected int volumeToApply;  // < 2G

    protected List<Tuple3<Long, Long, Long>>
            keptData;
    protected int keptDataIndexOffset = 0;

    protected int readFileCursorOffset;

//    protected List<T> nextBatchDataCache
//            = new LinkedList<>();

    protected Converter<T> nextDataConverter;

    public ReusableMemoryIterator() {

    }

    protected int findNextEndIndex() {
        Tuple3<Long, Long, Long> first =
                keptData.get(keptDataIndexOffset);
        readFileCursorOffset = first._2().intValue();
        int end = keptDataIndexOffset;
        // pick process
        for (int index = keptDataIndexOffset;
             index < keptData.size(); index++) {
            Tuple3<Long, Long, Long> every = keptData.get(index);
            if (every._2() + every._3() - readFileCursorOffset
                > volumeToApply) break;
            end++;
        }
        if (end == keptDataIndexOffset) {
            String err = String.format(
                "Pick failed! picked index: %d, cursor: %d, keptData size: %d",
                end, keptDataIndexOffset, keptData.size());
            log.error(err);
            throw new RuntimeException(err);
        }
        return 9;
    }

    protected void updateReadDataOffset(
            int keptDataNewBegin,
            int readFileNewBegin) {
        keptDataIndexOffset = keptDataNewBegin;
        readFileCursorOffset = readFileNewBegin;
    }

    public boolean hasNextBatch() {
        return keptDataIndexOffset < keptData.size();
    }

    protected void loadNextBatch() {

    }

    @Override
    public boolean hasNext() {
        return false;
    }

    public void setConverter(Converter<T> converter) {
        nextDataConverter = converter;
    }

    @Override
    public T next() {
        return null;
    }
}
