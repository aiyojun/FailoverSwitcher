package com.jpro.failover.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class PartitionIterator implements Iterator<Pair<Long, String>> {
    private ConcurrentHashMap<Long, IndexerPartition> managerTable;
    private ArrayList<TravelIterator> iterators;
    private List<Long> indexes;
    private TravelIterator iter;
    private int cursor = 0;
    private List<Long> failedUid;

    public PartitionIterator(ConcurrentHashMap<Long, IndexerPartition> managerTable) {
        this.managerTable = managerTable;
        iterators = new ArrayList<>(managerTable.size());
        indexes = Collections.list(managerTable.keys());
        if (indexes.isEmpty()) { iter = null; } else {
            indexes.sort(Long::compare);
            iterators.add(managerTable.get(indexes.get(cursor)).pull());
            List<Long> tp = iterators.get(iterators.size() - 1).getFailedUid();
            failedUid = tp.stream().map(p -> p += indexes.get(cursor)).collect(Collectors.toList());
            if (iterators.size() > 0) iter = iterators.get(cursor);
        }
    }

    public List<Long> getFailedUid() {
        return failedUid;
    }

    @Override
    public boolean hasNext() {
        if (iter == null) return false;
        if (iter.hasNext()) {
            return true;
        } else {
            if (cursor + 1 < indexes.size()) {
                iterators.add(managerTable.get(indexes.get(cursor + 1)).pull());
                List<Long> tp = iterators.get(iterators.size() - 1).getFailedUid();
                tp = tp.stream().map(p -> p += indexes.get(cursor + 1)).collect(Collectors.toList());
                if (failedUid == null) { failedUid = tp; } else { failedUid.addAll(tp); }
                iter = iterators.get(cursor + 1);
                cursor++;
                return hasNext();
            } else {
                return false;
            }
        }
    }

    @Override
    public Pair<Long, String> next() {
        return iter.next();
    }
}
