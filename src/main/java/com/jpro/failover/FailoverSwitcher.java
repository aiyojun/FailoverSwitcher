package com.jpro.failover;

import com.jpro.failover.impl.IndexerPartition;
import com.jpro.failover.impl.Pair;
import com.jpro.failover.impl.PartitionIterator;

import java.io.File;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class FailoverSwitcher {
    private long limit;
    private long batchSize;
    private long currentMaximumUid = 0;
    private ExecutorService asyncLoop;
    private File filesDir;

    /**
     * [Core api]
     * Constructor of failover.
     */
    public FailoverSwitcher(String dir, long maximum) {
        this(dir, maximum, 100000);
    }

    public FailoverSwitcher(String dir, long maximum, long batchSize) {
        this.batchSize = batchSize;
        this.indexerFilePrefix = "Indexer#" + batchSize + "#";
        asyncLoop = Executors.newFixedThreadPool(2);
        limit = maximum;
        reloadFiles(dir);
    }

    public long getHistoryMaximumUid() { return currentMaximumUid; }

    private ConcurrentHashMap<Long, IndexerPartition> managerTable = new ConcurrentHashMap<>();

    private String indexerFilePrefix = "indexer";

    private final String indexerFilePostfix = "lock";

    /**
     * You can not use this method!
     * Most of time, failover occur sporadically!
     */
    public void close() {
        managerTable.values().forEach(IndexerPartition::close);
    }

    /**
     * [Core api]
     * After restart the system, you can invoke this method at first!
     */
    @Deprecated
    public List<Pair<Long, String>> recover() {
        LinkedList<Pair<Long, String>> res = new LinkedList<>();
        for (Map.Entry<Long, IndexerPartition> group: managerTable.entrySet()) {
            List<Pair<Long, String>> data = group.getValue().recover();
            data.forEach(p -> p.setKey(p.getKey() + group.getKey()));
            res.addAll(data);
        }
        return res;
    }

    /**
     * [Core api] for replacing recover() function
     * Providing iterator to access all of the cached data.
     */
    public PartitionIterator pull() {
        return new PartitionIterator(managerTable);
    }

    private ReentrantLock uidMutex = new ReentrantLock();

    private void updateCurrentMaximumUid(long uid) {
        uidMutex.lock();
        if (uid > currentMaximumUid) currentMaximumUid = uid;
        uidMutex.unlock();
    }

    private ReentrantLock managerMutex = new ReentrantLock();

    /**
     * [Core api]
     * Throw a piece of data into disk immediately!
     */
    public void keep(long uid, String data) {
        if (uid < 1) throw new RuntimeException("[PartitionManager] Non-support zero, start from one!");
        updateCurrentMaximumUid(uid);
        long innerOffset = uid % batchSize;
        long prefix;
        if (innerOffset == 0) {
            prefix = uid - batchSize;
            innerOffset = batchSize;
        } else {
            prefix = uid - innerOffset;
        }
        managerMutex.lock();
        try {
            if (!managerTable.containsKey(prefix)) {
                managerTable.put(prefix, IndexerPartition.create(
                        String.format("%s/%s%010d.%s", filesDir.getAbsolutePath(),
                                indexerFilePrefix, prefix, indexerFilePostfix), prefix, batchSize));
            }
            managerTable.get(prefix).keep(innerOffset, data);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        } finally {
            managerMutex.unlock();
        }
    }

    /**
     * async method, means it won't take your time!
     */
    private void asyncCleanWorker() {
        CompletableFuture.supplyAsync(() -> { clean(); return 0; }, asyncLoop);
    }

    /**
     * [Core api]
     * Like the mechanism of 'ACK'
     * After you send 'ack' to me, i won't keep caching the data.
     */
    public void release(long uid) {
        long innerOffset = uid % batchSize;
        long prefix;
        if (innerOffset == 0) {
            prefix = uid - batchSize;
            innerOffset = batchSize;
        } else {
            prefix = uid - innerOffset;
        }
        if (managerTable.containsKey(prefix)) {
            managerTable.get(prefix).release(innerOffset);
        }
        asyncCleanWorker();
    }

    AtomicBoolean isOccupying = new AtomicBoolean(false);

    /**
     * [Inner method]
     * Remove the useless data which received 'ack' signal.
     */
    private void clean() {
        if (isOccupying.get()) return;
        isOccupying.set(true);
        long minimumUid = currentMaximumUid - limit;
        long minimumUidBelongTo = minimumUid - minimumUid % batchSize;
        HashMap<Long, Character> co0 = new HashMap<>();
        while (co0.size() < managerTable.size()) {
            List<Long> keysStates = Collections.list(managerTable.keys());
            keysStates.sort(Long::compareTo);
            if (keysStates.isEmpty()) return;
            for (int i = 0; i < keysStates.size(); i++) {
                long target = keysStates.get(i);
                if (co0.containsKey(target)) continue;
                managerTable.get(target).trim();
                co0.put(target, '0');
                break;
            }
        }
        List<Long> co = new ArrayList<>();
        for (Map.Entry<Long, IndexerPartition> every : managerTable.entrySet()) {
            every.getValue().trim();
            if (every.getKey() < minimumUidBelongTo) co.add(every.getKey());
        }
        co.forEach(k -> {managerTable.get(k).destroy(); managerTable.remove(k);});
        isOccupying.set(false);
    }

    private void reloadFiles(String dir) {
        filesDir = new File(dir);
        if (!filesDir.isDirectory()) {
            throw new RuntimeException(String.format("[PartitionManager] (%s) isn't directory!", dir));
        }
        File[] files = filesDir.listFiles((fp, name) ->
                name.contains(indexerFilePrefix) && name.endsWith(indexerFilePostfix));
        if (files == null) return;
        long currentMaxUid = 0;
        for (File file : files) {
            String fileName = file.getName();
            String[] fii = fileName.split("[.]");
            if (fii.length == 0) {
                if (!file.delete()) {}
                continue;
            }
            if (!fii[fii.length - 1].equals(indexerFilePostfix)
                    || fii.length != 2
                    || !fileName.substring(0, indexerFilePrefix.length()).equals(indexerFilePrefix)) {
                if (!file.delete()) {}
                continue;
            }
            if (!fileName.contains(indexerFilePrefix) || !fileName.contains(indexerFilePostfix)) {
                continue;
            }
            fii = fileName.split("#");
            if (fii.length == 3 && "indexer".equals(fii[0]) && Long.parseLong(fii[1]) == batchSize) {} else continue;
            String indexer_s = fileName.substring(indexerFilePrefix.length(),
                    fileName.length() - indexerFilePostfix.length() - 1);
            long indexer;
            try {
                indexer = Long.parseLong(indexer_s);
            } catch (Exception e) {
                continue;
            }
            managerTable.put(indexer, IndexerPartition.reload(file.getPath(), batchSize));
            long maxUid = indexer + managerTable.get(indexer).getHistoryMaximumUid();
            if (maxUid > currentMaxUid) { currentMaxUid = maxUid; }
        }
        currentMaximumUid = currentMaxUid;
        clean();
    }

    public void show() {
        for (Map.Entry<Long, IndexerPartition> every : managerTable.entrySet()) {
            every.getValue().recordState(every.getKey());
        }
    }
}
