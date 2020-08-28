package com.jpro.failover;

import com.jpro.util.Pair;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        asyncLoop = Executors.newFixedThreadPool(2);
        limit = maximum;
        reloadFiles(dir);
    }

    private ConcurrentHashMap<Long, IndexerPartition> managerTable = new ConcurrentHashMap<>();

    private final String indexerFilePrefix = "indexer";
    private final String indexerFilePost = "lock";

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
    public List<Pair<Long, String>> recover() {
        ArrayList<Pair<Long, String>> res = new ArrayList<>();
        for (Map.Entry<Long, IndexerPartition> group: managerTable.entrySet()) {
            List<Pair<Long, String>> data = group.getValue().recover();
            data.forEach(p -> p.setKey(p.getKey() + group.getKey()));
            res.addAll(data);
        }
        return res;
    }

    /**
     * [Core api]
     * Throw a piece of data into disk immediately!
     */
    public void keep(long uid, String data) {
        if (uid < 1) throw new RuntimeException("[PartitionManager] Non-support zero, start from one!");
        if (uid > currentMaximumUid) currentMaximumUid = uid;
        long innerOffset = uid % batchSize;
        long prefix;
        if (innerOffset == 0) {
            prefix = uid - batchSize;
            innerOffset = batchSize;
        } else {
            prefix = uid - innerOffset;
        }
        if (!managerTable.containsKey(prefix)) {
            managerTable.put(prefix, IndexerPartition.create(
                String.format("%s/%s%010d.%s", filesDir.getName(),
                    indexerFilePrefix, prefix, indexerFilePost), prefix, batchSize));
        }
        managerTable.get(prefix).keep(innerOffset, data);
        asyncCleanWorker();
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
        long prefix = uid - innerOffset;
        if (managerTable.containsKey(prefix)) {
            managerTable.get(prefix).release(innerOffset);
        }
    }

    /**
     * [Inner method]
     * Remove the useless data which received 'ack' signal.
     */
    private void clean() {
        long minimumUid = currentMaximumUid - limit;
        long minimumUidBelongTo = minimumUid - minimumUid % batchSize;
        List<Long> co = new ArrayList<>();
        for (Map.Entry<Long, IndexerPartition> every : managerTable.entrySet()) {
            if (every.getKey() < minimumUidBelongTo) co.add(every.getKey());
        }
        co.forEach(k -> {managerTable.get(k).destroy(); managerTable.remove(k);});
    }

    private void reloadFiles(String dir) {
        filesDir = new File(dir);
        if (!filesDir.isDirectory()) {
            throw new RuntimeException(String.format("[PartitionManager] (%s) isn't directory!", dir));
        }
        File[] files = filesDir.listFiles((fp, name) ->
            name.contains(indexerFilePrefix) && name.endsWith(indexerFilePost));
        if (files == null) return;
        for (File file : files) {
            String fileName = file.getName();
            String indexer_s = fileName.substring(indexerFilePrefix.length(),
                fileName.length() - indexerFilePost.length() - 1);
            long indexer = Long.parseLong(indexer_s);
            managerTable.put(indexer, file.exists() ?
                IndexerPartition.reload(file.getPath(), batchSize) :
                IndexerPartition.create(file.getPath(), indexer, batchSize));
        }
        clean();
    }

    /**
     * Just for testing
     */
    public void show() {
        for (Map.Entry<Long, IndexerPartition> every : managerTable.entrySet()) {
            every.getValue().recordState(every.getKey());
        }
//        managerTable.values().forEach(p -> p.recordState());
    }

}
