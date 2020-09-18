package com.jpro.failover.impl;

import com.jqs.bigdata.rs.overall.doris.impl.Tuple3;
import lombok.extern.log4j.Log4j2;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

@Log4j2
public class TravelIteratorV1 implements Iterator<Pair<Long, String>> {
    private int cursor = 0;
    private long currentFileOffset;
    private long batchSize = 512 * 1024 * 1024;
    private boolean innerInvoking = false;
    private IndexerPartition partition;
    private List<Tuple3<Long, Long, Long>> keptData;
    private List<Pair<Long, String>> cache = new LinkedList<>();
    private Iterator<Pair<Long, String>> iter;
    private byte[] regroup;
    private long regroupSize;
    private long regroupCursor = 0;
    private List<Long> failToRead;

    public TravelIteratorV1(IndexerPartition ip) {
        this.partition = ip;
        this.currentFileOffset = ip.getIndexerLength();
        this.init();
    }

    public TravelIteratorV1(IndexerPartition ip, long batchSize, byte[] headers, long size) {
        innerInvoking = true;
        this.batchSize = batchSize;
        this.regroupSize = size;
        this.regroup = headers;
        this.partition = ip;
        this.currentFileOffset = ip.getIndexerLength();
        this.init();
    }

    public List<Long> getFailedUid() {
        if (failToRead == null) return new LinkedList<>(); else return failToRead;
    }

    private void init() {
        byte[] ptr = partition.readIndexerSegment();
        List<Long> keptFlags = partition.parseFlags(ptr);
        keptData = partition.parseIndexers(ptr, keptFlags);
        keptData.sort(Comparator.comparing(Tuple3::_2));
        if (!innerInvoking) loadNextBatch();
        iter = cache.iterator();
    }

    private int pick() {
//            System.out.println(" -- -yyy 0");
        if (cursor < keptData.size()) {
//            System.out.println(" -- -yyy 1");
            Tuple3<Long, Long, Long> first = keptData.get(cursor);
            currentFileOffset = first._2();
        }
//            System.out.println(" -- -yyy 2 cursor: " + cursor + "; kept data: " + keptData.size());
        int index = cursor;
        for (int i = cursor; i < keptData.size(); i++) {
//            System.out.println(" - -loop");
            Tuple3<Long, Long, Long> el = keptData.get(i);
            if (el._2() + el._3() - currentFileOffset > batchSize) {
//                index--;
//            System.out.println(" - -loop break " + index + " ;cursor: " + cursor);
                break;
            }
            index++;
        }
//            System.out.println(" -- -yyy 3 " + index);
        if (cursor == index) {
            String err = String.format(
                    "Pick failed! picked index: %d, cursor: %d, keptData size: %d", index, cursor, keptData.size());
            log.error(err);
//            System.out.println(" -- -yyy err");
            throw new RuntimeException(err);
        }
//            System.out.println(" -- -yyy 4");
        return index;
    }

    protected boolean hasNextBatch() {
        return cursor < keptData.size();
    }

    protected void loadNextBatch() {
        if (innerInvoking) {throw new RuntimeException("Inner invoking!");}
        if (keptData.isEmpty()) return;
        cache.clear();
        int lastIndex = pick();
        Tuple3<Long, Long, Long> last = keptData.get(lastIndex - 1);
        byte[] buffer = new byte[(int) batchSize];
        partition.seekAndRead(currentFileOffset, buffer, 0, (int) (last._2() + last._3() - currentFileOffset));
        for (int i = cursor; i < lastIndex; i++) {
            Tuple3<Long, Long, Long> el = keptData.get(i);
            try {
                String s = IndexerPartition.uncompress(buffer, (int) (el._2() - currentFileOffset), (int) (el._3().longValue()));
                cache.add(new Pair<>(el._1(), s));
            } catch (Exception e) {
                e.printStackTrace();
                if (failToRead == null)
                    failToRead = new LinkedList<>();
                failToRead.add(el._1());
            }
        }
        cursor = lastIndex;
        currentFileOffset = last._2() + last._3();
    }

    protected Tuple3<byte[], Integer, Integer> nextBatch() {
        if (!innerInvoking) {throw new RuntimeException("Outer invoking!");}
//        System.out.println("---- xxx 0");
        int lastIndex = pick();
//        System.out.println("---- xxx 0.1 " + lastIndex);
        Tuple3<Long, Long, Long> last = keptData.get(lastIndex - 1);
        byte[] readBuffer = new byte[(int) batchSize];
        byte[] writeBuffer = new byte[(int) batchSize];
        int ptr = 0;
//        System.out.println("---- xxx 1");
        partition.seekAndRead(currentFileOffset, readBuffer, 0, (int) (last._2() + last._3() - currentFileOffset));
//        System.out.println("---- xxx 2");
        for (int i = cursor; i < lastIndex; i++) {
            Tuple3<Long, Long, Long> el = keptData.get(i);
            long len = el._3();
            BytesUtil.putBytes(writeBuffer, ptr, readBuffer, (int) (el._2() - currentFileOffset), (int) len);
            BytesUtil.putBytes(regroup, (int) (1 + regroupSize / 8 + (el._1() - 1) * 16),
                    NumberUtil.long2Byte(1 + regroupSize / 8 + regroupSize * 16 + regroupCursor + ptr));
            BytesUtil.putBytes(regroup, (int) (1 + regroupSize / 8 + (el._1() - 1) * 16 + 8),
                    NumberUtil.long2Byte(len));
            ptr += (int) len;
        }
//        System.out.println("---- xxx 3");
        cursor = lastIndex;
        regroupCursor += ptr;
        currentFileOffset = last._2() + last._3();
        return new Tuple3<>(writeBuffer, 0, ptr);
    }

    @Override
    public boolean hasNext() {
        if (iter.hasNext()) {
            return true;
        } else {
            if (hasNextBatch()) {
                loadNextBatch();
                iter = cache.iterator();
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
