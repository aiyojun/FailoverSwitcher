package com.jpro.failover.impl;

import com.jpro.failover.impl.Tuple3;
import lombok.extern.log4j.Log4j2;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

@Log4j2
public class TravelIterator implements Iterator<Pair<Long, String>> {
    private IndexerPartition partition;
    private int volumeToApply = 512 * 1024 * 1024; // < 2G

    private List<Tuple3<Long, Long, Long>> keptData;
    private int keptDataIndexOffset = 0;

    private long readFileCursorOffset;
    private List<Long> failToRead;

    private boolean innerInvoking = false;
    private List<Pair<Long, String>> cache = new LinkedList<>();
    private Iterator<Pair<Long, String>> iter;

    private byte[] headerToModify;
    private long writeFileCursorOffset = 0;

    public TravelIterator(IndexerPartition ip) {
        this.partition = ip;
        this.readFileCursorOffset = ip.getIndexerLength();
        this.init();
    }

    public TravelIterator(IndexerPartition ip, long batchSize, byte[] headers) {
        innerInvoking = true;
        this.volumeToApply = (int) batchSize;
        this.headerToModify = headers;
        this.partition = ip;
        this.readFileCursorOffset = ip.getIndexerLength();
        this.init();
    }

    public List<Long> getFailedUid() {
        if (failToRead == null) return new LinkedList<>(); else return failToRead;
    }

    private void init() {
        byte[] header = partition.readIndexerSegment();
        List<Long> keptFlags = partition.parseFlags(header);
        keptData = partition.parseIndexers(header, keptFlags);
        keptData.sort(Comparator.comparing(Tuple3::_2));
        long last = partition.getIndexerLength();
        for (Tuple3<Long, Long, Long> every : keptData) {
            if (every._2() < last) {
                throw new RuntimeException(String.format(
                    "File sequence error! uid %d, check current uid and the front.", every._1()));
            }
            last = every._2() + every._3();
        }
        if (!innerInvoking && hasNextBatch()) loadNextBatch();
        iter = cache.iterator();
    }

    private void updateKeptDataOffset(int keptDataNewOffset) {
        keptDataIndexOffset = keptDataNewOffset;
    }

    private void updateReadFileOffset(long readFileNewOffset) {
        readFileCursorOffset = readFileNewOffset;
    }

    private int findNextEndIndex() {
        Tuple3<Long, Long, Long> first =
            keptData.get(keptDataIndexOffset);
        updateReadFileOffset(first._2());
        int index = keptDataIndexOffset;
        for (int i = keptDataIndexOffset; i < keptData.size(); i++) {
            Tuple3<Long, Long, Long> el = keptData.get(i);
            if (el._2() + el._3() - readFileCursorOffset > volumeToApply) break;
            index++;
        }
        if (keptDataIndexOffset == index) {
            String err = String.format(
                "Pick failed! picked index: %d, cursor: %d, keptData size: %d",
                index, keptDataIndexOffset, keptData.size());
            log.error(err);
            throw new RuntimeException(err);
        }
        return index;
    }

    protected boolean hasNextBatch() {
        return keptDataIndexOffset < keptData.size();
    }

    protected void loadNextBatch() {
        if (innerInvoking) {throw new RuntimeException("Inner invoking!");}
        if (keptData.isEmpty()) return;
        cache.clear();
        int endIndex = findNextEndIndex();
        Tuple3<Long, Long, Long> last = keptData.get(endIndex - 1);
        byte[] buffer = new byte[volumeToApply];
        partition.seekAndRead(readFileCursorOffset, buffer, 0, (int) (last._2() + last._3() - readFileCursorOffset));
        for (int i = keptDataIndexOffset; i < endIndex; i++) {
            Tuple3<Long, Long, Long> el = keptData.get(i);
            try {
                String s = IndexerPartition.uncompress(buffer, (int) (el._2() - readFileCursorOffset), (int) (el._3().longValue()));
                cache.add(new Pair<>(el._1(), s));
            } catch (Exception e) {
                e.printStackTrace();
                if (failToRead == null) failToRead = new LinkedList<>();
                failToRead.add(el._1());
            }
        }
        updateKeptDataOffset(endIndex);
    }

    protected Tuple3<byte[], Integer, Integer> nextBatch() {
        if (!innerInvoking) {throw new RuntimeException("Outer invoking!");}
        // TODO: choose the end index of kept data
        int endIndex = findNextEndIndex();
        Tuple3<Long, Long, Long> last = keptData.get(endIndex - 1);
        byte[] readBuffer = new byte[volumeToApply];
        byte[] writeBuffer = new byte[volumeToApply];
        int writeBytesOffset = 0;
        partition.seekAndRead(readFileCursorOffset, readBuffer, 0, (int) (last._2() + last._3() - readFileCursorOffset));
//        log.info(String.format("kept size: %d, start: %d, end: %d;", keptData.size(), keptDataIndexOffset, endIndex - 1));
        for (int i = keptDataIndexOffset; i < endIndex; i++) {
            Tuple3<Long, Long, Long> el = keptData.get(i);
            long len = el._3();
            // todo: 1. write data
            //       2. update new position and length(?)
            BytesUtil.putBytes(writeBuffer, writeBytesOffset,
                    readBuffer, (int) (el._2() - readFileCursorOffset), (int) len);
            BytesUtil.putBytes(headerToModify, partition.beginIndex(el._1()),
                    NumberUtil.long2Byte(partition.getIndexerLength() + writeFileCursorOffset + writeBytesOffset));
            BytesUtil.putBytes(headerToModify, partition.beginIndex(el._1()) + 8,
                    NumberUtil.long2Byte(len));
//            log.info(String.format("cursor: %d, fp offset: %d, ptr_0: %d, ptr: %d, len: %d",
//                    i, el._2(), (int) (el._2() - readFileCursorOffset), writeBytesOffset, len));
            // TODO: update write buffer offset
            writeBytesOffset += (int) len;
        }
        // TODO: update kept data start index
        updateKeptDataOffset(endIndex);
        writeFileCursorOffset += writeBytesOffset;
        return new Tuple3<>(writeBuffer, 0, writeBytesOffset);
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
