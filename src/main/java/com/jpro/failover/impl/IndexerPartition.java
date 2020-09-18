package com.jpro.failover.impl;

import com.jpro.failover.impl.Tuple3;
import lombok.extern.log4j.Log4j2;
import org.xerial.snappy.Snappy;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

@Log4j2
public class IndexerPartition extends FileOps {
    private File fp;
    private long batchSize;
    private long prefix = 0;

    /** part 1. for building and invoking */
    public static IndexerPartition create(String path, long prefix, long batchSize) {
        return new IndexerPartition(path, prefix, batchSize);
    }

    public static IndexerPartition reload(String path, long batchSize) {
        return new IndexerPartition(path, batchSize);
    }

    private IndexerPartition(String path, long prefix, long batchSize) {
        this.batchSize = batchSize;
        this.prefix = prefix;
        justCreate(path);
    }

    private IndexerPartition(String path, long batchSize) {
        this.batchSize = batchSize;
        justReload(path);
    }

    /**
     * public invoking apis
     *  1. keep api
     */
    public void keep(long uid, String data) {
        trimmingMutex.lock();
        try {
            if (writeData(uid, data)) {
                setBit(uid, true);
            }
        } finally {
            trimmingMutex.unlock();
        }
    }

    /** 2. release api */
    public void release(long uid) {
        trimmingMutex.lock();
        try {
            setBit(uid, false);
        } finally {
            trimmingMutex.unlock();
        }
    }

    public long getHistoryMaximumUid() {
        List<Tuple3<Long, Long, Long>> res = parseIndexers(readIndexerSegment());
        return res.get(res.size() - 1)._1();
    }

    public void destroy() {
        try {
            fcs.close();
            if (!fp.delete()) {
                throw new RuntimeException(String.format(
                        "[IndexerPartition] Delete file (id - %s) failed! Please check!", prefix));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            fcs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getFileName() {
        return fp.getName();
    }

    public long getPrefix() { return prefix; }

    public void setPrefix(long p) {
        prefix = p;
        seekAndWrite(0, new byte[] { 0x01 });
    }

    private boolean checkFile() {
        try {
            long fileLength = fcs.length();
            if (fileLength < getIndexerLength()) {
                log.error("File length less than indexer header!");
                return false;
            }
//            List<Long> indexers = parseFlags(readIndexerSegment()); //getFlagsHeader();
//            for (int i = 0; i < batchSize; i++) {
//                long uid = i + 1;
//                if (!indexers.contains(uid)) continue;
//                Pair<Long, Long> location = locate(uid);
//                if (location.getKey() + location.getValue() > fileLength) {
//                    log.error("One data (position + length) beyond file length!");
//                    return false;
//                }
//            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private void justCreate(String path) {
        try {
            fp = new File(path);
            if (fp.exists()) {
                throw new RuntimeException(String.format(
                        "[IndexerPartition] File (%s) already existed!", path));
            }
            if(!fp.createNewFile())
                throw new RuntimeException(String.format(
                        "[IndexerPartition] File (%s) create failed!", path));
            fcs = new RandomAccessFile(fp, "rw");
            byte[] header = new byte[getIndexerLength()];
            header[0] = 0x01;
            seekAndWrite(0, header);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void justReload(String path) {
        try{
            fp = new File(path);
            if (!fp.exists()) {
                throw new RuntimeException(String.format(
                    "[IndexerPartition] Fail to find (%s) file!", path));
            }
            fcs = new RandomAccessFile(fp, "rw");
            if (!checkFile())
                throw new RuntimeException(String.format(
                    "[IndexerPartition] Check (%s) file failed!", path
                ));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** part 2. for public real-time writing */
    private void setBit(long uid, boolean open) {
        int offset = (int) (uid % 8);
        long pos = uid / 8 + (offset == 0 ? 0 : 1);
        byte[] bytes1 = new byte[1];
        seekAndRead(pos, bytes1);
        bytes1[0] = open ?
                (byte) (bytes1[0] | (offset == 0 ? (byte) 0x01 : (byte) Math.pow(2, 8 - offset))) :
                (byte) (bytes1[0] & (offset == 0 ? (byte) 0xfe : (byte) (0xff - Math.pow(2, 8 - offset))));
        if (pos < 1) throw new RuntimeException("Set bit error! position : " + pos + ";uid: " + uid);
        seekAndWrite(pos, bytes1);
    }

    private boolean writeData(long uid, String data) {
        try {
            byte[] locationBytes = new byte[16];
            byte[] compressedData = Snappy.compress(data);
            long writeBegin = fcs.length();
            BytesUtil.putBytes(locationBytes, 0, NumberUtil.long2Byte(writeBegin));
            BytesUtil.putBytes(locationBytes, 8, NumberUtil.long2Byte(compressedData.length));
            if (writeBegin == 0 || compressedData.length == 0) {
                throw new RuntimeException("write data error");
            }
            seekAndWrite(writeBegin, compressedData);
            seekAndWrite(beginIndex(uid), locationBytes);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Write data failed!");
        }
    }

    public int getIndexerLength() {
        return (int) (1 + batchSize / 8 + batchSize * 16);
    }

    public int beginIndex(long uid) {
        return (int) (1 + batchSize / 8 + (uid - 1) * 16);
    }

    /** inner core apis */
    public byte[] readIndexerSegment() {
        byte[] indexerBuffer = new byte[getIndexerLength()];
        trimmingMutex.lock();
        try {
            seekAndRead(0, indexerBuffer);
        } finally {
            trimmingMutex.unlock();
        }
        return indexerBuffer;
    }

    public List<Long> parseFlags(byte[] indexerBytes) {
        List<Long> flags = new ArrayList<>((int) (batchSize));
        for (long i = 1; i < batchSize + 1; i++) {
            long offset = i % 8;
            byte state = indexerBytes[(int) (i / 8 + (offset == 0 ? 0 : 1))];
            if ((state & (offset == 0 ? (byte) 0x01:
                (byte) (Math.pow(2, 8 - offset)))) != 0) {
                flags.add(i);
            }
        }
        return flags;
    }

    public List<Tuple3<Long, Long, Long>> parseIndexers(byte[] indexerBytes, List<Long> flags) {
        boolean isExist = flags != null;
        HashMap<Long, Character> flagsAcc = new HashMap<>();
        if (isExist) flags.forEach(p -> flagsAcc.put(p, '0'));
        List<Tuple3<Long, Long, Long>> indexes = (flags == null ? new ArrayList<>((int) (batchSize)) : new ArrayList<>(flags.size()));
        for (long i = 1; i < batchSize + 1; i++) {
            long pos = NumberUtil.byte2Long(indexerBytes, beginIndex(i));
            long len = NumberUtil.byte2Long(indexerBytes, beginIndex(i) + 8);
            if (!isExist && pos != 0x00) {
                indexes.add(new Tuple3<>(i, pos, len));
            } else if (isExist && flagsAcc.containsKey(i)) {
                indexes.add(new Tuple3<>(i, pos, len));
            }
        }
        return indexes;
    }

    public List<Tuple3<Long, Long, Long>> parseIndexers(byte[] indexerBytes) {
        return parseIndexers(indexerBytes, null);
    }

    public static String uncompress(byte[] buf, int start, int len) {
        byte[] var = BytesUtil.getBytes(buf, start, len);
        try {
            return new String(Snappy.uncompress(var));
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /** public api, calculate every kinds of factors. */
    public double releaseRate() {
        byte[] ptr = readIndexerSegment();
        return (double) (parseIndexers(ptr).size() - parseFlags(ptr).size()) / batchSize;
    }

    public TravelIterator pull() {
        return new TravelIterator(this);
    }

    public boolean askForTrim() {
        if (hasTrimmed()) { return false; }
        double rate = releaseRate();
//        System.out.println("ask " + rate);
        return rate > 0.9;
    }

    public boolean hasTrimmed() {
        byte[] byte1 = new byte[1];
        seekAndRead(0, byte1);
//        return byte1[0] == 0x02;
        return byte1[0] != 0x01;
    }

    private AtomicBoolean isTrimming = new AtomicBoolean(false);

//    private ReentrantReadWriteLock.WriteLock trimmingMutex = new ReentrantReadWriteLock().writeLock();
    private ReentrantLock trimmingMutex = new ReentrantLock();

    public void trim() {
        if (!askForTrim()) return;
        if (!isTrimming.compareAndSet(false, true)) return;
        trimmingMutex.lock();
        try {
            String onlyFileName = fp.getAbsolutePath();
            log.info("## trim " + getFileName() + " ; object uid: " + hashCode());
            File n_fp = new File(String.format("%s.%s", fp.getAbsolutePath(), "trim"));
            RandomAccessFile n_fcs = new RandomAccessFile(n_fp, "rw");
            // TODO: task preparation, for append the large data, set length first
            n_fcs.setLength(getIndexerLength());
            ////////////////////////////////////////
            byte[] p0 = readIndexerSegment();
            List<Long> flags = parseFlags(p0);
            List<Tuple3<Long, Long, Long>> indexes = parseIndexers(p0);
            HashMap<Long, Character> flagsAcc = new HashMap<>();
            flags.forEach(p -> flagsAcc.put(p, '0'));
            p0[0] = 0x02;
            for (Tuple3<Long, Long, Long> el : indexes) {
                if (!flagsAcc.containsKey(el._1())) {
                    int uid = el._1().intValue();
                    BytesUtil.putBytes(p0, beginIndex(uid),
                            NumberUtil.long2Byte(-1L));
                    BytesUtil.putBytes(p0, beginIndex(uid) + 8,
                            NumberUtil.long2Byte(0));
                }
            }
            InnerTravelIterator iter = new InnerTravelIterator(
                this, 10 * 1024 * 1024, p0, batchSize);
            FileOps fpOps = new FileOps().setFp(n_fcs);
            while (iter.hasTheNext()) {
//                System.out.println("next - -- " + hashCode());
                Tuple3<byte[], Integer, Integer> tp = iter.theNext();
                // TODO: task 1. here isn't append the single data, is batch of data.
                fpOps.append(tp._1(), tp._2(), tp._3());
            }
//            System.out.println("---10");
            // TODO: task 2. flush index data
            fpOps.seekAndWrite(0, p0);
            ////////////////////////////////////////
            // step final. replace file
//            fcs.close();
//            n_fcs.close();
//            if (!new File(fp.getAbsolutePath()).delete()) {
//                throw new RuntimeException("Fail to delete file.");
//            }
//            if (!n_fp.renameTo(new File(onlyFileName))) {
//                fp = n_fp;
//                fcs = new RandomAccessFile(fp, "rw");
//            }
            if (!fp.delete()) {
                throw new RuntimeException("Fail to delete file.");
            }
            if (!n_fp.renameTo(new File(onlyFileName))) {
                throw new RuntimeException("Fail to rename file.");
            }
//            System.out.println("obj: " + hashCode() + " n_fp.ab: "
//                    + n_fp.getAbsoluteFile() + "; n_fp.name: " + n_fp.getName());
            fp = n_fp;
            fcs = n_fcs;
        } catch (IOException e) {
            isTrimming.set(false);
            e.printStackTrace();
        } finally {
            trimmingMutex.unlock();
        }
        log.info("## over trim " + getFileName() + " ; object uid: " + hashCode());
        isTrimming.set(false);
    }

    /**
     * The following methods are inefficient api.
     * Avoid invoking these methods.
     */
    public List<Long> analysisFlags(long start, long end, byte[] bytesFlags) {
        LinkedList<Long> flags = new LinkedList<>();
        for (long i = start; i < end; i++) {
            long offset = i % 8;
            byte state = bytesFlags[(int) (i / 8 + (offset == 0 ? 0 : 1)) - 1];
            if ((state & (offset == 0 ? (byte) 0x01: (byte) (Math.pow(2, 8 - offset)))) != 0) {
                flags.add(i);
            }
        }
        return flags;
    }

    public List<Pair<Long, String>> recover() {
        byte[] bytesFlags = new byte[(int) (batchSize / 8)];
        seekAndRead(1, bytesFlags);
        List<Long> indexes = analysisFlags(1, batchSize + 1, bytesFlags);
        ArrayList<Pair<Long, String>> res = new ArrayList<>(indexes.size());
        for (Long index : indexes) {
            res.add(new Pair<>(index, takeTarget(locate(index))));
        }
        return res;
    }

    public boolean hasData(long uid) {
        int offset = (int) (uid % 8);
        long pos = uid / 8 + (offset == 0 ? 0 : 1);
        byte[] block = new byte[1];
        seekAndRead(pos, block);
        return (block[0] & (offset == 0 ? (byte) 0x01 : (byte) Math.pow(2, 8 - offset))) != 0;
    }

    private byte[] shared16b = new byte[16];
    public Pair<Long, Long> locate(long uid) {
        seekAndRead(beginIndex(uid), shared16b);
        long pos = NumberUtil.byte2Long(shared16b);
        long len = NumberUtil.byte2Long(BytesUtil.getBytes(shared16b, 8, 8));
        return new Pair<>(pos, len);
    }

    private String takeTarget(Pair<Long, Long> pos) {
        byte[] bytesDynamic = new byte[(int) pos.getValue().longValue()];
        seekAndRead(pos.getKey(), bytesDynamic);
        try {
            return new String(Snappy.uncompress(bytesDynamic));
        } catch (IOException e) {
//            throw new RuntimeException(e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public String getTarget(long uid) {
//        if (!hasData(uid)) return null;
        Pair<Long, Long> pos = locate(uid);
        if (pos.getKey() <= 0) { return null; }
        return takeTarget(pos);
    }

    /** Methods for testing */
    public void recordState(long rc) {
        try {
            File file = new File(fp.getAbsolutePath() + ".csv");
            FileOutputStream foc = new FileOutputStream(file);
            foc.write("uid,exists,start,len,data\n".getBytes());
            byte[] cache0 = readIndexerSegment();
            List<Long> headers = parseFlags(cache0);
            HashMap<Long, Character> flags = new HashMap<>();
            headers.forEach(p -> flags.put(p, '\0'));
            List<Tuple3<Long, Long, Long>> indexes = parseIndexers(cache0);
            HashMap<Long, Tuple3<Long, Long, Long>> indexers = new HashMap<>();
            indexes.forEach(p -> indexers.put(p._1(), p));
            for (int i = 0; i < batchSize; i++) {
                long uid = i + 1;
                if (indexers.containsKey(uid)) {
                    String info = String.format("uid: %d, exists: %d, start: %d, len: %d, data: %s\n",
                            uid, flags.containsKey(uid) ? 1 : 0, indexers.get(uid)._2(), indexers.get(uid)._3(), getTarget(uid));
                    foc.write(info.getBytes());
                } else {
                    String info = String.format("uid: %d, exists: %d, start: %d, len: %d, data: %s\n",
                            uid, 0, 0, 0, null);
                    foc.write(info.getBytes());
                }
            }
            foc.flush();
            foc.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
