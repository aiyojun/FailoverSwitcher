package com.jpro.failover;

import com.jpro.util.NumberUtil;
import com.jpro.util.Pair;
import org.xerial.snappy.Snappy;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class IndexerPartition {

    public static IndexerPartition create(String path, long prefix, long batchSize) {
        return new IndexerPartition(path, prefix, batchSize);
    }

    public static IndexerPartition reload(String path, long batchSize) {
        return new IndexerPartition(path, batchSize);
    }

    private File fp;
    private RandomAccessFile fcs;
    private long batchSize;
    private long prefix = 0;

    public long getPrefix() {
        return prefix;
    }

    public void setPrefix(long p) {
        prefix = p;
        seekAndWrite(0, NumberUtil.long2Byte(p));
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

    private void justCreate(String path) {
        try {
            fp = new File(path);
            if (fp.exists()) {
                throw new RuntimeException(String.format(
                    "[IndexerPartition] File (%s) already existed!", path));
            }
            fcs = new RandomAccessFile(fp, "rw");
            fcs.write(NumberUtil.long2Byte(prefix));
            byte[] bytes1 = new byte[] {0x00};
            for (int idx = 0; idx < batchSize / 8; idx++) {
                fcs.write(bytes1);
            }
            byte[] bytes8 = new byte[] {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
            for (int idx = 0; idx < batchSize; idx++) {
                fcs.write(bytes8);
                fcs.write(bytes8);
            }
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
            byte[] bytes8 = new byte[8];
            seekAndRead(0, bytes8);
            prefix = NumberUtil.byte2Long(bytes8);
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    private void setBit(long uid, boolean open) {
        int offset = (int) (uid % 8);
        long pos = uid / 8 + (offset == 0 ? 0 : 1);
        byte[] bytes1 = new byte[1];
        seekAndRead(pos, bytes1);
        bytes1[0] = open ?
            (byte) (bytes1[0] | (offset == 0 ? (byte) 0x01 : (byte) Math.pow(2, 8 - offset))) :
            (byte) (bytes1[0] & (offset == 0 ? (byte) 0xfe : (byte) (0xff - Math.pow(2, 8 - offset))));
        seekAndWrite(pos, bytes1);
    }

    byte[] write16b = new byte[16];
    private boolean writeData(long uid, String data) {
        try {
            byte[] compressedData = Snappy.compress(data);
            long writeBegin = fcs.length();
            putBytes(write16b, 0, NumberUtil.long2Byte(writeBegin));
            putBytes(write16b, 8, NumberUtil.long2Byte(compressedData.length));
            if (writeBegin == 0 || compressedData.length == 0) {
                throw new RuntimeException("write data error");
            }
            seekAndWrite(1 + batchSize / 8 + (uid - 1) * 16, write16b);
            seekAndWrite(writeBegin, compressedData);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public void keep(long uid, String data) {
//        if (hasData(uid)) return;
        if (writeData(uid, data)) {
            setBit(uid, true);
        }

    }

    public void release(long uid) {
        setBit(uid, false);
    }

    public boolean hasData(long uid) {
        int offset = (int) (uid % 8);
        long pos = uid / 8 + (offset == 0 ? 0 : 1);
        byte[] block = new byte[1];
        seekAndRead(pos, block);
        return (block[0] & (offset == 0 ? (byte) 0x01 : (byte) Math.pow(2, 8 - offset))) != 0;
    }

    public List<Pair<Long, String>> recover() {
        ArrayList<Long> indexes = new ArrayList<>();
        byte[] bytes1w = new byte[(int) (batchSize / 8)];
        seekAndRead(1, bytes1w);
        for (long i = 1; i <= batchSize; i++) {
            long offset = i % 8;
            byte state = bytes1w[(int) (i / 8 + (offset == 0 ? 0 : 1)) - 1];
            if ((state & (offset == 0 ? (byte) 0x01: (byte) (Math.pow(2, 8 - offset)))) != 0) {
                indexes.add(i);
            }
        }
//        indexes.forEach(p -> System.out.print(p + " "));
//        System.out.println();
        ArrayList<Pair<Long, String>> res = new ArrayList<>();
        for (Long index : indexes) {
            seekAndRead(1 + batchSize / 8 + (index - 1) * 16, shared16b);
            long pos = NumberUtil.byte2Long(shared16b);
            long len = NumberUtil.byte2Long(getBytes(shared16b, 8, 8));
            byte[] bytesn = new byte[(int) len];
            seekAndRead(pos, bytesn);
            try {
                res.add(new Pair<>(index, new String(Snappy.uncompress(bytesn))));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return res;
    }

    private ReentrantLock mutex = new ReentrantLock();

    private void seekAndRead(long pos, byte[] buf) {
        mutex.lock();
        try {
            fcs.seek(pos);
            fcs.read(buf);
            mutex.unlock();
        } catch (IOException e) {
            mutex.unlock();
            e.printStackTrace();
        }
    }

    private void seekAndWrite(long pos, byte[] buf) {
        mutex.lock();
        try {
            fcs.seek(pos);
            fcs.write(buf);
            mutex.unlock();
        } catch (IOException e) {
            mutex.unlock();
            e.printStackTrace();
        }
    }

    private static byte[] getBytes(byte[] buf, int start, int len) {
        byte[] res = new byte[len];
        System.arraycopy(buf, start, res, 0, len);
        return res;
    }

    private static void putBytes(byte[] dst, int start, byte[] src) {
        for (int i = 0; i < src.length; i++) {
            dst[start + i] = src[i];
        }
    }

    byte[] shared16b = new byte[16];
    private Pair<Long, Long> locate(long uid) {
        seekAndRead(1 + batchSize / 8 + (uid - 1) * 16, shared16b);
        long pos = NumberUtil.byte2Long(shared16b);
        long len = NumberUtil.byte2Long(getBytes(shared16b, 8, 8));
        return new Pair<>(pos, len);
    }

    private String takeTarget(Pair<Long, Long> pos) {
        byte[] bytesn = new byte[(int) pos.getValue().longValue()];
        seekAndRead(pos.getKey(), bytesn);
        try {
            return new String(Snappy.uncompress(bytesn));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public String getTarget(long uid) {
        if (!hasData(uid)) return null;
        Pair<Long, Long> pos = locate(uid);
        return takeTarget(pos);
    }

    public void recordState(long rc) {
        try {
            File file = new File("p_" + rc + "_show.csv");
            FileOutputStream foc = new FileOutputStream(file);
            foc.write("uid,exists,start,len,data\n".getBytes());
            for (int i = 0; i < batchSize; i++) {
                long uid = i + 1;
                seekAndRead(1 + batchSize / 8 + (uid - 1) * 16, shared16b);
                long pos = NumberUtil.byte2Long(shared16b);
                long len = NumberUtil.byte2Long(getBytes(shared16b, 8, 8));
                String data = String.format("%d,%d,%d,%d,%s\n",
                        i + 1,
                        hasData(i+1) ? 1 : 0,
                        pos,
                        len,
                        getTarget(uid)
                );
                foc.write(data.getBytes());
            }
            foc.flush();
            foc.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
