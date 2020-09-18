package com.jpro.failover.impl;

import lombok.extern.log4j.Log4j2;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReentrantLock;

@Log4j2
public class FileOps {
    protected RandomAccessFile fcs;

    private ReentrantLock mutex = new ReentrantLock();

    public FileOps setFp(RandomAccessFile fcs) {
        this.fcs = fcs;
        return this;
    }

    public void flush() {
        try {
            fcs.getFD().sync();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void append(byte[] bf) {
//        long t1 = TimeUtil.getCurrentTime();
        mutex.lock();
        try {
            fcs.seek(fcs.length());
            fcs.write(bf);
//            fcs.getFD().sync();
            mutex.unlock();
        } catch (IOException e) {
            mutex.unlock();
            e.printStackTrace();
        }
//        log.info("[FileWrite] cost: " + (TimeUtil.getCurrentTime() - t1) + "ms.");
    }

    public void append(byte[] bf, int start, int len) {
//        long t1 = TimeUtil.getCurrentTime();
        mutex.lock();
        try {
            fcs.seek(fcs.length());
            fcs.write(bf, start, len);
//            fcs.getFD().sync();
            mutex.unlock();
        } catch (IOException e) {
            mutex.unlock();
            e.printStackTrace();
        }
//        log.info("[FileWrite] cost: " + (TimeUtil.getCurrentTime() - t1) + "ms.");
    }

    public void seekAndWrite(long pos, byte[] bf) {
//        long t1 = TimeUtil.getCurrentTime();
        mutex.lock();
        try {
            fcs.seek(pos);
            fcs.write(bf);
//            fcs.getFD().sync();
            mutex.unlock();
        } catch (IOException e) {
            mutex.unlock();
            e.printStackTrace();
        }
//        log.info("[FileWrite] cost: " + (TimeUtil.getCurrentTime() - t1) + "ms.");
    }

    public void seekAndRead(long pos, byte[] buf) {
//        long t1 = TimeUtil.getCurrentTime();
        mutex.lock();
        try {
            fcs.seek(pos);
            fcs.read(buf);
            mutex.unlock();
        } catch (IOException e) {
            mutex.unlock();
            e.printStackTrace();
        }
//        log.info("[FileRead] cost: " + (TimeUtil.getCurrentTime() - t1) + "ms. pos: " + pos + "; len: " + buf.length);
    }

    public void seekAndRead(long pos, byte[] buf, int start, int len) {
//        long t1 = TimeUtil.getCurrentTime();
        if (len > buf.length) {
            throw new RuntimeException("Read length beyond buffer length!");
        }
        mutex.lock();
        try {
            fcs.seek(pos);
            fcs.read(buf, start, len);
            mutex.unlock();
        } catch (IOException e) {
            mutex.unlock();
            e.printStackTrace();
        }
//        log.info("[FileRead] cost: " + (TimeUtil.getCurrentTime() - t1) + "ms. pos: " + pos + "; len: " + buf.length);
    }
}
