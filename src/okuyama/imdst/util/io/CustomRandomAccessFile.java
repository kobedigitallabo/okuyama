package okuyama.imdst.util.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.*;

import okuyama.imdst.util.*;

public class CustomRandomAccessFile {

    private ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private Lock r = rwl.readLock();
    private Lock w = rwl.writeLock();

    private RandomAccessFile[] rafs = null;

    public CustomRandomAccessFile(File target, String type) throws FileNotFoundException {
        this.rafs = new RandomAccessFile[new Long(ImdstDefine.maxParallelRandomAccess).intValue()];

        for (int i = 0; i < new Long(ImdstDefine.maxParallelRandomAccess).intValue(); i++) {
            this.rafs[i] = new RandomAccessFile(target, type);
        }
    }

    public void seekAndWrite(long seekPoint, byte[] data, int start, int size) throws IOException {

        this.rafs[new Long(seekPoint % ImdstDefine.maxParallelRandomAccess).intValue()].seek(seekPoint);
        this.r.lock();
        try { 
            int radIdx = new Long(seekPoint % ImdstDefine.maxParallelRandomAccess).intValue();
            if (this.rafs[radIdx] != null) {
                synchronized (this.rafs[radIdx]) {
                    this.rafs[radIdx].seek(seekPoint);
                    this.rafs[radIdx].write(data, start, size);
                }
            }
        } finally { 
            this.r.unlock(); 
        }

    }

    public int seekAndRead(long seekPoint, byte[] data, int start, int size) throws IOException {
        int ret = 0;
        this.r.lock();
        try { 
            int radIdx = new Long(seekPoint % ImdstDefine.maxParallelRandomAccess).intValue();
            if (this.rafs[radIdx] != null) {
                synchronized (this.rafs[radIdx]) {
                    this.rafs[radIdx].seek(seekPoint);
                    ret = this.rafs[radIdx].read(data, start, size);
                }
            }
        } finally { 
            this.r.unlock(); 
        }
        return ret;
    }

    public void close() throws IOException {
        this.w.lock();
        try { 

            for (int i = 0; i < ImdstDefine.maxParallelRandomAccess; i++) {
                this.rafs[i].close();
                this.rafs[i] = null;
            }
        } finally { 
            this.w.unlock(); 
        }
    }
}

