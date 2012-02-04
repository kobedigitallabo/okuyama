package okuyama.imdst.util.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.*;

import okuyama.imdst.util.*;


/**
 * IOのRandomAccessFileのラッパー.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class CustomRandomAccess extends AbstractDataRandomAccess {

    protected Map dataPointMap = null;

    private InnerCustomRandomAccessFile innerCustomRandomAccessFile = null;

    private long nowSeekPoint = 0L;


    public CustomRandomAccess(File target, String type) throws FileNotFoundException {
        super(target, type);
        this.innerCustomRandomAccessFile = new InnerCustomRandomAccessFile(target, type, 0, ImdstDefine.dataFileWriteMaxSize);
        try {
            super.close();
        } catch (Exception e) {
        }
    }


    public void setDataPointMap(Map dataPointMap) {
        this.dataPointMap = dataPointMap;
    }


    public void requestSeekPoint(long seekPoint, int start, int size) {
    }

    public int seekAndRead(long seekPoint, byte[] data, int start, int size, Object key) throws IOException {
        throw new IOException("Not found methdo");
    }

    public void seek(long seekPoint) throws IOException {
        this.nowSeekPoint = seekPoint;
    }

    public int read(byte[] data, int start, int size) throws IOException {
        return this.innerCustomRandomAccessFile.seekAndRead(this.nowSeekPoint, data, start, size);
    }

    public void write(byte[] data, int start, int size) throws IOException {
        this.innerCustomRandomAccessFile.seekAndWrite(this.nowSeekPoint, data, start, size);
    }

    public void close() throws IOException {
        this.innerCustomRandomAccessFile.close();
        super.close();
    }
}


class InnerCustomRandomAccessFile extends Thread {

    // 遅延書き込み依頼用のQueue
    private ArrayBlockingQueue delayWriteQueue = new ArrayBlockingQueue(ImdstDefine.dataFileWriteDelayMaxSize);

    // 遅延書き込み前のデータを補完するMap
    private ConcurrentHashMap delayWriteDifferenceMap = new ConcurrentHashMap(ImdstDefine.dataFileWriteDelayMaxSize, ImdstDefine.dataFileWriteDelayMaxSize - 20, 64);



    private RandomAccessFile readRaf = null;

    private RandomAccessFile writeRaf = null;

    private boolean throwExceptionFlg = false;
    private Exception throwException = null;

    private boolean endFlg = false;

    private boolean delayWriteEndStatus = false;

    private boolean execute = false;


    private int parallelSize = 4999;
    private Object[] syncObjList = null;

    private int defaultStart = 0;
    private int defaultSize = 0;


    public InnerCustomRandomAccessFile(File target, String type, int defaultStart, int defaultSize) throws FileNotFoundException {
        this.readRaf = new RandomAccessFile(target, type);
        this.writeRaf = new RandomAccessFile(target, type);

        this.syncObjList = new Object[this.parallelSize];
        for (int i = 0; i < this.parallelSize; i++) {
            this.syncObjList[i] = new Object();
        }
        this.defaultStart = defaultStart;
        this.defaultSize = defaultSize;

        this.start();
    }


    public void run() {
        long continuousnessWrite = 0;
        int writeTimingCount = ImdstDefine.dataFileWriteDelayMaxSize / 10;
        int waitTimingCount = new Double(writeTimingCount * 0.70).intValue();

        boolean nowWrite = false;

        while (true) {
            Long seekPoint = null;
            if (this.endFlg) {
                this.delayWriteEndStatus = true;
                break;
            }

            try {

                int nowQueueSize = this.delayWriteQueue.size();

                if (this.execute == true || nowWrite == true || writeTimingCount < nowQueueSize) {
                    if (waitTimingCount > nowQueueSize) {
                        nowWrite = false;
                    } else {
                        nowWrite = true;
                    }

                    if ((continuousnessWrite % 1000) == 0) {

                        FileDescriptor fd = this.writeRaf.getFD();
                        fd.sync();
                    }

                    seekPoint = (Long)this.delayWriteQueue.poll(500, TimeUnit.MILLISECONDS);
                    if (seekPoint == null) continue;

                    long longSeekPoint = seekPoint.longValue();

                    synchronized (this.syncObjList[new Long(seekPoint % this.parallelSize).intValue()]) {
                        byte[] data = null;
                        data = (byte[])this.delayWriteDifferenceMap.get(seekPoint);
                        if (data != null) {

                            this.writeRaf.seek(longSeekPoint);
                            this.writeRaf.write(data, this.defaultStart, this.defaultSize);
                        }
                        continuousnessWrite++;
                    }
                   this.delayWriteDifferenceMap.remove(seekPoint);
                } else {
                    continuousnessWrite = 0;
                    Thread.sleep(500);
                }
            } catch (Exception e) {
                e.printStackTrace();
                this.throwException = e;
                this.throwExceptionFlg = true;
                this.delayWriteEndStatus = true;
                break;
            }
        }
    }


    public void seekAndWrite(long seekPoint, byte[] data) throws IOException {
        this.seekAndWrite(seekPoint, data, this.defaultStart, this.defaultSize);
    }


    public void seekAndWrite(long seekPoint, byte[] data, int start, int size) throws IOException {

        if (throwExceptionFlg) throw new IOException("delayDataFileWriteError [" +  throwException.getMessage() + "]");
        Long seekPointObj = new Long(seekPoint);
        this.delayWriteDifferenceMap.put(seekPointObj, data);
        try {
            this.delayWriteQueue.put(seekPointObj);
        } catch(Exception e) {
            throw new IOException("delayWriteQueue - put Error Message[" + e.getMessage() + "]");
        }
    }


    public int seekAndRead(long seekPoint, byte[] data, int start, int size) throws IOException {
        int ret = 0;

        try {
            Long seekPointObj = new Long(seekPoint);
            byte[] readData = (byte[])this.delayWriteDifferenceMap.get(seekPointObj);
            if (readData != null) {

                for (int i = start; i < size; i++) {
                    data[i] = readData[ret];
                    ret++;
                }
            } else {
                synchronized (this.syncObjList[new Long(seekPoint % this.parallelSize).intValue()]) {
                    this.readRaf.seek(seekPoint);
                    ret = this.readRaf.read(data, start, size);
                }
            }
        } catch (IOException ie) {
            throw ie;
        }
        return ret;
    }

    public void close() throws IOException {

        try { 
            this.execute = true;
            if (this.delayWriteEndStatus) {
                if (this.throwExceptionFlg) {
                    throw new IOException(this.throwException);
                }
                return;
            }

            while(true) {
    
                if (this.delayWriteQueue.size() > 0) {
                    Thread.sleep(10);
                } else {
                    this.endFlg = true;
                    break;
                }
            }
            this.join();
        } catch (IOException ie) {
            throw ie;
        } catch (Exception ie) {
        } finally {
            try {
                if (this.readRaf != null) {
                    this.readRaf.close();
                    this.readRaf = null;
                }


                if (this.writeRaf != null) {
                    this.writeRaf.close();
                    this.writeRaf = null;
                }
            } catch (IOException ie2) {
                throw ie2;
            }
        }
    }
}
