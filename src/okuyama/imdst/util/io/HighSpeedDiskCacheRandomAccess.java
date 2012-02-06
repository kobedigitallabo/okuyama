package okuyama.imdst.util.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.*;
import java.util.concurrent.ArrayBlockingQueue;

import okuyama.imdst.util.*;


/**
 * IOのRandomAccessFileのラッパー.<br>
 * 高速なディスクを利用して頻繁に読み込むデータをキャッシュする<br>
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class HighSpeedDiskCacheRandomAccess extends AbstractDataRandomAccess {

    protected Map dataPointMap = null;
    protected DiskCacheManager diskCacheManager = null;
    protected int maxCacheSize = ImdstDefine.maxDiskCacheSize;

    private String cacheFilePath = null;

    private long nowSeekPoint = 0L;

    private long cacheHitCount = 0L;


    public HighSpeedDiskCacheRandomAccess(File target, String type, String cacheFilePath) throws FileNotFoundException {
        super(target, type);
        File cacheFile = null;
        this.cacheFilePath = cacheFilePath;
        if (this.cacheFilePath != null && !this.cacheFilePath.trim().equals("")) {
            cacheFile = new File(cacheFilePath);
            this.diskCacheManager = new DiskCacheManager(maxCacheSize, cacheFile);
            this.diskCacheManager.start();
            System.out.println(" DiskCache Use - CacheFile=[" + cacheFilePath + "] Number of max cache data=" + this.maxCacheSize);
        } else {
            this.diskCacheManager = new DiskCacheManager();
        }
    }

    public void setDataPointMap(Map dataPointMap) {
        this.dataPointMap = dataPointMap;
    }

    public void requestSeekPoint(long seekPoint, int start, int size) {

    }

    public void seek(long seekPoint) throws IOException {
        super.seek(seekPoint);
        this.nowSeekPoint = seekPoint;
    }

    public void write(byte[] data, int start, int size) throws IOException {
        super.write(data, start, size);
        this.diskCacheManager.removeCache(this.nowSeekPoint);
    }


    public int seekAndRead(long seekPoint, byte[] data, int start, int size, Object key) throws IOException {
        int ret = -1;
        try {
            byte[] cacheData = null;

            cacheData = this.diskCacheManager.getCacheData(seekPoint);

            if (cacheData == null) {

                // キャッシュなし
                super.seek(seekPoint);
                ret = super.read(data, start, size);
                cacheData = new byte[data.length];
                ret = data.length;
                for (int i = 0; i < data.length; i++) {
                    cacheData[i] = data[i];
                }
                this.diskCacheManager.addCacheDarta(seekPoint, cacheData);
            } else {
                //cacheHitCount++;
                //if ((cacheHitCount % 5000) == 0) System.out.println("Cache hit count=" + cacheHitCount + " [" + new Date().toString());
                // キャッシュあり
                for (int i = 0; i < cacheData.length; i++) {
                    data[i] = cacheData[i];
                }
                ret = cacheData.length;
            }

        } catch(IOException ie) {
            throw ie;
        } catch(Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    public void close() throws IOException {
        super.close();
        this.diskCacheManager.endManged();
    }

    class DiskCacheManager extends Thread {

        private boolean runFlg = false;
        private ArrayBlockingQueue writeQueue = new ArrayBlockingQueue(20000);
        private ConcurrentHashMap fixWriteDataMap = new ConcurrentHashMap(20000);
        private DiskBaseCacheMap diskBaseCacheMap = null;
        private Object sync = new Object();

        private boolean noCache = false;

        public DiskCacheManager() {
            this.noCache = true;
        }


        public DiskCacheManager(int maxCacheSize, File dataFile) throws FileNotFoundException {
            this.diskBaseCacheMap = new DiskBaseCacheMap(maxCacheSize, dataFile);
            this.runFlg = true;
        }

        public void run() {
            boolean sleepFlg = false;
            long checkCount = 0L;
            try {
                while(this.runFlg) {
                    checkCount++;
                    if (checkCount > 50) {
                        this.diskBaseCacheMap.existsCacheFile();
                        checkCount = 0L;
                    }
                    if (this.diskBaseCacheMap.errorFlg) {
                        System.out.println("Cache File Error " + new Date().toString());
                        this.noCache = true;
                        Thread.sleep(10000);
                        this.diskBaseCacheMap.clear();
                        if(this.diskBaseCacheMap.errorFlg) continue;
                    }

                    this.noCache = false;
                    if (sleepFlg == true) Thread.sleep(1000);

                    sleepFlg = false;
                    synchronized (this.sync) {
                        Long cacheSeekPoint = (Long)this.writeQueue.poll();
                        if (cacheSeekPoint != null) {
                            byte[] cacheData = (byte[])fixWriteDataMap.remove(cacheSeekPoint);
                            if (cacheData != null) {
                                this.diskBaseCacheMap.put(cacheSeekPoint, cacheData);
                            }
                        } else {
                            sleepFlg = true;
                        }
                    }
                }
            } catch(Exception e) {
                e.printStackTrace();
            } finally {
                this.diskBaseCacheMap.clear();
            }
        }
    

        public void addCacheDarta(long seekPoint, byte[] cacheData) {
            if (this.noCache) return;
            synchronized (this.sync) {
                Long seekPointLong = new Long(seekPoint);
                if(this.writeQueue.offer(seekPointLong)) {
                    this.fixWriteDataMap.put(seekPointLong, cacheData);
                }
            }
        }


        public byte[] getCacheData(long seekPoint) {
            if (this.noCache) return null;
            return (byte[])this.diskBaseCacheMap.get(new Long(seekPoint));
        }

        public void removeCache(long seekPoint) {
            if (this.noCache) return;
            synchronized (this.sync) {
                this.diskBaseCacheMap.remove(new Long(seekPoint));
                this.fixWriteDataMap.remove(seekPoint);
            }
        }

        public void endManged() {
            this.runFlg = false;
        }
    }
}
