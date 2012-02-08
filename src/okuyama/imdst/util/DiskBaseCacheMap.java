package okuyama.imdst.util;

import java.util.*;
import java.io.*;

import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.ArrayBlockingQueue;



/**
 * LinkedHashMapを継承して読み出し用途専用のLRUキャッシュを実現.<br>
 * 主にFileMode時のValueのキャッシュに利用.<br>
 * putされたKeyはメモリに、Valueはコンストラクタで指定したファイルに<br>
 * 書き出される.<br>
 * 読み出しは都度ディスクからの読み出しとなるため、キャッシュ元のデータが<br>
 * メモリなどの場合は本キャッシュでの高速化は期待できない<br>
 * キャッシュ元のデータがHDDなどの読み出しが低速デバイスの場合は<br>
 * 本キャッシュのデータ配置場所をNAND型フラッシュメモリなどにすることで<br>
 * 高速化を図ることが出来る.<br>
 * なお、書き込みは低速であるため、外部でバックグラウンド書き込みなどの工夫が必要となる.<br>
 * 全てのメソッドはスレッドセーフに実装されている.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class DiskBaseCacheMap extends LinkedHashMap {

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();

    private RandomAccessFile raf = null;
    private ArrayBlockingQueue freeCacheSpaceQueue = null;

    private String cacheStoreFilePath = null;
    private File cacheStoreFile = null;

    private int maxCacheSize = 8192;

    // 自身を維持できないエラーが発生した場合にtrueとなる
    public boolean errorFlg = false;


    // コンストラクタ
    public DiskBaseCacheMap(int maxCacheCapacity, File cacheFile) throws FileNotFoundException {
        super(maxCacheCapacity, 0.75f, true);
        this.maxCacheSize = maxCacheCapacity;
        this.cacheStoreFile = cacheFile;
        this.freeCacheSpaceQueue = new ArrayBlockingQueue(this.maxCacheSize);
        this.raf = new RandomAccessFile(this.cacheStoreFile, "rw");
        long freeSpacePoint = 0L;

        for (long i = 0; i < this.maxCacheSize; i++) {

            this.freeCacheSpaceQueue.offer(new Long(freeSpacePoint));
            freeSpacePoint = freeSpacePoint + ImdstDefine.dataFileWriteMaxSize;
        }
        this.cacheStoreFilePath = this.cacheStoreFile.getAbsolutePath();
    }


    /**
     * set<br>
     *
     * @param key
     * @param value
     */
    public Object put(Object key, Object value) {
        if (freeCacheSpaceQueue.size() < 1) return null;
        w.lock();
        try {
            if(!super.containsKey(key)) {

                Long freeSpacePoint = (Long)freeCacheSpaceQueue.poll();

                if (freeSpacePoint != null) {

                    this.raf.seek(freeSpacePoint.longValue());
                    this.raf.write((byte[])value);
                    return super.put(key, freeSpacePoint);
                }
            }

        } catch(Exception e) {
            this.errorFlg = true;
            e.printStackTrace();
        } finally {
            w.unlock(); 
        }
        return null;
    }


    /**
     * containsKey<br>
     *
     * @param key
     * @return boolean
     */
    public boolean containsKey(Object key) {
        r.lock();
        try { 
            return super.containsKey(key);
        } finally { 
            r.unlock(); 
        }
    }


    /**
     * get<br>
     *
     * @param key
     * @return Object
     */
    public Object get(Object key) {
        w.lock();
        byte[] retData = null;
        try { 
            Long cacheSeekPoint = (Long)super.get(key);

            if (cacheSeekPoint != null) {
                retData = new byte[ImdstDefine.dataFileWriteMaxSize];
                this.raf.seek(cacheSeekPoint.longValue());
                this.raf.read(retData);
            }
        } catch(Exception e) {
            this.errorFlg = true;
            e.printStackTrace();
        } finally { 
            w.unlock(); 
        }
        return retData;
    }


    /**
     * remove<br>
     *
     * @param key
     * @return Object
     */
    public Object remove(Object key) {

        w.lock();
        Object ret = null;
        try {
            ret = super.remove(key);
            if (ret != null) {
                this.freeCacheSpaceQueue.offer((Long)ret);
            }
        } catch (Exception e) {
            this.errorFlg = true;
        } finally {
            w.unlock(); 
        }
        return ret;
    }


    /**
     * clear<br>
     *
     */
    public void clear() {
        w.lock();
        try { 
            super.clear();
            this.freeCacheSpaceQueue = new ArrayBlockingQueue(this.maxCacheSize);

            this.cacheStoreFile.delete();
            this.cacheStoreFile = null;
            this.raf = null;

            this.cacheStoreFile = new File(this.cacheStoreFilePath);
            this.raf = new RandomAccessFile(this.cacheStoreFile, "rw");
            long freeSpacePoint = 0L;

            for (long i = 0; i < this.maxCacheSize; i++) {

                this.freeCacheSpaceQueue.offer(new Long(freeSpacePoint));
                freeSpacePoint = freeSpacePoint + ImdstDefine.dataFileWriteMaxSize;
            }
            this.errorFlg = false;
        } catch (Exception e) {
            this.errorFlg = true;
        } finally {
            w.unlock(); 
        }
    }


    /**
     * データファイルの存在確認用メソッド<br>
     *
     */
    public void existsCacheFile() {
        try { 
            File checkCacheFile = new File(this.cacheStoreFilePath);
            if (!checkCacheFile.exists()) this.errorFlg = true;
        } catch (Exception e) {
            this.errorFlg = true;
        }
    }

    /**
     * 削除指標実装.<br>
     */
    protected boolean removeEldestEntry(Map.Entry eldest) {
        if ((maxCacheSize - 1) < super.size()) {

            Long ret = (Long)super.remove(eldest.getKey());
            if (ret != null) {

                this.freeCacheSpaceQueue.offer(ret);
            }
        }
        return false;
    }
}
