package fuse.okuyamafs;


import java.util.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.*;

import okuyama.imdst.client.*;
import okuyama.imdst.util.*;


/**
 * OkuyamaFuse.<br>
 * LinkedHashMapを継承してLRUキャッシュを実現.<br>
 * このLRUキャッシュは一定のデータ数でも自動的に消えるが、指定したExpireTimeが経過した
 * キャッシュも無効となり消えていく。
 *
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class BufferedDataStoredMap extends Thread {

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();
    private DelayStoreDaemon delayStoreDaemon = null;
    private String[] masterNodeInfo = null;

    private Map dataMap = new ConcurrentHashMap(100000);
    private Queue storeRequestQueue = new ArrayBlockingQueue(3000);

    private String okuyamaTag = "d";

    /**
     * コンストラクタ.<br>
     */
    public BufferedDataStoredMap(String[] masterNodeInfo) {
        this.masterNodeInfo = masterNodeInfo;
        this.start();
    }

    public void run () {
        Object[] requestData = null;

        while (true) {

            try {

                // リクエストがnullの場合だけQueueから取り出す。
                // 正常にokuyamaに伝播した場合、nullとするからである。Exception発生時はnull化されない
                if (requestData == null) {
                    // [0] = type(1=put, 2=remove), [1]=DataType(1=byte, 2=String, 3=Object), [2]=Key, [3]=Value

                    while (true) {
                        requestData = (Object[])this.storeRequestQueue.poll(1000, TimeUnit.MILLISECONDS);

                        if (requestData != null) break;
                        if (OkuyamaFilesystem.jvmShutdownStatus == true) break;
                    }

                    if (requestData == null && OkuyamaFilesystem.jvmShutdownStatus == true) break;
                    if (DelayStoreDaemon.nowQueueJob.get() > 0) DelayStoreDaemon.nowQueueJob.decrementAndGet();
                }
                Object[] request = storeRequestQueue.
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * set<br>
     *
     * @param key
     * @param value
     */
    public Object put(String key, byte[] value) {
        w.lock();
        try {
        } finally {
            w.unlock();
        }
        return null;
    }


    /**
     * set<br>
     *
     * @param key
     * @param value
     */
    public Object put(String key, String value) {
        w.lock();
        try {
        } finally {
            w.unlock();
        }
        return null;
    }

    /**
     * set<br>
     *
     * @param key
     * @param value
     */
    public Object put(String key, Map value) {
        w.lock();
        try {
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
        } finally { 
            r.unlock();
        }
        return false;
    }


    /**
     * get<br>
     *
     * @param key
     * @return Object
     */
    public Object get(Object key) {
        r.lock();
        try {
        } finally { 
            r.unlock();
        }
        return null;
    }


    /**
     * remove<br>
     *
     * @param key
     * @return Object
     */
    public Object remove(Object key) {
        w.lock();
        try {
        } finally {
            w.unlock();
        }
        return null;
    }

    /**
     * clear<br>
     *
     */
    public void clear() {
        w.lock();
        try { 
        
        } finally {
            w.unlock();
        }
    }

}