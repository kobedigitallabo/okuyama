package fuse.okuyamafs;

import java.util.concurrent.locks.*;
import java.util.*;
    

/**
 * OkuyamaFuse.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class LRUCacheMap extends LinkedHashMap {

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();

    private int maxCacheSize = 8192;


    // コンストラクタ
    public LRUCacheMap() {
        super(1024, 0.75f, true);
    }


    // コンストラクタ
    public LRUCacheMap(int maxCacheCapacity) {
        super(maxCacheCapacity, 0.75f, true);
    }


    /**
     * set<br>
     *
     * @param key
     * @param value
     */
    public Object put(Object key, Object value) {
        w.lock();
        try {
            return super.put(key, value);
        } finally {
            w.unlock(); 
        }
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
        r.lock();
        try { 
            return super.get(key); 
        } finally { 
            r.unlock(); 
        }
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
            return super.remove(key);
        } finally {
            w.unlock(); 
        }
    }


    /**
     * 削除指標実装.<br>
     */
    protected boolean removeEldestEntry(Map.Entry eldest) {
        if (maxCacheSize < super.size()) {
            return true;
        }
        return false;
    }
}
