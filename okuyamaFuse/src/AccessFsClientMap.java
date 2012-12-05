package fuse.okuyamafs;


import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

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
public class AccessFsClientMap {

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();

    private static Map accessFhMap = new ConcurrentHashMap(10000);


    /**
     * set<br>
     *
     * @param key
     * @param value
     */
    public void addAccessFh(String file, Object value) {
        w.lock();
        try {
            Map accessFh = null;
            accessFh = (Map)this.accessFhMap.get(file);
            if (accessFh == null) {
                accessFh = new LinkedHashMap();
            }
            accessFh.put(value, file);

            this.accessFhMap.put(file, accessFh);
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
    public boolean containsKey(String file) {
        r.lock();
        try { 
            return this.accessFhMap.containsKey(file);
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
    public Object[] getAccessFh(String file) {
        r.lock();
        try {
            Map accessFh = (Map)this.accessFhMap.get(file);

            if (accessFh == null) return null;

            Object[] fhList = new Object[accessFh.size()];
            int idx = 0;

            Set entrySet = accessFh.entrySet();
            Iterator entryIte = entrySet.iterator(); 
            while(entryIte.hasNext()) {

                Map.Entry obj = (Map.Entry)entryIte.next();
                fhList[idx] = obj.getKey();
                idx++;
            }
            return fhList;
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
    public void removeAccessFh(String file, Object value) {
        w.lock();
        try {

            Map accessFh = (Map)this.accessFhMap.get(file);
            if (accessFh != null) {
                accessFh.remove(value);
                if (accessFh.size() > 0) {
                    this.accessFhMap.put(file, accessFh);
                } else {
                    this.accessFhMap.remove(file);
                }
            }
        } finally {
            w.unlock(); 
        }
    }
    
    
    /**
     * clear<br>
     *
     */
    public void clear() {
        w.lock();
        try { 
            this.accessFhMap.clear();
        } finally {
            w.unlock(); 
        }
    }
}

