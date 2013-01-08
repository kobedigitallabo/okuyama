package fuse.okuyamafs;


import java.util.*;
import java.util.concurrent.locks.*;

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
public class ExpireCacheMap extends LinkedHashMap {

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();


    private int maxCacheSize = 8192;
    private long cacheExpireTime = 15001L;

    private OkuyamaClientFactory factory = null;

    private boolean compress = true;


    /**
     * コンストラクタ.<br>
     * キャッシュ数は8192個<br>
     * キャッシュ有効時間は15秒<br>
     *
     */
    public ExpireCacheMap() {
        super(1024, 0.75f, true);
    }


    /**
     * コンストラクタ.<br>
     * キャッシュ有効時間は15秒<br>
     *
     * @param maxCacheCapacity 最大キャッシュ数
     */
    public ExpireCacheMap(int maxCacheCapacity) {
        super(maxCacheCapacity, 0.75f, true);
        maxCacheSize = maxCacheCapacity;
    }


    /**
     * コンストラクタ.<br>
     * キャッシュ有効時間は15秒<br>
     *
     * @param maxCacheCapacity 最大キャッシュ数
     * @param expireTime 最大キャッシュ有効時間(ミリ秒/単位)
     */
    public ExpireCacheMap(int maxCacheCapacity, long expireTime) {
        super(maxCacheCapacity, 0.75f, true);
        maxCacheSize = maxCacheCapacity;
        this.cacheExpireTime = expireTime;
    }

    /**
     * コンストラクタ.<br>
     * キャッシュ有効時間は15秒<br>
     *
     * @param maxCacheCapacity 最大キャッシュ数
     * @param expireTime 最大キャッシュ有効時間(ミリ秒/単位)
     */
    public ExpireCacheMap(int maxCacheCapacity, long expireTime, boolean compress) {
        super(maxCacheCapacity, 0.75f, true);
        maxCacheSize = maxCacheCapacity;
        this.cacheExpireTime = expireTime;
        this.compress = compress;
    }

    /**
     * コンストラクタ.<br>
     * キャッシュ有効時間は15秒<br>
     *
     * @param maxCacheCapacity 最大キャッシュ数
     * @param expireTime 最大キャッシュ有効時間(ミリ秒/単位)
     */
    public ExpireCacheMap(int maxCacheCapacity, long expireTime, OkuyamaClientFactory factory) {
        super(maxCacheCapacity, 0.75f, true);
        maxCacheSize = maxCacheCapacity;
        this.cacheExpireTime = expireTime;
        this.factory = factory;
    }

    /**
     * コンストラクタ.<br>
     * キャッシュ有効時間は15秒<br>
     *
     * @param maxCacheCapacity 最大キャッシュ数
     * @param expireTime 最大キャッシュ有効時間(ミリ秒/単位)
     */
    public ExpireCacheMap(int maxCacheCapacity, long expireTime, OkuyamaClientFactory factory, boolean compress) {
        super(maxCacheCapacity, 0.75f, true);
        maxCacheSize = maxCacheCapacity;
        this.cacheExpireTime = expireTime;
        this.factory = factory;
        this.compress = compress;
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
            if (value instanceof byte[]) {
                Object[] valSt = new Object[2];
                if (this.compress) {
                    valSt[0] = SystemUtil.dataCompress((byte[])value);
                } else {
                    valSt[0] = (byte[])value;
                }
                valSt[1] = new Long(System.currentTimeMillis());
                return super.put(key, valSt);
            } else {
                Object[] valSt = new Object[2];
                valSt[0] = value;
                valSt[1] = new Long(System.currentTimeMillis());
                return super.put(key, valSt);
            }
        } finally {
            w.unlock(); 
        }
    }


    /**
     * set<br>
     *
     * @param key
     * @param value
     */
    public Object put(Object key, Object value, Integer realStartPoint, long cacheSetTime) {
        w.lock();
        try {
            Object[] valSt = (Object[])super.get(key);
            if (valSt != null) {
                byte[] replaceBytes = null;
                if (this.compress) {
                    replaceBytes = SystemUtil.dataDecompress((byte[])valSt[0]);
                } else {
                    replaceBytes = (byte[])valSt[0];
                }
                int realStartPointInt = realStartPoint.intValue();
                System.arraycopy((byte[])value, realStartPointInt, replaceBytes, realStartPointInt, (((byte[])value).length - realStartPointInt));
                value = replaceBytes;

                if (valSt.length > 2) {
                    int beforeRealStP  = ((Integer)valSt[2]).intValue();

                    if (realStartPointInt > realStartPointInt) {
                        realStartPoint = (Integer)valSt[2];
                    } else {
                        realStartPoint = realStartPoint;
                    }
                }
            }

            valSt = new Object[3];
            if (this.compress) {
                valSt[0] = SystemUtil.dataCompress((byte[])value);
            } else {
                valSt[0] = (byte[])value;
            }
            valSt[1] = new Long(cacheSetTime);
            valSt[2] = realStartPoint;
            return super.put(key, valSt);
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
            Object[] valSt = (Object[])super.get(key);
            if (valSt == null) return false;
            Long cacheTime = (Long)valSt[1];

            // 15秒経過していたら無効
            if ((System.currentTimeMillis() - cacheTime.longValue()) < cacheExpireTime) {
                return true;
            } else {
                super.remove(key);
                return false;
            }
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
        if (!super.containsKey(key)) return null;
        r.lock();
        try { 
            Object[] valSt = (Object[])super.get(key);

            if (valSt == null) return null;
            Long cacheTime = (Long)valSt[1];

            // 15秒経過していたら無効

            if (valSt.length < 3) {
                if ((System.currentTimeMillis() - cacheTime.longValue()) < cacheExpireTime) {
                    if (valSt[0] instanceof byte[]) {
                        if (this.compress) {
                            return SystemUtil.dataDecompress((byte[])valSt[0]);
                        } else {
                            return (byte[])valSt[0];
                        }
                    } else {
                        return valSt[0];
                    }
                } else {
                    super.remove(key);
                    return null;
                }
            } else {

                Integer realStartPoint = (Integer)valSt[2];
                OkuyamaClient client = this.factory.getClient(300*1000);

                Object[] replaceRet = client.readByteValue((String)key);
                byte[] value = null;
                if (this.compress) {
                    value = SystemUtil.dataDecompress((byte[])valSt[0]);
                } else {
                    value = (byte[])valSt[0];
                }

                byte[] replaceBytes = null;
                if (replaceRet[0].equals("true")) {
                    // データ有り
                    replaceBytes = (byte[])replaceRet[1];
                    if (replaceBytes != null) {

                        int realStartPointInt = realStartPoint.intValue();
                        System.arraycopy(value, realStartPointInt, replaceBytes, realStartPointInt, (value.length - realStartPointInt));
                    } else {
                        replaceBytes = value;
                    }
                }
                if (replaceBytes == null) return null;
                valSt = new Object[2];
                if (this.compress) {
                    valSt[0] = SystemUtil.dataCompress((byte[])replaceBytes);
                } else {
                    valSt[0] = (byte[])replaceBytes;
                }
                valSt[1] = cacheTime;
                super.put(key, valSt);
                return replaceBytes;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
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

            Object ret = super.remove(key);

            return ret;
        } finally {
            w.unlock(); 
        }
    }

    /**
     * remove<br>
     * バイトデータをokuyamaに反映する間の一時的なキャッシュを消し込む
     *
     * @param key
     * @return Object
     */
    public void removeStoreTmpCache(Object key, long storeTime) {
        w.lock();
        try {
            Object[] cache = (Object[])super.get(key);
            if (cache != null) {

                Long cacheSetTime = (Long)cache[1];
                if (cacheSetTime.longValue() == storeTime) {
                    super.remove(key);
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
            super.clear();
        } finally {
            w.unlock(); 
        }
    }


    /**
     * 削除指標実装.<br>
     */
    protected boolean removeEldestEntry(Map.Entry eldest) {

        Object[] valSt = (Object[])eldest.getValue();
        if (valSt != null && valSt.length == 3) return false;

        if (maxCacheSize < super.size()) {
            return true;
        }

        if (valSt == null) return true;
        Long cacheTime = (Long)valSt[1];

        // 15秒経過していたら無効
        if ((System.currentTimeMillis() - cacheTime.longValue()) < cacheExpireTime) {
            return false;
        } else {
            return true;
        }
    }
}