package okuyama.imdst.util;

import java.lang.ref.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;


/**
 * SoftReferenceを利用したCache機構.<br>
 * 主にCustomRandomReaderから利用される想定.<br>
 * 
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class SoftRefCacheMap extends Thread {

    private ValueCacheMap innerCacheMap = null;

    private volatile boolean cleanerExec = true;

    // コンストラクタ
    public SoftRefCacheMap() {
        this(1024);
    }


    // コンストラクタ
    public SoftRefCacheMap(int maxCacheCapacity) {
        this.innerCacheMap =  new ValueCacheMap(maxCacheCapacity);
        //this.start();
    }


    /**
     * put<br>
     *
     * @param key
     * @param value
     */
    public void put(Object key, Object value) {
        SoftReference oldRef = (SoftReference)this.innerCacheMap.put(key, new SoftReference(value));
        if (oldRef != null) oldRef.clear();
    }


    /**
     * get<br>
     *
     * @param key
     * @return Object
     */
    public Object get(Object key) {
        Object value = null;
        SoftReference refValue = null;

        // nullであれば存在しない
        if ((refValue = (SoftReference)this.innerCacheMap.get(key)) == null) return null;

        // Referenceは存在するが内容がない場合はgcにて消されてるのでkeyも消す
        if ((value = refValue.get())== null) this.innerCacheMap.remove(key);

        return value;
    }


    /**
     * remove<br>
     *
     * @param key
     * @return Object
     */
    public Object remove(Object key) {
        return this.innerCacheMap.remove(key);
    }


    /**
     * end<br>
     * 本メソッド呼び出した後の本クラスの動きは保障されない.<br>
     * 再利用できない.<br>
     */
    public void end() {
        this.cleanerExec = false;
        this.innerCacheMap.clear();
    }


    /**
     * 動かさない
     */ 
    public void run() {
        boolean exec = true;

        while(exec) {
            try {
                int idx = 0;
                if(innerCacheMap != null) {
                    Set keySet = innerCacheMap.keySet();
                    Iterator keyIte = keySet.iterator();

                    while(keyIte.hasNext()) {
                        Object key = keyIte.next();  
                        SoftReference refValue = (SoftReference)innerCacheMap.get(key);
                        if (refValue.get()== null)this.innerCacheMap.remove(key);

                        if (cleanerExec == false) {
                            exec = false;
                            break;
                        }
                        idx++;
                        if ((idx % 50) == 0) Thread.sleep(50);
                    }
                    Thread.sleep(5000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
