package org.imdst.util;


import java.util.*;
import java.io.*;
import java.util.concurrent.ConcurrentHashMap;

import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.batch.lang.BatchException;
import org.imdst.util.StatusUtil;

/**
 * ConcurrentHashMap拡張.<br>
 * メモリが足りない対応いれたい.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class CoreValueMap extends ConcurrentHashMap implements Cloneable, Serializable {

    private boolean fileWrite = false;

    // コンストラクタ
    public CoreValueMap(int size, int upper, int multi) {
        super(size, upper, multi);
    }

    /**
     * set<br>
     *
     * @param key
     * @param value
     */
    public Object put(Object key, Object value) {
        return super.put(key, value);
    }


    /**
     * get<br>
     *
     * @param key
     * @return Object
     */
    public Object get(Object key) {
        return super.get(key);
    }


    /**
     * remove<br>
     *
     * @param key
     * @return Object
     */
    public Object remove(Object key) {
        return super.remove(key);
    }
}
