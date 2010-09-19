package org.imdst.util;


import java.util.*;
import java.io.*;
import java.util.concurrent.ConcurrentHashMap;

import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.batch.lang.BatchException;
import org.imdst.util.StatusUtil;

import com.sun.mail.util.BASE64DecoderStream;
import com.sun.mail.util.BASE64EncoderStream;

/**
 * ConcurrentHashMap拡張.<br>
 * メモリが足りない対応いれたい.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class CoreValueMap extends ConcurrentHashMap implements Cloneable, Serializable {

    private boolean fileWrite = false;

    private ICoreValueConverter converter = null;


    // コンストラクタ
    public CoreValueMap(int size, int upper, int multi, boolean memoryMode) {
        super(size, upper, multi);
        if (memoryMode) {
            converter = new MemoryModeCoreValueCnv();
        } else {
            converter = new FileModeCoreValueCnv();
        }
    }


    /**
     * set<br>
     *
     * @param key
     * @param value
     */
    public Object put(Object key, Object value) {
        return super.put(converter.convertEncodeKey(key), converter.convertEncodeValue(value));
    }


    /**
     * get<br>
     *
     * @param key
     * @return Object
     */
    public Object get(Object key) {
        return converter.convertDecodeValue(super.get(converter.convertEncodeKey(key)));
    }


    /**
     * remove<br>
     *
     * @param key
     * @return Object
     */
    public Object remove(Object key) {
        return converter.convertDecodeValue(super.remove(converter.convertEncodeKey(key)));
    }


    /**
     * containsKey<br>
     *
     * @param key
     * @return boolean
     */
    public boolean containsKey(Object key) {
        return super.containsKey(converter.convertEncodeKey(key));
    }


    /**
     * containsKey<br>
     *
     * @param key
     * @return boolean
     */
    public Set entrySet() {
        return new CoreValueMapSet(super.entrySet(), converter);
    }
}
