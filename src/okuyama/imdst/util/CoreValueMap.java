package okuyama.imdst.util;


import java.util.*;
import java.io.*;
import java.util.concurrent.ConcurrentHashMap;

import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.base.lang.BatchException;
import okuyama.imdst.util.StatusUtil;

import com.sun.mail.util.BASE64DecoderStream;
import com.sun.mail.util.BASE64EncoderStream;

/**
 * ConcurrentHashMap拡張.<br>
 * メモリが足りない対応いれたい.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class CoreValueMap extends AbstractMap implements Cloneable, Serializable {

    private boolean fileWrite = false;

    private ICoreValueConverter converter = null;

    private AbstractMap mainMap = null;

    // コンストラクタ
    public CoreValueMap(int size, int upper, int multi, boolean memoryMode) {

        if (memoryMode) {

            mainMap  = new ConcurrentHashMap(size, upper, multi);
            converter = new MemoryModeCoreValueCnv();
        } else {

            mainMap  = new ConcurrentHashMap(size, upper, multi);
            converter = new PartialFileModeCoreValueCnv();
        }
    }


    // コンストラクタ
    public CoreValueMap(String[] dirs, int numberOfDataSize) {

        mainMap  = new FileBaseDataMap(dirs, numberOfDataSize);
        converter = new PartialFileModeCoreValueCnv();
    }


    /**
     * set<br>
     *
     * @param key
     * @param value
     */
    public Object put(Object key, Object value) {
        return mainMap.put(converter.convertEncodeKey(key), converter.convertEncodeValue(value));
    }


    /**
     * get<br>
     *
     * @param key
     * @return Object
     */
    public Object get(Object key) {
        return converter.convertDecodeValue(mainMap.get(converter.convertEncodeKey(key)));
    }


    /**
     * remove<br>
     *
     * @param key
     * @return Object
     */
    public Object remove(Object key) {
        return converter.convertDecodeValue(mainMap.remove(converter.convertEncodeKey(key)));
    }


    /**
     * containsKey<br>
     *
     * @param key
     * @return boolean
     */
    public boolean containsKey(Object key) {
        return mainMap.containsKey(converter.convertEncodeKey(key));
    }


    /**
     * clear<br>
     *
     */
    public void clear() {
        this.mainMap.clear();
    }


    /**
     * size.<br>
     *
     * @param
     * @return int
     * @throws
     */
    public int size() {
        return this.mainMap.size();
    }
    
    
    /**
     * entrySet<br>
     *
     * @return Set
     */
    public Set entrySet() {
        return new CoreValueMapSet(mainMap.entrySet(), converter);
    }
}
