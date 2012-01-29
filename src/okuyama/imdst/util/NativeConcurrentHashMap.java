package okuyama.imdst.util;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * okuyamaデータ保存用のConcurrentHashMapラッパー<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class NativeConcurrentHashMap extends ConcurrentHashMap implements Cloneable, Serializable, ICoreStorage {

    public NativeConcurrentHashMap() {
        super();
    }

    public NativeConcurrentHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    public NativeConcurrentHashMap(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    public NativeConcurrentHashMap(int initialCapacity, float loadFactor, int concurrencyLevel) {
        super(initialCapacity, loadFactor, concurrencyLevel);
    }

    public NativeConcurrentHashMap(Map m) {
        super(m);
    }
}