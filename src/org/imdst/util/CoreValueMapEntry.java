package org.imdst.util;


import java.util.*;

import com.sun.mail.util.BASE64DecoderStream;
import com.sun.mail.util.BASE64EncoderStream;

/**
 * Map.Entry実装.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class CoreValueMapEntry implements Map.Entry {

    private ICoreValueConverter converter = null;
    private Map.Entry map = null;

    // コンストラクタ
    public CoreValueMapEntry(Map.Entry map, ICoreValueConverter converter) {
        this.map = map;
        this.converter = converter;
    }


    /**
     * equals<br>
     *
     */
    public boolean equals(Object o) {
        return this.map.equals(o);
    }


    /**
     * getKey<br>
     *
     */
    public Object getKey() {
        return this.converter.convertDecodeKey(this.map.getKey());
    }


    /**
     * getValue<br>
     *
     */
    public Object getValue() {
        return this.converter.convertDecodeValue(this.map.getValue());
    }

    /**
     * hashCode<br>
     *
     */
    public int hashCode() {
        return this.map.hashCode();
    }

    /**
     * hashCode<br>
     *
     */
    public Object setValue(Object o) {
        return this.map.setValue(this.converter.convertEncodeValue(o));
    }


}
