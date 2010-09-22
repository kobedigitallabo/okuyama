package org.okuyama.imdst.util;

import java.util.*;

/**
 * Iterator実装.<br>
 *
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class CoreValueMapIterator implements Iterator {

    private ICoreValueConverter converter = null;
    private Iterator myIte = null;

    // コンストラクタ
    public CoreValueMapIterator(Iterator myIte, ICoreValueConverter converter) {
        this.converter = converter;
        this.myIte = myIte;
    }


    /**
     * hasNext<br>
     *
     */
    public boolean hasNext() {
        return this.myIte.hasNext();
    }


    /**
     * next<br>
     *
     */
    public Map.Entry next() {
        CoreValueMapEntry coreValueMapEntry = new CoreValueMapEntry((Map.Entry)myIte.next(), this.converter);
        return coreValueMapEntry;
    }


    /**
     * remove<br>
     *
     */
    public void remove() {
        this.myIte.remove();
    }
}
