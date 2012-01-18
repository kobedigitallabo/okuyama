package okuyama.imdst.util.persistentmap;

import java.util.*;
import okuyama.imdst.util.*;

/**
 * Iterator実装.<br>
 *
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class PersistentMapIterator implements Iterator {

    private Iterator myIte = null;

    private PersistentMap persistentMap = null;

    private Iterator nowChildIterator = null;
    private Map nowChildMap = null;
    private Map.Entry nowEntry = null;

    // コンストラクタ
    public PersistentMapIterator(Iterator myIte, PersistentMap persistentMap) {

        this.myIte = myIte;
        this.persistentMap = persistentMap;
    }




    /**
     * hasNext<br>
     *
     */
    public boolean hasNext() {
        boolean ret = false;

        return false;
    }


    /**
     * next<br>
     *
     */
    public Map.Entry next() {

        this.nowEntry = (Map.Entry)this.nowChildIterator.next();

        if (this.nowEntry == null) return null;

        Object key = this.nowEntry.getKey();
        return this.nowEntry;
    }


    /**
     * remove<br>
     *
     */
    public void remove() {
        
        this.persistentMap.w.lock();
        try { 
            
            this.persistentMap.remove(this.nowEntry.getKey());
        } finally {
            this.persistentMap.w.unlock(); 
        }
    }
}
