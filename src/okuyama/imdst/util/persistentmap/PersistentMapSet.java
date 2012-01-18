package okuyama.imdst.util.persistentmap;


import java.util.*;


/**
 * AbstractSet拡張.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class PersistentMapSet extends AbstractSet implements Set {

    private Set set = null;

    private PersistentMap persistentMap = null;

    // コンストラクタ
    public PersistentMapSet(Set set, PersistentMap map) {
        this.set = set;
        this.persistentMap = map;
    }


    public int size() {
        return this.persistentMap.size();
    }


    public Iterator iterator() {

        return new PersistentMapIterator(this.set.iterator(), this.persistentMap);
    }
}
