package okuyama.imdst.util;


import java.util.*;


/**
 * AbstractSet拡張.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class CoreValueMapSet extends AbstractSet implements Set {

    private Set set = null;

    private ICoreValueConverter converter = null;


    // コンストラクタ
    public CoreValueMapSet(Set set, ICoreValueConverter converter) {
        this.set = set;
        this.converter = converter;
    }


    public int size() {
        return set.size();
    }


    public Iterator iterator() {
        return new CoreValueMapIterator(set.iterator(), this.converter);
    }
}
