package okuyama.imdst.util.serializemap;


import java.util.*;


/**
 * AbstractSet拡張.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class SerializeMapSet extends AbstractSet implements Set {

    private Set set = null;

    private SerializeMap serializeMap = null;

    // コンストラクタ
    public SerializeMapSet(Set set, SerializeMap map) {
        this.set = set;
        this.serializeMap = map;
    }


    public int size() {
        return this.serializeMap.size();
    }


    public Iterator iterator() {

        return new SerializeMapIterator(this.set.iterator(), this.serializeMap);
    }
}
