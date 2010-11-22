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

    // 仮想ストレージモード用
    private boolean urgentSaveMode = false;
    private Set urgentSaveSet = null;
    private ICoreValueConverter urgentSaveConverter = null;

    // コンストラクタ
    public CoreValueMapSet(Set set, ICoreValueConverter converter) {
        this.set = set;
        this.converter = converter;
    }

    // コンストラクタ
    // 仮想ストレージ起動モード
    public CoreValueMapSet(Set set, Set urgentSaveSet, ICoreValueConverter converter, ICoreValueConverter urgentSaveConverter) {

        this.set = set;
        this.converter = converter;

        this.urgentSaveMode = true;
        this.urgentSaveSet = urgentSaveSet;
        this.urgentSaveConverter = urgentSaveConverter;
    }


    public int size() {

        // 仮想ストレージモードが起動しているかを確認
        if (!this.urgentSaveMode) {
            return set.size();
        } else {
            int retSize = set.size();
            retSize = retSize + urgentSaveSet.size();
            return retSize;
        }
    }


    public Iterator iterator() {

        // 仮想ストレージモードが起動しているかを確認
        if (!this.urgentSaveMode) {

            return new CoreValueMapIterator(this.set.iterator(), this.converter);
        } else {
            return new CoreValueMapIterator(this.set.iterator(), this.urgentSaveSet.iterator(), this.converter, this.urgentSaveConverter);
        }
    }
}
