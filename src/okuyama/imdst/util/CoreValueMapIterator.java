package okuyama.imdst.util;

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

    private boolean urgentSaveMode = false;
    private Iterator urgentSaveMyIte = null;
    private ICoreValueConverter urgentSaveConverter = null;

    // コンストラクタ
    public CoreValueMapIterator(Iterator myIte, ICoreValueConverter converter) {
        this.converter = converter;
        this.myIte = myIte;
    }


    // コンストラクタ
    // 仮想ストレージ起動モード
    public CoreValueMapIterator(Iterator myIte, Iterator urgentSaveMyIte, ICoreValueConverter converter, ICoreValueConverter urgentSaveConverter) {

        this.converter = converter;
        this.myIte = myIte;

        this.urgentSaveMode = true;
        this.urgentSaveMyIte = urgentSaveMyIte;
        this.urgentSaveConverter = urgentSaveConverter;
    }


    /**
     * hasNext<br>
     *
     */
    public boolean hasNext() {

        // 仮想ストレージモードが起動しているかを確認
        if (!this.urgentSaveMode) {
            return this.myIte.hasNext();
        } else {
            // 仮想ストレージモードが起動している場合は、myIteがfalseを返してもurgentSaveMyIteがfalseを返さない限りはtrueを返す
            if (this.myIte.hasNext()) {
                return true;
            } else {
                if (this.urgentSaveMyIte.hasNext()) {
                    return true;
                } else {
                    return false;
                }
            }
        }
    }


    /**
     * next<br>
     *
     */
    public Map.Entry next() {
        // 仮想ストレージモードが起動しているかを確認
        if (!this.urgentSaveMode) {

            Map.Entry entry = (Map.Entry)this.myIte.next();
            if (entry == null) return null;

            CoreValueMapEntry coreValueMapEntry = new CoreValueMapEntry(entry, this.converter);
            return coreValueMapEntry;
        } else {
            if (this.myIte.hasNext()) {

                Map.Entry entry = (Map.Entry)this.myIte.next();
                if (entry == null) return null;

                CoreValueMapEntry coreValueMapEntry = new CoreValueMapEntry(entry, this.converter);
                return coreValueMapEntry;
            } else {
                if (this.urgentSaveMyIte.hasNext()) {

                    Map.Entry entry = (Map.Entry)this.urgentSaveMyIte.next();
                    if (entry == null) return null;

                    CoreValueMapEntry coreValueMapEntry = new CoreValueMapEntry(entry, this.urgentSaveConverter);
                    return coreValueMapEntry;
                } else {

                    Map.Entry entry = (Map.Entry)this.urgentSaveMyIte.next();
                    if (entry == null) return null;

                    CoreValueMapEntry coreValueMapEntry = new CoreValueMapEntry(entry, this.urgentSaveConverter);
                    return coreValueMapEntry;
                }
            }
        }
    }


    /**
     * remove<br>
     *
     */
    public void remove() {
        // 仮想ストレージモードが起動しているかを確認
        if (!this.urgentSaveMode) {

            this.myIte.remove();
        } else {

            this.myIte.remove();
            this.urgentSaveMyIte.remove();
        }
    }
}
