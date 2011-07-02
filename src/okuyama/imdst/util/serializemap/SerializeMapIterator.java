package okuyama.imdst.util.serializemap;

import java.util.*;
import okuyama.imdst.util.*;
/**
 * Iterator実装.<br>
 *
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class SerializeMapIterator implements Iterator {

    private Iterator myIte = null;

    private SerializeMap serializeMap = null;

    private Iterator nowChildIterator = null;
    private Map nowChildMap = null;
    private Map.Entry nowEntry = null;

    // コンストラクタ
    public SerializeMapIterator(Iterator myIte, SerializeMap serializeMap) {

        this.myIte = myIte;
        this.serializeMap = serializeMap;
    }




    /**
     * hasNext<br>
     *
     */
    public boolean hasNext() {
        boolean ret = false;
        if (nowChildIterator != null && nowChildIterator.hasNext() == false && this.myIte.hasNext() == false) return false;

        if ((nowChildIterator != null && nowChildIterator.hasNext() == false) || (nowChildIterator == null && this.myIte.hasNext() == true)) {
            while(this.myIte.hasNext()) {

                Map.Entry entry = (Map.Entry)this.myIte.next();
                byte[] childMapBytes = (byte[])entry.getValue();
                if (childMapBytes == null) continue;

                this.nowChildMap = this.serializeMap.dataDeserialize(childMapBytes);

                Set childSet = this.nowChildMap.entrySet();

                this.nowChildIterator = childSet.iterator();

                if (this.nowChildIterator.hasNext()) {
                    return true;
                }
            }
        } else if(this.nowChildIterator != null && this.nowChildIterator.hasNext() == true) {
            return true;
        } else if (this.nowChildIterator == null && this.myIte.hasNext() == false){
            ret = false;
        }

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
        
        this.serializeMap.w.lock();
        try { 
            
            this.serializeMap.remove(this.nowEntry.getKey());
        } finally {
            this.serializeMap.w.unlock(); 
        }
    }
}
