package okuyama.imdst.client.result;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import okuyama.imdst.client.*;

/**
 * OkuyamaClientのgetMultiTagKeyResultで取得可能なクラス<br>
 * 以下のような構文にてTagを利用して全ての紐付くKeyとValueを取得する<br>
 * その際に複数のTagを指定してAND,ORのどちらかを指定可能<br>
 * -----------------------------------------------------------------
 * OkuyamaResultSet resultSet = client.getTagKeyResult(<String[]>tagStrList, true);
 * 
 * while(resultSet.next()) {
 *     System.out.println("Key=" + (Object)resultSet.getKey());
 *     System.out.println("Value=" + (Object)resultSet.getValue());
 * }
 * resultSet.close();
 * ------------------------------------------------------------------
 * 
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */ 
public class OkuyamaMultiTagKeysResultSet implements OkuyamaResultSet {

    protected int maxMultiGetSize = 100;

    protected OkuyamaClient client = null;

    protected String[] tagStrList = null;
    
    protected Map<String, String[]> tagIndexListMap = null;

    protected String encoding = null;

    // false = or, true=and
    protected boolean margeType = false;

    private static final int buketMaxLinkGroupSize = 500000;

    private int nowIndex = 0;

    private boolean closeFlg = true;

    private Map getIndexMap = null;

    private LinkedBlockingQueue indexQueue = null;

    private LinkedBlockingQueue keyQueue = null;

    private LinkedBlockingQueue keyValueQueue = null;

    private String nowKey = null;

    private String nowValue = null;



    /**
     * コンストラクタ.<br>
     *
     * @param client
     * @param tagStrList
     * @param tagIndexListMap
     * @param encoding
     * @param margeType true=AND, false=OR
     */ 
    public OkuyamaMultiTagKeysResultSet(OkuyamaClient client, String[] tagStrList, Map<String, String[]> tagIndexListMap, String encoding, boolean margeType) {
        this.client = client;
        this.tagStrList = tagStrList;
        this.tagIndexListMap = tagIndexListMap;
        this.encoding = encoding;
        this.margeType = margeType;
        this.indexQueue = new LinkedBlockingQueue();
        this.keyQueue = new LinkedBlockingQueue();
        this.keyValueQueue = new LinkedBlockingQueue();
        this.closeFlg = false;
        try {
            // このMapはKeyが全てのTagのIndexから導き出した、buketのIndexの丸めた値(つまり5000000区切りの値)をKeyに、そのIndexのグループないで実際に取得しなければ
            // 行けないTagと本当にそのTagが取得しなければ行けないIndexの値を格納したMapが入っている
            // {0,{"Tag1","0","Tag2","1","Tag3","0"}, 5000000, {"Tag1","500001","Tag2","5000000","Tag3","500000"}}
            // つまり、同じ範囲のKeyはorであれば全て取得対象になり、ANDであればそもそも同じグループないにどれか一つのTagでも対象になければ
            // 取得対象から外れる。取得した後は通常のOR, ANDのロジックで処理すれば目当てのデータがとれる
            this.getIndexMap = new HashMap(300);
            
            for (int idx = 0; idx < this.tagStrList.length; idx++) {

                String[] tmpTagIndexList = (String[])this.tagIndexListMap.get(this.tagStrList[idx]);

                for (int tmpIdx = 0; tmpIdx < tmpTagIndexList.length; tmpIdx++) {
                    Map equalTagGroupMap = (Map)this.getIndexMap.get(new Integer(Integer.parseInt(tmpTagIndexList[tmpIdx]) / buketMaxLinkGroupSize));
                    if (equalTagGroupMap != null) {
                        equalTagGroupMap.put(this.tagStrList[idx], tmpTagIndexList[tmpIdx]);
                    } else {
                        equalTagGroupMap = new HashMap(8);
                        equalTagGroupMap.put(this.tagStrList[idx], tmpTagIndexList[tmpIdx]);
                    }
                    
                    this.getIndexMap.put(new Integer(Integer.parseInt(tmpTagIndexList[tmpIdx]) / buketMaxLinkGroupSize), equalTagGroupMap);
                }
            }
            
            // 上で作成したMapをベースに、Queueに順番に投入する
            Set entrySet = this.getIndexMap.entrySet();
            Iterator entryIte = entrySet.iterator(); 
            while(entryIte.hasNext()) {
    
                Map.Entry obj = (Map.Entry)entryIte.next();
                Map equalTagGroupMap = (Map)obj.getValue();
                this.indexQueue.put(equalTagGroupMap);
            }
            System.out.println(this.indexQueue);

        } catch (Exception e){}
    }

    public boolean next() throws OkuyamaClientException {

        try {
            while (true) {

                if (this.keyValueQueue.size() > 0) {
                    String[] keyValue = (String[])this.keyValueQueue.take();
                    this.nowKey = keyValue[0];
                    this.nowValue = keyValue[1];
                    return true;
                }

                while (this.keyQueue.size() > 0) {
                    List keys = new ArrayList(maxMultiGetSize);
                    for (int idx = 0; idx < maxMultiGetSize; idx++) {
                        String tmpKey = (String)this.keyQueue.poll();
                        if (tmpKey == null) break;
                        keys.add(tmpKey);
                    }

                    if (keys.size() > 0) {
                        String[] getMultiKeys = new String[keys.size()];

                        for (int idx = 0; idx < keys.size(); idx++) {
                            getMultiKeys[idx] = (String)keys.get(idx);
                        }
                        Map keyValueRetMap = null;
                        if (getMultiKeys.length > 1) {
                            keyValueRetMap = this.client.getMultiValue(getMultiKeys, encoding);
                        } else {
                            keyValueRetMap = new HashMap();
                            String[] singleGetRet = this.client.getValue(getMultiKeys[0], encoding);
                            if (singleGetRet[0].equals("true")) {
                                keyValueRetMap.put(getMultiKeys[0], singleGetRet[1]);
                            }
                        }

                        if (keyValueRetMap != null && keyValueRetMap.size() > 0) {


                            Set entrySet = keyValueRetMap.entrySet();
                            Iterator entryIte = entrySet.iterator(); 
                            while(entryIte.hasNext()) {

                                Map.Entry obj = (Map.Entry)entryIte.next();
                                String[] keyValueTmp = new String[2];
                                keyValueTmp[0] = (String)obj.getKey();
                                keyValueTmp[1] = (String)obj.getValue();
                                this.keyValueQueue.put(keyValueTmp);
                            }
                        }
                    }

                    if (this.keyValueQueue.size() > 0) break;
                }
                if (this.keyValueQueue.size() > 0) continue;

                while (this.indexQueue.size() > 0) {

                    Map buketIdxStrMap = (Map)this.indexQueue.take();
                    // ANDの場合はどれか一つでも対象のTagが含まれていない場合はここでcontiune;
                    if (buketIdxStrMap.size() != this.tagStrList.length) continue;
                    
                    // AND,ORに合わせてそれにマッチするKeyのみが格納されたMap<String, null>
                    Map margeKeyMap = new HashMap(1000);
                    
                    for (int idx = 0; idx < this.tagStrList.length; idx++) {
                        if (this.margeType) {
                            // AND
                            Object[] bucketKeysRet = this.client.getTargetIndexTagKeys(this.tagStrList[idx], (String)buketIdxStrMap.get(this.tagStrList[idx]));
                            if (bucketKeysRet[0].equals("true")) {
                                String[] keysStrList = (String[])bucketKeysRet[1];
                                for (int buketIdx = 0; buketIdx < keysStrList.length; buketIdx++) {
                                    if (idx == 0) { 
                                        margeKeyMap.put(keysStrList[buketIdx], null);
                                    } else {
                                        if (!margeKeyMap.containsKey(keysStrList[buketIdx])) margeKeyMap.remove(keysStrList[buketIdx]);
                                    }
                                }
                            }
                        } else {
                            // OR
                            Object[] bucketKeysRet = this.client.getTargetIndexTagKeys(this.tagStrList[idx], (String)buketIdxStrMap.get(this.tagStrList[idx]));
                            if (bucketKeysRet[0].equals("true")) {
                                String[] keysStrList = (String[])bucketKeysRet[1];
                                for (int buketIdx = 0; buketIdx < keysStrList.length; buketIdx++) {
                                    margeKeyMap.put(keysStrList[buketIdx], null);
                                }
                            }
                        }
                    }

                    // Marge済みのKeyのMapからKeyを取り出して、それをKeyのQueueに詰める
                    Set entrySet = margeKeyMap.entrySet();
                    Iterator entryIte = entrySet.iterator(); 
                    while(entryIte.hasNext()) {

                        Map.Entry obj = (Map.Entry)entryIte.next();
                        this.keyQueue.put((String)obj.getKey());
                    }
                    if (this.keyQueue.size() > 0) break;
                }

                if (this.keyQueue.size() > 0) continue;
                if (this.indexQueue.size() < 1) break;
            }
        } catch (Exception e) {
            throw new OkuyamaClientException(e);
        }
        return false;
    }

    public Object getKey() throws OkuyamaClientException {
        return this.nowKey;
    }

    public Object getValue() throws OkuyamaClientException {
        return this.nowValue;
    }

    public void close() throws OkuyamaClientException {
        try {
            if (this.client != null) {

                this.client.close();
                this.client = null;
                this.tagStrList = null;
                this.tagIndexListMap = null;
                this.nowIndex = -1;
                this.indexQueue = null;
                this.keyQueue = null;
                this.keyValueQueue = null;
                this.getIndexMap = null;

            }
            this.closeFlg = true;
        } catch (Exception e) {
            throw new OkuyamaClientException(e);
        }

    }

    public boolean isClose(){
        return this.closeFlg;
    }
}