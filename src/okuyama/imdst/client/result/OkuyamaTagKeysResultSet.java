package okuyama.imdst.client.result;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import okuyama.imdst.client.*;

/**
 * OkuyamaClientのgetTagKeyResultで取得可能なクラス<br>
 * 以下のような構文にてTagを利用して全ての紐付くKeyとValueを取得する<br>
 * -----------------------------------------------------------------
 * OkuyamaResultSet resultSet = client.getTagKeysResult(tagStr);
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
public class OkuyamaTagKeysResultSet implements OkuyamaResultSet {

    protected int maxMultiGetSize = 100;

    protected OkuyamaClient client = null;

    protected String tagStr = null;

    protected String[] indexList = null;

    protected String encoding = null;

    private int nowIndex = 0;

    private boolean closeFlg = true;

    private LinkedBlockingQueue indexQueue = null;

    private LinkedBlockingQueue keyQueue = null;

    private LinkedBlockingQueue keyValueQueue = null;

    private String nowKey = null;

    private String nowValue = null;



    /**
     * コンストラクタ.<br>
     *
     * @param client
     * @param tagStr
     * @param indexList
     */ 
    public OkuyamaTagKeysResultSet(OkuyamaClient client, String tagStr, String[] indexList, String encoding) {
        this.client = client;
        this.tagStr = tagStr;
        this.indexList = indexList;
        this.encoding = encoding;
        this.indexQueue = new LinkedBlockingQueue();
        this.keyQueue = new LinkedBlockingQueue();
        this.keyValueQueue = new LinkedBlockingQueue();
        this.closeFlg = false;
        try {

            for (int idx = 0; idx < this.indexList.length; idx++) {
                System.out.println(this.indexList[idx]);
                this.indexQueue.put(this.indexList[idx]);
            }
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

                    String buketIdxStr = (String)this.indexQueue.take();
                    Object[] bucketKeysRet = this.client.getTargetIndexTagKeys(this.tagStr, buketIdxStr);
                    if (bucketKeysRet[0].equals("true")) {
                        String[] keysStrList = (String[])bucketKeysRet[1];
                        for (int idx = 0; idx < keysStrList.length; idx++) {
                            this.keyQueue.put(keysStrList[idx]);
                        }
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
                this.tagStr = null;
                this.indexList = null;
                this.nowIndex = -1;
                this.indexQueue = null;
                this.keyQueue = null;
                this.keyValueQueue = null;

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