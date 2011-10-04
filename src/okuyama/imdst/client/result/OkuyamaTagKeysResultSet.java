package okuyama.imdst.client.result;

import java.util.concurrent.LinkedBlockingQueue;

import okuyama.imdst.client.*;

/**
 * OkuyamaClientのgetTagKeyResultで取得可能なクラス<br>
 * 以下のような構文にてTagを利用して全ての紐付くKeyとValueを取得する<br>
 * -----------------------------------------------------------------
 * OkuyamaResultSet resultSet = client.getTagKeyResult(tagStr);
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

    protected OkuyamaClient client = null;

    protected String tagStr = null;

    protected String[] indexList = null;

    protected String encoding = null;

    private int nowIndex = 0;

    private boolean closeFlg = true;


    private LinkedBlockingQueue keyQueue = null;

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
        this.keyQueue = new LinkedBlockingQueue();
        this.closeFlg = false;
    }

    public boolean next() throws OkuyamaClientException {
        

        return false;
    }

    public Object getKey() throws OkuyamaClientException {
        return null;
    }

    public Object getValue() throws OkuyamaClientException {
        return null;
    }

    public void close() throws OkuyamaClientException {
        try {
            if (this.client != null) {

                this.client.close();
                this.client = null;
                this.tagStr = null;
                this.indexList = null;
                this.nowIndex = -1;
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