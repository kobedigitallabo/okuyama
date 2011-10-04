package okuyama.imdst.client;

/**
 * OkuyamaClientが一度のリクエストでは取得しきれないような<br>
 * 大量のデータを扱う場合に利用するResultSetクラス.<br>
 * 以下のような構文にてデータを取得する
 * 以下は、Tagを利用して全ての紐付くKeyとValueを出力している
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
public interface OkuyamaResultSet {

    public boolean next() throws OkuyamaClientException;

    public Object getKey() throws OkuyamaClientException;

    public Object getValue() throws OkuyamaClientException;

    public void close() throws OkuyamaClientException;

    public boolean isClose() throws OkuyamaClientException;
}