package okuyama.imdst.client;

/**
 * OkuyamaClientが一度のリクエストでは取得しきれないような<br>
 * 大量のデータを扱う場合に利用するResultSetクラス.<br>
 * 以下のような構文にてデータを取得する<br>
 * 以下は、Tagを利用して全ての紐付くKeyとValueを出力している<br>
 * -----------------------------------------------------------------<br>
 * OkuyamaResultSet resultSet = client.getTagKeysResult(tagStr);<br>
 * <br>
 * while(resultSet.next()){<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;System.out.println("Key=" + (Object)resultSet.getKey());<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;System.out.println("Value=" + (Object)resultSet.getValue());<br>
 * }<br>
 * resultSet.close();<br>
 * ------------------------------------------------------------------<br>
 * 
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */ 
public interface OkuyamaResultSet {

    /**
     * データ取得前に呼び出すことで取得位置を1つ進める.<br>
     * OkuyamaResultSetが返された直後はカーソル位置がデータの最初の場所にないため、まず呼び出さなければデータは取得できない<br>
     * カーソルが終端に達し場合はfalseが返却されカーソルが終端ではない場合はtrueが返される
     *
     * @return boolean  カーソルが終端に達し場合はfalse  カーソルが終端ではない場合はtrue
     * @throws OkuyamaClientException なんだかの理由でカーソルの移動に失敗した場合
     */
    public boolean next() throws OkuyamaClientException;

    /**
     * 現在のカーソル位置のKey値を取得する.<br>
     *
     * @return Object Key値
     * @throws OkuyamaClientException なんだかの理由でKey値の取得に失敗した場合
     */
    public Object getKey() throws OkuyamaClientException;

    /**
     * 現在のカーソル位置のValue値を取得する.<br>
     *
     * @return Object Value値
     * @throws OkuyamaClientException なんだかの理由でValue値の取得に失敗した場合
     */
    public Object getValue() throws OkuyamaClientException;

    /**
     * ResultSetを終了する.<br>
     * 利用後は必ず呼び出してリソースを解放する必要がある<br>
     *
     * @throws OkuyamaClientException なんだかの理由でリソースの解放に失敗した場合
     */
    public void close() throws OkuyamaClientException;

    /**
     * このResultSetが終了しているかを確認する.<br>
     *
     * @return boolean true=既に終了している false=終了していない
     */
    public boolean isClose();
}