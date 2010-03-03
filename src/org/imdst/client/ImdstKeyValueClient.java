package org.imdst.client;

import java.util.*;
import java.io.*;
import java.net.*;
import java.util.zip.*;


import com.sun.mail.util.BASE64DecoderStream;   
import com.sun.mail.util.BASE64EncoderStream; 

import org.imdst.util.ImdstDefine;

/**
 * MasterNodeと通信を行うプログラムインターフェース<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ImdstKeyValueClient {

    // ソケット
    private Socket socket = null;

    // サーバへの出力用
    private PrintWriter pw = null;

    // サーバからの受信用
    private BufferedReader br = null;

    // データセパレータ文字列
    private static final String sepStr = ImdstDefine.keyHelperClientParamSep;

    // 接続時のデフォルトのエンコーディング
    private static final String connectDefaultEncoding = ImdstDefine.keyHelperClientParamEncoding;

    // ブランク文字列の代行文字列
    private static final String blankStr = ImdstDefine.imdstBlankStrData;

    // 接続要求切断文字列
    private static final String connectExitStr = ImdstDefine.imdstConnectExitRequest;

    // Tagで取得出来るキー値のセパレータ文字列
    private static final String tagKeySep = ImdstDefine.imdstTagKeyAppendSep;

    private static final String byteDataKeysSep = ":#:";


    // バイナリデータ分割保存サイズ
    private int saveSize = ImdstDefine.saveDataMaxSize;

    // 保存できる最大長
    private int maxValueSize = ImdstDefine.saveDataMaxSize;

    /**
     * コンストラクタ
     *
     */
    public ImdstKeyValueClient() {
        // エンコーダ、デコーダの初期化に時間を使うようなので初期化
        BASE64EncoderStream.encode("".getBytes());
        BASE64DecoderStream.decode("".getBytes());
    }

    /**
     * バイナリデータ分割保存サイズを変更<br>
     *
     * @param size サイズ
     */
    public void changeByteSaveSize(int size) {
        saveSize = size;
    }

    /**
     * 接続処理.<br>
     * エンコーディング指定なし.<br>
     *
     * @param server
     * @param port
     * @throws Exception
     */
    public void connect(String server, int port) throws Exception {
        this.connect(server, port, ImdstKeyValueClient.connectDefaultEncoding);
    }

    /**
     * 接続処理.<br>
     * エンコーディング指定有り.<br>
     *
     * @param server
     * @param port
     * @param encoding
     * @throws Exception
     */
    public void connect(String server, int port, String encoding) throws Exception {
        try {
            this.socket = new Socket(server, port);
 
            this.pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), encoding)));
            this.br = new BufferedReader(new InputStreamReader(socket.getInputStream(), encoding));

        } catch (Exception e) {
            try {
                if (this.br != null) {
                    this.br.close();
                    this.br = null;
                }

                if (this.pw != null) {
                    this.pw.close();
                    this.pw = null;
                }

                if (this.socket != null) {
                    this.socket.close();
                    this.socket = null;
                }
            } catch (Exception e2) {
                // 無視
                this.socket = null;
            }
            throw e;
        }
    }


    /**
     * マスタサーバとの接続を切断.<br>
     *
     * @throw Exception
     */
    public void close() throws Exception {
        try {
            if (this.pw != null) {
                // 接続切断を通知
                this.pw.println(connectExitStr);
                this.pw.flush();

                this.pw.close();
                this.pw = null;
            }

            if (this.br != null) {
                this.br.close();
                this.br = null;
            }

            if (this.socket != null) {
                this.socket.close();
                this.socket = null;
            }
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * マスタサーバへデータを送信する.<br>
     * Tagなし.<br>
     *
     * @param keyStr
     * @param value
     * @return boolean
     * @throws Exception
     */
    public boolean setValue(String keyStr, String value) throws Exception {
        return this.setValue(keyStr, null, value);
    }

    /**
     * マスタサーバへデータを送信する.<br>
     * Tag有り.<br>
     *
     * @param keyStr
     * @param tagStrs
     * @param value
     * @return boolean
     * @throws Exception
     */
    public boolean setValue(String keyStr, String[] tagStrs, String value) throws Exception {
        boolean ret = false; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;
//long start2 = 0;
//long end2 = 0;
        try {
            // Byte Lenghtチェック
            if (keyStr.getBytes().length > maxValueSize) throw new Exception("Save Key Max Size " + maxValueSize + " Byte");
            if (tagStrs != null) {
                for (int i = 0; i < tagStrs.length; i++) {
                    if (tagStrs[i].getBytes().length > maxValueSize) throw new Exception("Tag Max Size " + maxValueSize + " Byte");
                }
            }
            if (value.getBytes().length > maxValueSize) throw new Exception("Save Value Max Size " + maxValueSize + " Byte");

            if (this.socket == null) throw new Exception("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.equals("")) {
                throw new Exception("The blank is not admitted on a key");
            }

            // valueに対する無指定チェック(Valueはnullやブランクの場合は代行文字列に置き換える)
            if (value == null ||  value.equals("")) {
                value = ImdstKeyValueClient.blankStr;
            } else {

                // ValueをBase64でエンコード
                value = new String(BASE64EncoderStream.encode(value.getBytes()));


            }

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("1");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(BASE64EncoderStream.encode(keyStr.getBytes())));
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // Tag連結
            // Tag指定の有無を調べる
            if (tagStrs == null || tagStrs.length < 1) {

                // ブランク規定文字列を連結
                serverRequestBuf.append(ImdstKeyValueClient.blankStr);
            } else {

                // Tag数分連結
                serverRequestBuf.append(new String(BASE64EncoderStream.encode(tagStrs[0].getBytes())));
                for (int i = 1; i < tagStrs.length; i++) {
                    serverRequestBuf.append(tagKeySep);
                    serverRequestBuf.append(new String(BASE64EncoderStream.encode(tagStrs[i].getBytes())));
                }
            }

            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);

            // Value連結
            serverRequestBuf.append(value);
//start2 = System.nanoTime();
            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();
//end2 = System.nanoTime();
            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);
//long end1 = System.nanoTime();
//System.out.println("[" + (end2 - start2) + "]");


            // 処理の妥当性確認
            if (serverRet.length == 3 && serverRet[0].equals("1")) {
                if (serverRet[1].equals("true")) {

                    // 処理成功
                    ret = true;
                } else{

                    // 処理失敗(メッセージ格納)
                    throw new Exception(serverRet[1]);
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity [" + serverRetStr + "]");
            }
        } catch (Exception e) {
            throw e;
        }
        return ret;
    }


    /**
     * マスタサーバへデータを送信する(バイナリデータ).<br>
     * Tagなし.<br>
     *
     * @param keyStr
     * @param values
     * @return boolean
     * @throws Exception
     */
    public boolean setByteValue(String keyStr, byte[] values) throws Exception {
        return this.setByteValue(keyStr, null, values);
    }


    /**
     * マスタサーバへデータを送信する(バイナリデータ).<br>
     * Tag有り.<br>
     * 処理の流れとしては、まずvalueを一定の容量で区切り、その単位で、<br>
     * Key値にプレフィックスを付けた値を作成し、かつ、特定のセパレータで連結して、<br>
     * 渡されたKeyを使用して連結文字を保存する<br>
     * 
     *
     * @param keyStr
     * @param tagStrs
     * @param values
     * @return boolean
     * @throws Exception
     */
    public boolean setByteValue(String keyStr, String[] tagStrs, byte[] values) throws Exception {

        boolean ret = false;

        int bufSize = 0;
        int nowCounter = 0;

        byte[] workData = null;
        int counter = 0;
        int tmpKeyIndex = 0;
        String tmpKey = null;
        StringBuffer saveKeys = new StringBuffer();
        String sep = "";

        String[] tmpKeys = null;

        int keyCount = values.length / this.saveSize;
        int much = values.length % this.saveSize;

        try {

            // Byte Lenghtチェック
            if (keyStr.getBytes().length > maxValueSize) throw new Exception("Save Key Max Size " + maxValueSize + " Byte");
            if (tagStrs != null) {
                for (int i = 0; i < tagStrs.length; i++) {
                    if (tagStrs[i].getBytes().length > maxValueSize) throw new Exception("Tag Max Size " + maxValueSize + " Byte");
                }
            }

            if (much > 0) keyCount = keyCount + 1;

            // バイトデータを分割してノードに転送する
            // 転送サイズは動的に指定可能である
            for (int i = 0; i < keyCount; i++) {

                if (keyCount == (i + 1)) {

                    bufSize = values.length - nowCounter;
                } else {

                    bufSize = this.saveSize;
                }

                // 保存バッファコピー領域作成
                workData = new byte[bufSize];

                for (int workCounter = 0; workCounter < bufSize; workCounter++) {
                    workData[workCounter] = values[nowCounter];
                    nowCounter++;
                }

                // 分割したデータのキーを作成
                tmpKey = keyStr.hashCode() + "_" + i;

                // ノードにバイナリデータ保存
                if(!this.sendByteData(tmpKey, workData)) throw new Exception("Byte Data Save Node Error");

                saveKeys.append(sep);
                saveKeys.append(tmpKey);
                sep = this.byteDataKeysSep;
            }

            ret = this.setValue(keyStr, tagStrs, saveKeys.toString());

        } catch (Exception e) {
            throw e;
        }
        return ret;
    }


    /**
     * マスタサーバへデータを送信する(バイナリデータ).<br>
     *
     * @param keyStr
     * @param values
     * @return boolean
     * @throws Exception
     */
    private boolean sendByteData(String keyStr, byte[] values) throws Exception {
        boolean ret = false; 
        String serverRetStr = null;
        String[] serverRet = null;
        String value = null;
        StringBuffer serverRequestBuf = new StringBuffer();

        String saveStr = null;

        try {

            // valuesがnullであることはない
            // Valueを圧縮し、Base64でエンコード
            value = new String(BASE64EncoderStream.encode(this.execCompress(values)));

            // 処理番号連結
            serverRequestBuf.append("1");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);

            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(BASE64EncoderStream.encode(keyStr.getBytes())));
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);

            // Tagは必ず存在しない
            // ブランク規定文字列を連結
            serverRequestBuf.append(ImdstKeyValueClient.blankStr);

            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);

            // Value連結
            serverRequestBuf.append(value);

            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確認
            if (serverRet.length == 3 && serverRet[0].equals("1")) {
                if (serverRet[1].equals("true")) {

                    // 処理成功
                    ret = true;
                } else{

                    // 処理失敗(メッセージ格納)
                    throw new Exception(serverRet[1]);
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity [" + serverRetStr + "]");
            }
        } catch (Exception e) {
            throw e;
        }
        return ret;
    }


    /**
     * マスタサーバからKeyでデータを取得する.<br>
     * 文字列エンコーディング指定なし.<br>
     * デフォルトエンコーディングにて復元.<br>
     *
     * @param keyStr
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws Exception
     */
    public String[] getValue(String keyStr) throws Exception {
        return this.getValue(keyStr, null);
    }

    /**
     * マスタサーバからKeyでデータを取得する.<br>
     * 文字列エンコーディング指定あり.<br>
     *
     * @param keyStr
     * @param encoding
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws Exception
     */
    public String[] getValue(String keyStr, String encoding) throws Exception {
        String[] ret = new String[2]; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;

        try {
            if (this.socket == null) throw new Exception("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.equals("")) {
                throw new Exception("The blank is not admitted on a key");
            }

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("2");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(BASE64EncoderStream.encode(keyStr.getBytes())));


            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("2")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(ImdstKeyValueClient.blankStr)) {
                        ret[1] = "";
                    } else {

                        // Value文字列をBase64でデコード
                        if (encoding == null) {
                            ret[1] = new String(BASE64DecoderStream.decode(serverRet[2].getBytes()));
                        } else {
                            ret[1] = new String(BASE64DecoderStream.decode(serverRet[2].getBytes()), encoding);
                        }
                    }
                } else if(serverRet[1].equals("false")) {

                    // データなし
                    ret[0] = serverRet[1];
                    ret[1] = null;
                } else if(serverRet[1].equals("error")) {

                    // エラー発生
                    ret[0] = serverRet[1];
                    ret[1] = serverRet[2];
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity");
            }
        } catch (Exception e) {
            throw e;
        }
        return ret;
    }

    /**
     * マスタサーバからKeyでデータを削除する.<br><br>
     *
     * @param keyStr
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws Exception
     */
    public String[] removeValue(String keyStr) throws Exception {
		return this.removeValue(keyStr, null);
	}
    /**
     * マスタサーバからKeyでデータを削除する.<br>
     * 取得値のエンコーディング指定あり.<br>
     * @param keyStr
     * @return String[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws Exception
     */
    public String[] removeValue(String keyStr, String encoding) throws Exception {
        String[] ret = new String[2]; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;

        try {
            if (this.socket == null) throw new Exception("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.equals("")) {
                throw new Exception("The blank is not admitted on a key");
            }

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("5");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(BASE64EncoderStream.encode(keyStr.getBytes())));


            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("5")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(ImdstKeyValueClient.blankStr)) {
                        ret[1] = "";
                    } else {

                        // Value文字列をBase64でデコード
                        if (encoding == null) {
                            ret[1] = new String(BASE64DecoderStream.decode(serverRet[2].getBytes()));
                        } else {
                            ret[1] = new String(BASE64DecoderStream.decode(serverRet[2].getBytes()), encoding);
                        }
                    }
                } else if(serverRet[1].equals("false")) {

                    // データなし
                    ret[0] = serverRet[1];
                    ret[1] = null;
                } else if(serverRet[1].equals("error")) {

                    // エラー発生
                    ret[0] = serverRet[1];
                    ret[1] = serverRet[2];
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity");
            }
        } catch (Exception e) {
            throw e;
        }
        return ret;
    }

    /**
     * マスタサーバからKeyでデータを取得する(バイナリ).<br>
     *
     * @param keyStr
     * @return Object[] 要素1(String)(データ有無):"true" or "false",要素2(byte[])(データ):{バイト配列}
     * @throws Exception
     */
    public Object[] getByteValue(String keyStr) throws Exception {
        Object[] ret = new Object[2];
        Object[] byteTmpRet = null;

        String[] workKeyRet = null;
        String workKeyStr = null;
        String[] workKeys = null;

        byte[] workValue = null;
        byte[] tmpValue = new byte[0];
        byte[] retValue = new byte[0];

        boolean execFlg = true;
        try {
            workKeyRet = this.getValue(keyStr);
            
            if (workKeyRet[0].equals("true")) {

                workKeyStr = (String)workKeyRet[1];

                workKeyRet = workKeyStr.split(byteDataKeysSep);

                for (int idx = 0; idx < workKeyRet.length; idx++) {

                    byteTmpRet = this.getByteData(workKeyRet[idx]);

                    if (byteTmpRet[0].equals("true")) {

                        workValue = (byte[])byteTmpRet[1];

                        if (execFlg) {

                            tmpValue = new byte[retValue.length + workValue.length];

                            for (int i = 0; i < retValue.length; i++) {
                                tmpValue[i] = retValue[i];
                            }

                            for (int i = 0; i < workValue.length; i++) {
                                tmpValue[retValue.length + i] = workValue[i];
                            }
                            execFlg = false;
                        } else {

                            retValue = new byte[tmpValue.length + workValue.length];

                            for (int i = 0; i < tmpValue.length; i++) {
                                retValue[i] = tmpValue[i];
                            }

                            for (int i = 0; i < workValue.length; i++) {
                                retValue[tmpValue.length + i] = workValue[i];
                            }
                            execFlg = true;
                        }
                    } else {

                        // エラー発生
                        ret[0] = byteTmpRet[0];
                        ret[1] = byteTmpRet[1];
                        break;
                    }
                }

                ret[0] = "true";
                if (retValue.length >= tmpValue.length) {
                    ret[1] = retValue;
                } else{
                    ret[1] = tmpValue;
                }
            } else {
                ret[0] = workKeyRet[0];
                ret[1] = workKeyRet[1];
            }
        } catch(Exception e) {
            throw e;
        }
        return ret;
    }


    /**
     * マスタサーバからKeyでデータを取得する(バイナリ).<br>
     *
     * @param keyStr
     * @return Object[] 要素1(String)(データ有無):"true" or "false",要素2(byte[])(データ):{バイト配列}
     * @throws Exception
     */
    private Object[] getByteData(String keyStr) throws Exception {
        Object[] ret = new Object[2];
        byte[] byteRet = null;
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;

        try {
            if (this.socket == null) throw new Exception("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (keyStr == null ||  keyStr.equals("")) {
                throw new Exception("The blank is not admitted on a key");
            }

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("2");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // Key連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(BASE64EncoderStream.encode(keyStr.getBytes())));


            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確認
            if (serverRet[0].equals("2")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(ImdstKeyValueClient.blankStr)) {
                        byteRet = new byte[0];
                        ret[1] = byteRet;
                    } else {

                        // Value文字列をBase64でデコードし、圧縮解除
                        ret[1] = this.execDecompres(BASE64DecoderStream.decode(serverRet[2].getBytes()));
                        //ret[1] = BASE64DecoderStream.decode(this.execDecompres(BASE64DecoderStream.decode(serverRet[2].getBytes())));
                    }
                } else if(serverRet[1].equals("false")) {

                    // データなし
                    ret[0] = serverRet[1];
                    ret[1] = null;
                } else if(serverRet[1].equals("error")) {

                    // エラー発生
                    ret[0] = serverRet[1];
                    ret[1] = serverRet[2];
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity");
            }
        } catch (Exception e) {
            throw e;
        }
        return ret;
    }


    /**
     * マスタサーバからTagでKey値群を取得する.<br>
     *
     * @param tagStr
     * @return Object[] 要素1(データ有無):"true" or "false",要素2(データ):"データ文字列"
     * @throws Exception
     */
    public Object[] getTagKeys(String tagStr) throws Exception {
        Object[] ret = new Object[2]; 
        String serverRetStr = null;
        String[] serverRet = null;

        StringBuffer serverRequestBuf = null;

        try {
            if (this.socket == null) throw new Exception("No ServerConnect!!");

            // エラーチェック
            // Keyに対する無指定チェック
            if (tagStr == null ||  tagStr.equals("")) {
                throw new Exception("The blank is not admitted on a tag");
            }

            // 文字列バッファ初期化
            serverRequestBuf = new StringBuffer();


            // 処理番号連結
            serverRequestBuf.append("3");
            // セパレータ連結
            serverRequestBuf.append(ImdstKeyValueClient.sepStr);


            // tag連結(Keyはデータ送信時には必ず文字列が必要)
            serverRequestBuf.append(new String(BASE64EncoderStream.encode(tagStr.getBytes())));


            // サーバ送信
            pw.println(serverRequestBuf.toString());
            pw.flush();

            // サーバから結果受け取り
            serverRetStr = br.readLine();

            serverRet = serverRetStr.split(ImdstKeyValueClient.sepStr);

            // 処理の妥当性確
            if (serverRet[0].equals("4")) {
                if (serverRet[1].equals("true")) {

                    // データ有り
                    ret[0] = serverRet[1];

                    // Valueがブランク文字か調べる
                    if (serverRet[2].equals(ImdstKeyValueClient.blankStr)) {
                        String[] tags = {""};
                        ret[1] = tags;
                    } else {
                        String[] tags = null;
                        String[] cnvTags = null;

                        tags = serverRet[2].split(tagKeySep);
                        String[] decTags = new String[tags.length];
                        for (int i = 0; i < tags.length; i++) {
                            decTags[i] = new String(BASE64DecoderStream.decode(tags[i].getBytes()));
                        }
                        ret[1] = decTags;
                    }
                } else if(serverRet[1].equals("false")) {

                    // データなし
                    ret[0] = serverRet[1];
                    ret[1] = null;
                } else if(serverRet[1].equals("error")) {

                    // エラー発生
                    ret[0] = serverRet[1];
                    ret[1] = serverRet[2];
                }
            } else {

                // 妥当性違反
                throw new Exception("Execute Violation of validity [" + serverRet[0] + "]");
            }
        } catch (Exception e) {
            throw e;
        }
        return ret;
    }


    // 圧縮メソッド
    private byte[] execCompress(byte[] bytes) throws Exception {
        try {
            Deflater compresser = new Deflater(); 

            compresser.setInput(bytes); 
            compresser.finish();

            // 圧縮単位
            int bufSize = 1024 * 1024 * 5;

            byte[] output = new byte[bufSize]; 
            byte[] workByte1 = new byte[0];
            byte[] workByte2 = new byte[0];

            int flg = 0;
            int use = 1;

            while(true) {
                int compressedDataLength = compresser.deflate(output);

                if (compressedDataLength == bufSize) {
                    if (flg == 0) {
                        workByte1 = new byte[workByte2.length + bufSize];

                        for (int i = 0; i < workByte2.length; i++) {
                            workByte1[i] = workByte2[i];
                        }

                        for (int i = 0; i < bufSize; i++) {
                            workByte1[workByte2.length + i] = output[i];
                        }

                        flg = 1;
                    } else {
                        workByte2 = new byte[workByte1.length + bufSize];

                        for (int i = 0; i < workByte1.length; i++) {
                            workByte2[i] = workByte1[i];
                        }

                        for (int i = 0; i < bufSize; i++) {
                            workByte2[workByte1.length + i] = output[i];
                        }
                        flg = 0;
                    }
                } else {
                    if (workByte1.length == workByte2.length) {

                        workByte1 = new byte[compressedDataLength];
                        for (int i = 0; i < compressedDataLength; i++) {
                            workByte1[i] = output[i];
                        }
                    } else if (workByte1.length > workByte2.length) {

                        workByte2 = new byte[workByte1.length + compressedDataLength];
                        for (int i = 0; i < workByte1.length; i++) {
                            workByte2[i] = workByte1[i];
                        }

                        for (int i = 0; i < compressedDataLength; i++) {
                            workByte2[workByte1.length + i] = output[i];
                        }
                    } else if (workByte1.length < workByte2.length) {

                        workByte1 = new byte[workByte2.length + compressedDataLength];

                        for (int i = 0; i < workByte2.length; i++) {
                            workByte1[i] = workByte2[i];
                        }

                        for (int i = 0; i < compressedDataLength; i++) {
                            workByte1[workByte2.length + i] = output[i];
                        }
                    }
                    break;
                }
            }



            if (workByte1.length > workByte2.length) {

                return workByte1;
            } else if (workByte1.length < workByte2.length) {

                return workByte2;
            }
        } catch (Exception e) {
            throw e;
        }
        return null;
    }


    // 圧縮解除メソッド
    private byte[] execDecompres(byte[] bytes)  throws Exception {

        try {
            // 圧縮解除単位
            int bufSize = 1024 * 1024 * 5;

            Inflater decompresser = new Inflater(); 
            decompresser.setInput(bytes, 0, bytes.length); 

            byte[] result = new byte[bufSize]; 
            byte[] workByte1 = new byte[0];
            byte[] workByte2 = new byte[0];
            int flg = 0;

            while(true) {
                int resultLength = decompresser.inflate(result); 

                if (resultLength == bufSize) {
                    if (flg == 0) {
                        workByte1 = new byte[workByte2.length + bufSize];

                        for (int i = 0; i < workByte2.length; i++) {
                            workByte1[i] = workByte2[i];
                        }

                        for (int i = 0; i < bufSize; i++) {
                            workByte1[workByte2.length + i] = result[i];
                        }

                        flg = 1;
                    } else {
                        workByte2 = new byte[workByte1.length + bufSize];

                        for (int i = 0; i < workByte1.length; i++) {
                            workByte2[i] = workByte1[i];
                        }

                        for (int i = 0; i < bufSize; i++) {
                            workByte2[workByte1.length + i] = result[i];
                        }
                        flg = 0;
                    }
                } else {

                    if (workByte1.length == workByte2.length) {

                        workByte1 = new byte[resultLength];
                        for (int i = 0; i < resultLength; i++) {
                            workByte1[i] = result[i];
                        }
                    } else if (workByte1.length > workByte2.length) {

                        workByte2 = new byte[workByte1.length + resultLength];
                        for (int i = 0; i < workByte1.length; i++) {
                            workByte2[i] = workByte1[i];
                        }

                        for (int i = 0; i < resultLength; i++) {
                            workByte2[workByte1.length + i] = result[i];
                        }
                    } else if (workByte1.length < workByte2.length) {

                        workByte1 = new byte[workByte2.length + resultLength];

                        for (int i = 0; i < workByte2.length; i++) {
                            workByte1[i] = workByte2[i];
                        }

                        for (int i = 0; i < resultLength; i++) {
                            workByte1[workByte2.length + i] = result[i];
                        }
                    }
                    break;
                }
            }

            decompresser.end();

            if (workByte1.length > workByte2.length) {

                return workByte1;
            } else if (workByte1.length < workByte2.length) {

                return workByte2;
            }
        } catch (Exception e) {
            throw e;
        }
        return null;
    }
}
