package org.imdst.helper;

import java.io.*;
import java.util.*;
import java.net.*;
import javax.script.*;

import org.batch.lang.BatchException;
import org.batch.job.AbstractHelper;
import org.batch.job.IJob;
import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.imdst.util.KeyMapManager;
import org.imdst.util.ImdstDefine;
import org.imdst.util.StatusUtil;


import com.sun.mail.util.BASE64DecoderStream;
import com.sun.mail.util.BASE64EncoderStream;
/**
 * Key情報を格納するHelper.<br>
 * 本helperはKeyManagerJobから呼び出されることを想定している.<br>
 * 1スレッド1コネクションに対して対応する.<br>
 * Jobからの引数<br>
 * 接続済みソケットインスタンス、keyMapManagerインスタンス<br>
 * クライアントからの引数は1行文字列で送られてくる。<br>
 * 処理番号<セパレート文字>メソッド固有パラメータ_1<セパレート文字>メソッド固有パラメータ_2<br>
 * クライアントへの返却値は1行文字列で返される。<br>
 *<br>
 * <br>
 * 実装メソッド.<br>
 *<br>
 * ■Keyとデータノード設定メソッド.<br>
 * ●パラメータ<br>
 * 1.処理番号 = 1<br>
 * 2.キー値<br>
 * 3.データノード<br>
 * ○戻り値<br>
 * 1.処理番号<セパレート文字>格納有無("true or false")<セパレート文字>NGの場合はエラー文字列<br>
 *<br>
 *<br>
 * ■Keyでデータノードを取得するメソッド.<br>
 * ●パラメータ<br>
 * 1.処理番号 = 2<br>
 * 2.キー値<br>
 * ○戻り値<br>
 * 1.処理番号<セパレート文字>取得有無("true or false")<セパレート文字>取得出来た場合データノード名<br>
 *<br>
 *<br>
 * ■Tagにキーを追加するメソッド.<br>
 * ●パラメータ<br>
 * 1.処理番号 = 3<br>
 * 2.キー値<br>
 * 3.Tag<br>
 * ○戻り値<br>
 * 1.処理番号<セパレート文字>格納有無("true or false")<セパレート文字>NGの場合はエラー文字列<br>
 *<br>
 * ■TagでKeyを取得するメソッド.<br>
 * ●パラメータ<br>
 * 1.処理番号 = 4<br>
 * 2.Tag<br>
 * ○戻り値<br>
 * 1.処理番号<セパレート文字>取得有無("true or false")<セパレート文字>取得出来た場合キー一覧(内部セパレータは",")<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyManagerHelper extends AbstractHelper {

    // KeyMapManagerインスタンス
    private KeyMapManager keyMapManager = null;

    private Socket soc = null;

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(KeyManagerHelper.class);

    // 初期化メソッド定義
    public void initHelper(String initValue) {
    }

    // Jobメイン処理定義
    public String executeHelper(String optionParam) throws BatchException {
        logger.debug("KeyManagerHelper - executeHelper - start");

        String ret = null;
        boolean closeFlg = false;

        try{
            String[] retParams = null;
            StringBuffer retParamBuf = null;

            Object[] parameters = super.getParameters();

            String clientParametersStr = null;
            String[] clientParameterList = null;

            Integer requestHashCode = null;
            String requestDataNode = null;

            Integer requestTag = null;
            String requestKey = null;

            String transactionCode = null;

            // Jobからの引数
            this.keyMapManager = (KeyMapManager)parameters[0];
            this.soc = (Socket)parameters[1];

            // クライアントへのアウトプット
            OutputStreamWriter osw = new OutputStreamWriter(this.soc.getOutputStream() , 
                                                            ImdstDefine.keyHelperClientParamEncoding);
            PrintWriter pw = new PrintWriter(new BufferedWriter(osw));

            // クライアントからのインプット
            InputStreamReader isr = new InputStreamReader(this.soc.getInputStream(),
                                                          ImdstDefine.keyHelperClientParamEncoding);
            BufferedReader br = new BufferedReader(isr);

            while(!closeFlg) {
                try {

                    clientParametersStr = br.readLine();

                    // クライアントからの要求が接続切要求ではないか確認
                    if (clientParametersStr == null || 
                            clientParametersStr.equals("") || 
                                clientParametersStr.equals(ImdstDefine.imdstConnectExitRequest)) {
                        // 切断要求
                        logger.debug("Client Connect Exit Request");
                        closeFlg = true;
                        break;
                    }

                    clientParameterList = clientParametersStr.split(ImdstDefine.keyHelperClientParamSep);

                    // 処理番号を取り出し
                    retParamBuf = new StringBuffer();

                    if(clientParameterList[0].equals("1")) {

                        // Key値とDataNode名を格納する
                        requestHashCode = new Integer(clientParameterList[1]);
                        transactionCode = clientParameterList[2];
                        requestDataNode = clientParameterList[3];

                        // 値の中にセパレータ文字列が入っている場合もデータとしてあつかう
                        if (clientParameterList.length > 4) {
                            requestDataNode = requestDataNode + 
                                ImdstDefine.keyHelperClientParamSep + 
                                    clientParameterList[4];
                        }

                        // メソッド呼び出し
                        retParams = this.setDatanode(requestHashCode, requestDataNode, transactionCode);
                        retParamBuf.append(retParams[0]);
                        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        retParamBuf.append(retParams[1]);
                        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        retParamBuf.append(retParams[2]);

                    } else if(clientParameterList[0].equals("2")) {

                        // Key値でDataNode名を返す
                        requestHashCode = new Integer(clientParameterList[1]);
                        // メソッド呼び出し
                        retParams = this.getDatanode(requestHashCode);
                        retParamBuf.append(retParams[0]);
                        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        retParamBuf.append(retParams[1]);
                        if (retParams.length > 2) {
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(retParams[2]);
                        }
                    } else if(clientParameterList[0].equals("3")) {

                        // Tag値とキー値を格納する
                        requestTag = new Integer(clientParameterList[1]);
                        transactionCode = clientParameterList[2];         // TransactionCode
                        requestKey = clientParameterList[3];

                        // メソッド呼び出し
                        retParams = this.setTagdata(requestTag, requestKey, transactionCode);
                        retParamBuf.append(retParams[0]);
                        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        retParamBuf.append(retParams[1]);
                    } else if(clientParameterList[0].equals("4")) {

                        // Tag値でKey値を返す
                        requestHashCode = new Integer(clientParameterList[1]);
                        // メソッド呼び出し
                        retParams = this.getTagdata(requestHashCode);
                        retParamBuf.append(retParams[0]);
                        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        retParamBuf.append(retParams[1]);
                        if (retParams.length > 2) {
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(retParams[2]);
                        }
                    } else if(clientParameterList[0].equals("5")) {

                        // Key値を指定する事でデータを削除する
                        requestHashCode = new Integer(clientParameterList[1]);
                        transactionCode = clientParameterList[2];

                        // メソッド呼び出し
                        retParams = this.removeDatanode(requestHashCode, transactionCode);
                        retParamBuf.append(retParams[0]);
                        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        retParamBuf.append(retParams[1]);
                        if (retParams.length > 2) {
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(retParams[2]);
                        }
                    } else if(clientParameterList[0].equals("8")) {

                        // Key値でDataNode名を返す(Script実行バージョン)
                        requestHashCode = new Integer(clientParameterList[1]);
                        // メソッド呼び出し
                        retParams = this.getDatanodeScriptExec(requestHashCode,clientParameterList[2]);
                        retParamBuf.append(retParams[0]);
                        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        retParamBuf.append(retParams[1]);
                        if (retParams.length > 2) {
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(retParams[2]);
                        }
                    
                    } else if(clientParameterList[0].equals("10")) {

                        // ServerConnect Test Ping
                        retParamBuf.append("10");
                        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        retParamBuf.append("true");
                        // エラーの場合は以下でエラーメッセメッセージも連結
                        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        retParamBuf.append(StatusUtil.getNowMemoryStatus());
                    } else if(clientParameterList[0].equals("11")) {

                        // 最終データ更新時間を返す
                        retParamBuf.append("11");
                        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        retParamBuf.append("true");
                        // エラーの場合は以下でエラーメッセメッセージも連結
                        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        retParamBuf.append(keyMapManager.getLastDataChangeTime());
                    } else if(clientParameterList[0].equals("20")) {

                        // KeyMapManager Direct Connection
                        // KeyMapObjectを読み込んで渡す
                        this.keyMapManager.outputKeyMapObj2Stream(pw);
                        pw.flush();
                        retParamBuf = null;
                    } else if(clientParameterList[0].equals("21")) {

                        // KeyMapManager Direct Connection
                        // KeyMapObjectを読み込んで書き出す
                        this.keyMapManager.inputKeyMapObj2Stream(br);
                        retParamBuf = null;
                    }

                    if (retParamBuf != null) {
                        pw.println(retParamBuf.toString());
                        pw.flush();
                    }
                } catch (SocketException se) {
                    closeFlg = true;
                }
            }
            pw.close();
            br.close();
            ret = super.SUCCESS;
        } catch(Exception e) {

            logger.error("KeyManagerHelper - executeHelper - Error", e);
            ret = super.ERROR;
            throw new BatchException(e);
        } finally {

            try {
                if (this.soc != null) {
                    this.soc.close();
                    this.soc = null;
                }
            } catch(Exception e2) {
                logger.error("KeyManagerHelper - executeHelper - Error2", e2);
                ret = super.ERROR;
                throw new BatchException(e2);
            }
        }

        logger.debug("KeyManagerHelper - executeHelper - end");
        return ret;
    }

    /**
     * 初期化メソッド定義
     */
    public void endHelper() {
        logger.debug("KeyManagerHelper - endHelper - start");
        try {
            if (this.soc != null) {
                this.soc.close();
                this.soc = null;
            }
        } catch(Exception e2) {
            logger.error("KeyManagerHelper - executeHelper - Error2", e2);
        }
        logger.debug("KeyManagerHelper - endHelper - end");
    }

    // KeyとDataNode値を格納する
    private String[] setDatanode(Integer key, String dataNodeStr, String transactionCode) {
        //logger.debug("KeyManagerHelper - setDatanode - start");
        String[] retStrs = new String[3];
        try {
            if(!this.keyMapManager.checkError()) {
                this.keyMapManager.setKeyPair(key, dataNodeStr, transactionCode);
                retStrs[0] = "1";
                retStrs[1] = "true";
                retStrs[2] = "OK";
            } else {
                retStrs[0] = "1";
                retStrs[1] = "false";
                retStrs[2] = "NG:KeyMapManager - setDatanode - CheckError - NG";
            }
        } catch (BatchException be) {
            logger.debug("KeyManagerHelper - setDatanode - Error", be);
            retStrs[0] = "1";
            retStrs[1] = "false";
            retStrs[2] = "NG:KeyManagerHelper - setDatanode - Exception - " + be.toString();
        }
        //logger.debug("KeyManagerHelper - setDatanode - end");
        return retStrs;
    }


    // KeyでDataNode値を取得する
    private String[] getDatanode(Integer key) {
        //logger.debug("KeyManagerHelper - getDatanode - start");
        String[] retStrs = null;
        try {
            if(!this.keyMapManager.checkError()) {
                if (this.keyMapManager.containsKeyPair(key)) {

                    retStrs = new String[3];
                    retStrs[0] = "2";
                    retStrs[1] = "true";
                    retStrs[2] = this.keyMapManager.getKeyPair(key);
                } else {
                    retStrs = new String[2];
                    retStrs[0] = "2";
                    retStrs[1] = "false";
                }
            } else {
                    retStrs = new String[2];
                    retStrs[0] = "2";
                    retStrs[1] = "false";
            }
        } catch (Exception e) {
            logger.error("KeyManagerHelper - getDatanode - Error", e);
            retStrs = new String[2];
            retStrs[0] = "2";
            retStrs[1] = "false";
        }
        //logger.debug("KeyManagerHelper - getDatanode - end");
        return retStrs;
    }


    // KeyでDataNode値を取得する
    // 実行後データが取得出来た場合はデータに対してScriptを実行する
    // 現在スクリプトはJavaScriptとなる
    // スクリプトには必ずdataValueという変数が定義されているものとして、該当変数に取得したValue値が格納
    // されてスクリプトが実行される。
    // もどり値はScript内にexecRetという0 or 1で表す値とretValueという返却値が
    // 定義されているものとする
    // スクリプト実行後、execRetの値で状態を判断し、retValueが返却されるかが決定される。
    // execRet=1値を返す、execRet=0値を返さない
    // retValue=Value返却される値となる
    // 返却値の配列の2番目の値がtrueならスクリプト実行後結果あり、
    // falseならスクリプト実行後結果なし、errorならスクリプト実行エラー
    private String[] getDatanodeScriptExec(Integer key, String scriptStr) {
        //logger.debug("KeyManagerHelper - getDatanode - start");
        String[] retStrs = null;
        try {
            if(!this.keyMapManager.checkError()) {
                if (this.keyMapManager.containsKeyPair(key)) {

                    retStrs = new String[3];
                    retStrs[0] = "8";
                    retStrs[1] = "true";
                    String tmpValue = this.keyMapManager.getKeyPair(key);
                    
                    if (scriptStr != null && !scriptStr.trim().equals("") &&
                        !(new String(BASE64DecoderStream.decode(scriptStr.getBytes())).equals(ImdstDefine.imdstBlankStrData))) {

                        // TODO:エンジンの初期化に時間がかかるので他の影響を考えここで初期化
                        ScriptEngineManager manager = new ScriptEngineManager();
                        ScriptEngine engine = manager.getEngineByName("JavaScript");

                        // 引数設定
                        if (tmpValue.equals(ImdstDefine.imdstBlankStrData)) {
                            engine.put("dataValue", "");
                        } else {
                            engine.put("dataValue", new String(BASE64DecoderStream.decode(tmpValue.getBytes()), ImdstDefine.keyWorkFileEncoding));
                        }

                        // 実行 
                        try {
                            engine.eval(new String(BASE64DecoderStream.decode(scriptStr.getBytes()), ImdstDefine.keyWorkFileEncoding));

                            String execRet = (String)engine.get("execRet");
                            if (execRet != null && execRet.equals("1")) {
                                // データを返す
                                String retValue = (String)engine.get("retValue");
                                if (retValue != null && !retValue.equals("")) {
                                    retStrs[2] = new String(BASE64EncoderStream.encode(retValue.getBytes()),ImdstDefine.keyWorkFileEncoding);
                                } else { 
                                    retStrs[2] = ImdstDefine.imdstBlankStrData;
                                }
                            } else {
                                // データを返さない
                                retStrs[1] = "false";
                            }
                        } catch (Exception e) {
                            retStrs[1] = "error";
                            retStrs[2] = e.getMessage();
                        }
                    } else {
                        retStrs[2] = tmpValue;
                    }
                } else {
                    retStrs = new String[2];
                    retStrs[0] = "8";
                    retStrs[1] = "false";
                }
            } else {
                    retStrs = new String[2];
                    retStrs[0] = "8";
                    retStrs[1] = "false";
            }
        } catch (Exception e) {
            logger.error("KeyManagerHelper - getDatanode - Error", e);
            retStrs = new String[2];
            retStrs[0] = "8";
            retStrs[1] = "false";
        }
        //logger.debug("KeyManagerHelper - getDatanode - end");
        return retStrs;
    }


    // KeyとDataNode値を格納する
    private String[] setTagdata(Integer tag, String key, String transactionCode) {
        //logger.debug("KeyManagerHelper - setTagdata - start");
        String[] retStrs = new String[3];
        try {
            if(!this.keyMapManager.checkError()) {

                this.keyMapManager.setTagPair(tag, key, transactionCode);

                retStrs[0] = "3";
                retStrs[1] = "true";
                retStrs[2] = "OK";
            } else {
                retStrs[0] = "3";
                retStrs[1] = "false";
                retStrs[2] = "NG:KeyMapManager - setTagdata - CheckError - NG";
            }
        } catch (BatchException be) {
            logger.debug("KeyManagerHelper - setTagdata - Error", be);
            retStrs[0] = "3";
            retStrs[1] = "false";
            retStrs[2] = "NG:KeyManagerHelper - setTagdata - Exception - " + be.toString();
        }
        //logger.debug("KeyManagerHelper - setTagdata - end");
        return retStrs;
    }


    // KeyでDataNode値を削除する
    private String[] removeDatanode(Integer key, String transactionCode) {
        //logger.debug("KeyManagerHelper - removeDatanode - start");
        String[] retStrs = null;
        try {
            if(!this.keyMapManager.checkError()) {
                if (this.keyMapManager.containsKeyPair(key)) {

                    retStrs = new String[3];
                    retStrs[0] = "5";
                    retStrs[1] = "true";
                    retStrs[2] = this.keyMapManager.removeKeyPair(key, transactionCode);
                } else {
                    retStrs = new String[2];
                    retStrs[0] = "5";
                    retStrs[1] = "false";
                }
            } else {
                    retStrs = new String[2];
                    retStrs[0] = "5";
                    retStrs[1] = "false";
            }
        } catch (Exception e) {
            logger.error("KeyManagerHelper - removeDatanode - Error", e);
            retStrs = new String[2];
            retStrs[0] = "5";
            retStrs[1] = "false";
        }
        //logger.debug("KeyManagerHelper - removeDatanode - end");
        return retStrs;
    }


    // TagでKey値を取得する
    private String[] getTagdata(Integer tag) {
        //logger.debug("KeyManagerHelper - getTagdata - start");
        String[] retStrs = null;
        try {
            if(!this.keyMapManager.checkError()) {
                if (this.keyMapManager.containsTagPair(tag)) {

                    retStrs = new String[3];
                    retStrs[0] = "4";
                    retStrs[1] = "true";
                    retStrs[2] = this.keyMapManager.getTagPair(tag);
                } else {
                    retStrs = new String[2];
                    retStrs[0] = "4";
                    retStrs[1] = "false";
                }
            } else {
                    retStrs = new String[2];
                    retStrs[0] = "4";
                    retStrs[1] = "false";
            }
        } catch (Exception e) {
            logger.error("KeyManagerHelper - getTagdata - Error", e);
            retStrs = new String[2];
            retStrs[0] = "4";
            retStrs[1] = "false";
        }
        //logger.debug("KeyManagerHelper - getTagdata - end");
        return retStrs;
    }

}