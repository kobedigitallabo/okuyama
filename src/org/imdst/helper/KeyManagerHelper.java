package org.imdst.helper;

import java.io.*;
import java.util.*;
import java.net.*;

import org.batch.lang.BatchException;
import org.batch.job.AbstractHelper;
import org.batch.job.IJob;
import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.imdst.util.KeyMapManager;
import org.imdst.util.ImdstDefine;

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
            Integer execPattern = null;
            Integer requestHashCode = null;
            String requestDataNode = null;
            Integer requestTag = null;
            String requestKey = null;

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
                    if (clientParametersStr.equals(ImdstDefine.imdstConnectExitRequest)) {
                        // 切断要求
                        logger.debug("Client Connect Exit Request");
                        closeFlg = true;
                        break;
                    }

                    clientParameterList = clientParametersStr.split(ImdstDefine.keyHelperClientParamSep);

                    // 処理番号を取り出し
                    execPattern = new Integer(clientParameterList[0]);
                    retParamBuf = new StringBuffer();
                    if(execPattern.equals(new Integer(1))) {

                        // Key値とDataNode名を格納する
                        requestHashCode = new Integer(clientParameterList[1]);
                        requestDataNode = clientParameterList[2];
                        // メソッド呼び出し
                        retParams = this.setDatanode(requestHashCode, requestDataNode);
                        retParamBuf.append(retParams[0]);
                        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        retParamBuf.append(retParams[1]);
                        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        retParamBuf.append(retParams[2]);
                    } else if(execPattern.equals(new Integer(2))) {

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
                    } else if(execPattern.equals(new Integer(3))) {

                        // Tag値とキー値を格納する
                        requestTag = new Integer(clientParameterList[1]);
                        requestKey = clientParameterList[2];
                        // メソッド呼び出し
                        retParams = this.setTagdata(requestTag, requestKey);
                        retParamBuf.append(retParams[0]);
                        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        retParamBuf.append(retParams[1]);
                    } else if(execPattern.equals(new Integer(4))) {

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
                    }


                    pw.println(retParamBuf.toString());
                    pw.flush();
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
    private String[] setDatanode(Integer key, String dataNodeStr) {
        //logger.debug("KeyManagerHelper - setDatanode - start");
        String[] retStrs = new String[3];
        try {
            if(!this.keyMapManager.checkError()) {
                this.keyMapManager.setKeyPair(key, dataNodeStr);
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


    // KeyとDataNode値を格納する
    private String[] setTagdata(Integer tag, String key) {
        //logger.debug("KeyManagerHelper - setTagdata - start");
        String[] retStrs = new String[3];
        try {
            if(!this.keyMapManager.checkError()) {
                //long start = new Date().getTime();
                this.keyMapManager.setTagPair(tag, key);
                //long end = new Date().getTime();
                //System.out.println((end - start));

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