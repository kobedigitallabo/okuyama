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
import org.imdst.util.DataDispatcher;
import org.imdst.util.StatusUtil;
/**
 * MasterNodeのメイン実行部分<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class MasterManagerHelper extends AbstractMasterManagerHelper {

    private HashMap keyNodeConnectMap = new HashMap();
    private HashMap dataNodeConnectMap = new HashMap();

    // 過去ルール
    private int[] oldRule = null;

    // 自身のモード(1=Key-Value, 2=DataSystem)
    private int mode = 1;

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(MasterManagerHelper.class);

    // 初期化メソッド定義
    public void initHelper(String initValue) {
        this.mode = Integer.parseInt(initValue);
    }

    // Jobメイン処理定義
    public String executeHelper(String optionParam) throws BatchException {
        logger.debug("MasterManagerHelper - executeHelper - start");

        String ret = null;

        Socket soc = null;
        boolean closeFlg = false;
        int keyLength = 128;
        int tagLength = 256;
        int oneDataLength = 1024;

        String[] retParams = null;
        StringBuffer retParamBuf = null;

        int execSt = 1;

        try{

            Object[] parameters = super.getParameters();

            String clientParametersStr = null;
            String[] clientParameterList = null;
            Integer execPattern = null;

            // Jobからの引数
            soc = (Socket)parameters[0];

            this.oldRule = (int[])parameters[1];

            // クライアントへのアウトプット(結果セット用の文字列用と、バイトデータ転送用)
            OutputStreamWriter osw = new OutputStreamWriter(soc.getOutputStream(),
                                                            ImdstDefine.keyHelperClientParamEncoding);
            BufferedOutputStream bos = new BufferedOutputStream(soc.getOutputStream());
            PrintWriter pw = new PrintWriter(new BufferedWriter(osw));


            // クライアントからのインプット
            InputStreamReader isr = new InputStreamReader(soc.getInputStream(),
                                                            ImdstDefine.keyHelperClientParamEncoding);
            BufferedReader br = new BufferedReader(isr);

            // 接続終了までループ
            while(!closeFlg) {
                try {

                    // モード別処理
                    if (this.mode == 1) {
                        // Key-Valueモードで使用する
                        String keyParam = null;
                        String tagParam = null;
                        String dataParam = null;

                        // クライアントからの要求を取得
                        clientParametersStr = br.readLine();

                        // クライアントからの要求が接続切要求ではないか確認
                        if (clientParametersStr.equals(ImdstDefine.imdstConnectExitRequest)) {
                            // 切断要求
                            logger.debug("Client Connect Exit Request");
                            closeFlg = true;
                            break;
                        }

                        while(true) {
                            if (StatusUtil.getStatus() != 3) {
                                execSt = 1;
                                super.execSt(execSt);
                                clientParameterList = clientParametersStr.split(ImdstDefine.keyHelperClientParamSep);

                                // 処理番号を取り出し
                                execPattern = new Integer(clientParameterList[0]);
                                retParamBuf = new StringBuffer();

                                if(execPattern.equals(new Integer(1))) {

                                    // Key値とValueを格納する
                                    retParams = this.setKeyValue(clientParameterList[1], clientParameterList[2], clientParameterList[3]);
                                    retParamBuf.append(retParams[0]);
                                    retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                                    retParamBuf.append(retParams[1]);
                                    retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                                    retParamBuf.append(retParams[2]);
                                } else if(execPattern.equals(new Integer(2))) {

                                    // Key値でValueを取得する
                                    retParams = this.getKeyValue(clientParameterList[1]);
                                    retParamBuf.append(retParams[0]);
                                    retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                                    retParamBuf.append(retParams[1]);
                                    retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                                    retParamBuf.append(retParams[2]);

                                } else if(execPattern.equals(new Integer(3))) {

                                    // Tag値でキー値群を取得する
                                    retParams = this.getTagKeys(clientParameterList[1]);
                                    retParamBuf.append(retParams[0]);
                                    retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                                    retParamBuf.append(retParams[1]);
                                    retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                                    retParamBuf.append(retParams[2]);
                                } else if(execPattern.equals(new Integer(4))) {

                                    // Tag値で紐付くキーとValueのセット配列を返す

                                }
                                break;
                            }
                            Thread.sleep(50); 
                        }
                    }

                    // クライアントに結果送信
                    pw.println(retParamBuf.toString());
                    pw.flush();
                    if (execSt == 1) {
                        execSt = -1;
                        super.execSt(execSt);
                    }
                } catch (SocketException se) {

                    // クライアントとの接続が強制的に切れた場合は切断要求とみなす
                    closeFlg = true;
                    if (execSt == 1) {
                        execSt = -1;
                        super.execSt(execSt);
                    }
                }
            }

            // クライアントとの接続を切断
            pw.close();
            bos.close();

            ret = super.SUCCESS;
        } catch(Exception e) {

            logger.error("MasterManagerHelper - executeHelper - Error", e);
            ret = super.ERROR;
            throw new BatchException(e);
        } finally {
            if (execSt == 1) {
                execSt = -1;
                super.execSt(execSt);
            }

            try {
                // クライアントとのメインソケットクローズ
                if (soc != null) {
                    soc.close();
                    soc = null;
                }

                // KeyNodeとの接続を切断
                this.closeAllKeyNodeConnect();
            } catch(Exception e2) {
                logger.error("MasterManagerHelper - executeHelper - Error2", e2);
                ret = super.ERROR;
                throw new BatchException(e2);
            }
        }

        logger.debug("MasterManagerHelper - executeHelper - end");
        return ret;
    }


    /**
     * Helper後処理.<br>
     *
     */
    public void endHelper() {
        this.closeAllKeyNodeConnect();
    }


    /**
     * Key-Valueを保存する.<br>
     * 処理フロー.<br>
     * 1.DataDispatcherに依頼してTagの保存先を問い合わせる。Tag情報を全保存する<br>
     * 2.DataDispatcherに依頼してKeyの保存先を問い合わせる。Tag情報を保存する<br>
     * 3.結果文字列の配列を作成(成功時は処理番号"1"と"true"、失敗時は処理番号"1"と"false")<br>
     *
     * @param keyStr key値の文字列
     * @param tagStr tag値の文字列
     * @param isr クライアントからのインプット
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] setKeyValue(String keyStr, String tagStr, String dataStr) throws BatchException {
        //logger.debug("MasterManagerHelper - setKeyValue - start");
        String[] retStrs = new String[3];
        keyStr = keyStr.trim();

        // Tagは指定なしの場合はクライアントから規定文字列で送られてくるのでここでTagなしの扱いとする
        // ブランクなどでクライアントから送信するとsplit時などにややこしくなる為である。
        tagStr = tagStr.trim();
        if (tagStr.equals(ImdstDefine.imdstBlankStrData)) {
            tagStr = "";
        }

        // data部分はブランクの場合はブランク規定文字列で送られてくるのでそのまま保存する
        dataStr = dataStr.trim();
        String[] tagKeyPair = null;
        String[] keyNodeSaveRet = null;
        String[] keyDataNodePair = null;

        try {
            // Tag値を保存
            if (!tagStr.equals("")) {
                // Tag指定あり
                // タグとキーとのセットをタグ分保存する
                String[] tags = tagStr.split(ImdstDefine.imdstTagKeyAppendSep);

                for (int i = 0; i < tags.length; i++) {

                    // Tag値保存先を問い合わせ
                    String[] tagKeyNodeInfo = DataDispatcher.dispatchKeyNode(tags[i]);
                    tagKeyPair = new String[2];

                    tagKeyPair[0] = tags[i];
                    tagKeyPair[1] = keyStr;

                    // KeyNodeに接続して保存 //
                    // スレーブKeyNodeの存在有無で値を変化させる
                    if (tagKeyNodeInfo.length == 2) {
                        keyNodeSaveRet = this.setKeyNodeValue(tagKeyNodeInfo[0], tagKeyNodeInfo[1], null, null, "3", tagKeyPair);
                    } else if (tagKeyNodeInfo.length == 4) {
                        keyNodeSaveRet = this.setKeyNodeValue(tagKeyNodeInfo[0], tagKeyNodeInfo[1], tagKeyNodeInfo[2], tagKeyNodeInfo[3], "3", tagKeyPair);
                    }

                    // 保存結果確認
                    if (keyNodeSaveRet[1].equals("false")) {
                        // 保存失敗
                        retStrs[0] = "1";
                        retStrs[1] = "false";
                        retStrs[2] = keyNodeSaveRet[2];
                        throw new BatchException("Tag Data Save Error");
                    }
                }
            }

            // キー値とデータを保存
            // 保存先問い合わせ
            String[] keyNodeInfo = DataDispatcher.dispatchKeyNode(keyStr);

            // KeyNodeに接続して保存 //
            keyDataNodePair = new String[2];
            keyDataNodePair[0] = keyStr;
            keyDataNodePair[1] = dataStr;

            // 保存実行
            // スレーブKeyNodeが存在する場合で値を変更
           if (keyNodeInfo.length == 2) {
                keyNodeSaveRet = this.setKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], null, null, "1", keyDataNodePair);
            } else if (keyNodeInfo.length == 4) {
                keyNodeSaveRet = this.setKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], keyNodeInfo[3], "1", keyDataNodePair);
            }

            // 保存結果確認
            if (keyNodeSaveRet[1].equals("false")) {
                // 保存失敗
                retStrs[0] = "1";
                retStrs[1] = "false";
                retStrs[2] = keyNodeSaveRet[2];

                throw new BatchException("Key Data Save Error");
            }

            retStrs[0] = "1";
            retStrs[1] = "true";
            retStrs[2] = "OK";

        } catch (BatchException be) {
            logger.debug("MasterManagerHelper - setData - Error", be);
        } catch (Exception e) {
            retStrs[0] = "1";
            retStrs[1] = "false";
            retStrs[2] = "NG:MasterManagerHelper - setKeyValue - Exception - " + e.toString();
        }
        //logger.debug("MasterManagerHelper - setKeyValue - end");
        return retStrs;
    }


    /**
     * KeyでValueを取得する.<br>
     * 処理フロー.<br>
     * 1.DataDispatcherにKeyを使用してValueの保存先を問い合わせる<br>
     * 2.KeyNodeに接続してValueを取得する<br>
     * 3.結果文字列の配列を作成(成功時は処理番号"2"と"true"とValue、失敗時は処理番号"2"と"false"とValue)<br>
     *
     * @param keyStr key値の文字列
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] getKeyValue(String keyStr) throws BatchException {
        //logger.debug("MasterManagerHelper - getKeyValue - start");
        String[] retStrs = new String[3];
        keyStr = keyStr.trim();

        String[] keyNodeSaveRet = null;
        String[] keyNodeInfo = null;

        try {

            // キー値を使用して取得先を決定
            keyNodeInfo = DataDispatcher.dispatchKeyNode(keyStr);

            // 取得実行
            if (keyNodeInfo.length == 2) {
                keyNodeSaveRet = getKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], null, null ,"2", keyStr);
            } else {
                keyNodeSaveRet = getKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], keyNodeInfo[3], "2", keyStr);
            }

            // 過去に別ルールを設定している場合は過去ルール側でデータ登録が行われている可能性があるのでそちらのルールでの
            // データ格納場所も調べる
            if (keyNodeSaveRet[1].equals("false")) {
                if (this.oldRule != null) {

                    //System.out.println("過去ルールを探索");
                    for (int i = 0; i < this.oldRule.length; i ++) {
                        // キー値を使用して取得先を決定
                        keyNodeInfo = DataDispatcher.dispatchKeyNode(keyStr, this.oldRule[i]);

                        // 取得実行
                        if (keyNodeInfo.length == 2) {
                            keyNodeSaveRet = getKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], null, null, "2", keyStr);
                        } else {
                            keyNodeSaveRet = getKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], keyNodeInfo[3], "2", keyStr);
                        }
                        if (keyNodeSaveRet[1].equals("true")) break;
                    }
                }
            }


            // 取得結果確認
            if (keyNodeSaveRet[1].equals("false")) {


                // 取得失敗(データなし)
                retStrs[0] = keyNodeSaveRet[0];
                retStrs[1] = "false";
                retStrs[2] = "";
                
            } else {
                retStrs[0] = keyNodeSaveRet[0];
                retStrs[1] = "true";
                retStrs[2] = keyNodeSaveRet[2];
            }
        } catch (BatchException be) {
            logger.error("MasterManagerHelper - getKeyValue - Error", be);
        } catch (Exception e) {
            retStrs[0] = "2";
            retStrs[1] = "error";
            retStrs[2] = "NG:MasterManagerHelper - getKeyValue - Exception - " + e.toString();
        }
        //logger.debug("MasterManagerHelper - getKeyValue - end");
        return retStrs;
    }


    /**
     * TagでKey値群を取得する.<br>
     * 処理フロー.<br>
     * 1.DataDispatcherにTagを使用してKey値群の保存先を問い合わせる<br>
     * 2.KeyNodeに接続してKey値群を取得する<br>
     * 3.結果文字列の配列を作成(成功時は処理番号"2"と"true"とKey値群、失敗時は処理番号"2"と"false"とKey値群)<br>
     *
     * @param tagStr tag値の文字列
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] getTagKeys(String tagStr) throws BatchException {
        //logger.debug("MasterManagerHelper - getTagKeys - start");
        String[] retStrs = new String[3];
        tagStr = tagStr.trim();

        String[] keyNodeSaveRet = null;

        try {

            // キー値を使用して取得先を決定
            String[] keyNodeInfo = DataDispatcher.dispatchKeyNode(tagStr);

            // 取得実行
            if (keyNodeInfo.length == 2) {
                keyNodeSaveRet = getKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], null, null, "4", tagStr);
            } else {
                keyNodeSaveRet = getKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], keyNodeInfo[3], "4", tagStr);
            }

            // 取得結果確認
            if (keyNodeSaveRet[1].equals("false")) {

                // 取得失敗(データなし)
                retStrs[0] = keyNodeSaveRet[0];
                retStrs[1] = "false";
                retStrs[2] = "";
            } else {

                // データ有り
                retStrs[0] = keyNodeSaveRet[0];
                retStrs[1] = "true";
                retStrs[2] = keyNodeSaveRet[2];
            }
        } catch (BatchException be) {
            logger.error("MasterManagerHelper - getTagKeys - Error", be);
        } catch (Exception e) {
            logger.error("MasterManagerHelper - getTagKeys - Exception", e);
            retStrs[0] = "4";
            retStrs[1] = "error";
            retStrs[2] = "NG:MasterManagerHelper - getTagKeys - Exception - " + e.toString();
        }

        //logger.debug("MasterManagerHelper - getTagKeys - end");
        return retStrs;
    }



    private String[] getData(byte[] keyBytes, byte[] tagBytes, BufferedOutputStream bos) {
        //logger.debug("MasterManagerHelper - getDatanode - start");
        String[] retStrs = null;
        try {
        } catch (Exception e) {
            logger.error("MasterManagerHelper - getDatanode - Error", e);
        }
        //logger.debug("MasterManagerHelper - getDatanode - end");
        return retStrs;
    }


    /**
     * KeyNodeからデータを取得する.<br>
     * 
     * @param keyNodeName マスターデータノードの名前(IPなど)
     * @param keyNodePort マスターデータノードのアクセスポート番号
     * @param subKeyNodeName スレーブデータノードの名前(IPなど)
     * @param subKeyNodePort スレーブデータノードのアクセスポート番号
     * @param type 処理タイプ(2=Keyでデータを取得, 4=TagでKey値を返す)
     * @param key Key値
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] getKeyNodeValue(String keyNodeName, String keyNodePort, String subKeyNodeName, String subKeyNodePort, String type, String key) throws BatchException {
        PrintWriter pw = null;
        BufferedReader br = null;
        HashMap dtMap = null;

        String[] retParams = null;

        boolean slaveUse = false;
        boolean mainRetry = false;

        SocketException se = null;
        IOException ie = null;
        try {

            // KeyNodeとの接続を確立
            dtMap = this.createKeyNodeConnection(keyNodeName, keyNodePort, false);

            while (true) {

                // 戻り値がnullの場合は何だかの理由で接続に失敗しているのでスレーブの設定がある場合は接続する
                // スレーブの設定がない場合は、エラーとしてExceptionをthrowする
                if (dtMap == null) {
                    if (subKeyNodeName != null) dtMap = this.createKeyNodeConnection(subKeyNodeName, subKeyNodePort, false);

                    if (dtMap == null) throw new BatchException("Key Node IO Error: detail info for log file");
                    slaveUse = true;
                }

                // writerとreaderを取り出し
                pw = (PrintWriter)dtMap.get("writer");
                br = (BufferedReader)dtMap.get("reader");


                try {
                    // 処理種別判別
                    if (type.equals("2")) {

                        // Key値でValueを取得
                        StringBuffer buf = new StringBuffer();
                        // パラメータ作成 処理タイプ[セパレータ]キー値のハッシュ値文字列
                        buf.append("2");
                        buf.append(ImdstDefine.keyHelperClientParamSep);
                        buf.append(new Integer(key.hashCode()).toString());

                        // 送信
                        pw.println(buf.toString());
                        pw.flush();

                        // 返却値取得
                        String retParam = br.readLine();

                        retParams = retParam.split(ImdstDefine.keyHelperClientParamSep);
                    } else if (type.equals("4")) {

                        // Tag値でキー値群を取得
                        StringBuffer buf = new StringBuffer();
                        // パラメータ作成 処理タイプ[セパレータ]キー値のハッシュ値文字列
                        buf.append("4");
                        buf.append(ImdstDefine.keyHelperClientParamSep);
                        buf.append(new Integer(key.hashCode()).toString());

                        // 送信
                        pw.println(buf.toString());
                        pw.flush();

                        // 返却値取得
                        String retParam = br.readLine();
                        retParams = retParam.split(ImdstDefine.keyHelperClientParamSep);
                    }
                    break;
                } catch(SocketException tSe) {
                    // ここでのエラーは通信中に発生しているので、スレーブノードを使用していない場合のみ再度スレーブへの接続を試みる
                    se = tSe;
                } catch(IOException tIe) {
                    // ここでのエラーは通信中に発生しているので、スレーブノードを使用していない場合のみ再度スレーブへの接続を試みる
                    ie = tIe;
                }

                // 既にスレーブの接続を使用している場合は、もう一度だけメインノードに接続を試みる
                // それで駄目な場合はエラーとする
                if (slaveUse) {
                    if (mainRetry) {
                        if (se != null) throw se;
                        if (ie != null) throw ie;
                        throw new BatchException("Key Node IO Error: detail info for log file");
                    } else {

                        // メインKeyNodeとの接続を確立
                        dtMap = this.createKeyNodeConnection(keyNodeName, keyNodePort, true);
                        if (dtMap == null) throw new BatchException("Key Node IO Error: detail info for log file");
                        mainRetry = true;
                    }
                } else {
                    if (subKeyNodeName == null) {
                        if (se != null) throw se;
                        if (ie != null) throw ie;
                    } else{
                        dtMap = null;
                    }
                }
            }
        } catch (Exception e) {
            throw new BatchException(e);
        }
        return retParams;
    }


    /**
     * KeyNodeに対してデータを保存する.<br>
     * 
     * @param keyNodeName マスターデータノードの名前(IPなど)
     * @param keyNodePort マスターデータノードのアクセスポート番号
     * @param subKeyNodeName スレーブデータノードの名前(IPなど)
     * @param subKeyNodePort スレーブデータノードのアクセスポート番号
     * @param type 処理タイプ(1=Keyとデータノード設定, 3=Tagにキーを追加)
     * @param values 送信データ
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] setKeyNodeValue(String keyNodeName, String keyNodePort, String subKeyNodeName, String subKeyNodePort, String type, String[] values) throws BatchException {
        PrintWriter pw = null;
        BufferedReader br = null;
        HashMap dtMap = null;

        String nodeName = keyNodeName;
        String nodePort = keyNodePort;

        String[] retParams = null;

        int counter = 0;


        try {
            do {
                // KeyNodeとの接続を確立
                dtMap = this.createKeyNodeConnection(nodeName, nodePort, false);
                if (dtMap == null) throw new BatchException("Key Node IO Error: detail info for log file");

                // writerとreaderを取り出し
                pw = (PrintWriter)dtMap.get("writer");
                br = (BufferedReader)dtMap.get("reader");

                // 処理種別判別
                if (type.equals("1")) {

                    // Key値でデータノード名を保存
                    StringBuffer buf = new StringBuffer();
                    // パラメータ作成 処理タイプ[セパレータ]キー値のハッシュ値文字列[セパレータ]データノード名
                    buf.append("1");
                    buf.append(ImdstDefine.keyHelperClientParamSep);
                    buf.append(new Integer(values[0].hashCode()).toString());
                    buf.append(ImdstDefine.keyHelperClientParamSep);
                    buf.append(values[1]);

                    // 送信
                    pw.println(buf.toString());
                    pw.flush();

                    // 返却値取得
                    String retParam = br.readLine();

                    retParams = retParam.split(ImdstDefine.keyHelperClientParamSep);

                    if (!retParams[1].equals("true")) break;
                } else if (type.equals("3")) {

                    // Tag値でキー値を保存
                    StringBuffer buf = new StringBuffer();
                    buf.append("3");
                    buf.append(ImdstDefine.keyHelperClientParamSep);
                    buf.append(new Integer(values[0].hashCode()).toString());
                    buf.append(ImdstDefine.keyHelperClientParamSep);
                    buf.append(values[1]);

                    // 送信
                    pw.println(buf.toString());
                    pw.flush();

                    // 返却値取得
                    String retParam = br.readLine();

                    retParams = retParam.split(ImdstDefine.keyHelperClientParamSep);
                    if (!retParams[1].equals("true")) break;
                }

                // スレーブデータノードの名前を代入
                nodeName = subKeyNodeName;
                nodePort = subKeyNodePort;
                counter++;
            // スレーブデータノードが存在しない場合もしくは、既に2回保存を実施した場合は終了
            } while(nodeName != null && counter < 2);
        } catch (BatchException be) {

            throw be;
        } catch (Exception e) {

            throw new BatchException(e);
        }
        return retParams;
    }


    /**
     * KeyNodeとの接続を確立して返す.<br>
     * 接続が確立出来ない場合はエラー結果をログに残し、戻り値はnullとなる.<br>
     *
     * @param keyNodeName
     * @param keyNodePort
     * @param retryFlg キャッシュを一度破棄して再接続を行う
     * @return HashMap
     * @throws BatchException
     */
    private HashMap createKeyNodeConnection(String keyNodeName, String keyNodePort, boolean retryFlg) throws BatchException {
        PrintWriter pw = null;
        BufferedReader br = null;
        HashMap dtMap = null;

        try {

            // フラグがtrueの場合はキャッシュしている接続を破棄してやり直す
            if (retryFlg) {
                if (this.keyNodeConnectMap.containsKey(keyNodeName + ":" + keyNodePort)) this.keyNodeConnectMap.remove(keyNodeName + ":" + keyNodePort);
            }

            // 既にKeyNodeに対するコネクションが確立出来ている場合は使いまわす
            if (this.keyNodeConnectMap.containsKey(keyNodeName + ":" + keyNodePort)) {

                // 接続済み
                //logger.debug("Existing Key Node Connection KeyNodeName = [" + keyNodeName + "] Port = [" + keyNodePort + "]");

                dtMap = (HashMap)this.keyNodeConnectMap.get(keyNodeName + ":" + keyNodePort);
            } else {

                // 新規接続

                //logger.debug("New Key Node Connection KeyNodeName = [" + keyNodeName + "] Port = [" + keyNodePort + "]");
                Socket socket = new Socket(keyNodeName, new Integer(keyNodePort).intValue());

                OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding);
                pw = new PrintWriter(new BufferedWriter(osw));

                InputStreamReader isr = new InputStreamReader(socket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding);
                br = new BufferedReader(isr);

                dtMap = new HashMap();
                dtMap.put("socket", socket);
                dtMap.put("stream_writer", osw);
                dtMap.put("stream_reader", isr);

                dtMap.put("writer", pw);
                dtMap.put("reader", br);
                this.keyNodeConnectMap.put(keyNodeName + ":" + keyNodePort, dtMap);
            }
        } catch (IOException ie) {
            logger.error(ie);
            dtMap = null;
            // 一度接続不慮が発生した場合はこのSESSIONでは接続しない設定とする
            this.keyNodeConnectMap.put(keyNodeName + ":" + keyNodePort, dtMap);
        } catch (Exception e) {
            throw new BatchException(e);
        }

        return dtMap;
    }


    /**
     * 全てのKeyNodeとの接続を切断する.<br>
     *
     */
    private void closeAllKeyNodeConnect() {
        try {
            // KeyNodeとの接続を切断
            if (this.keyNodeConnectMap != null) {
                Set keys = this.keyNodeConnectMap.keySet();
                String connKeyStr = null;
                HashMap cacheConn = null;

                if (keys != null) {
                    for (Iterator iterator = keys.iterator(); iterator.hasNext();) {
                        connKeyStr = (String)iterator.next();
                        cacheConn = (HashMap)this.keyNodeConnectMap.get(connKeyStr);

                        PrintWriter cachePw = (PrintWriter)cacheConn.get("writer");
                        if (cachePw != null) {
                            // 切断要求を送る
                            cachePw.println(ImdstDefine.imdstConnectExitRequest);
                            cachePw.flush();
                            cachePw.close();
                        }

                        BufferedReader cacheBr = (BufferedReader)cacheConn.get("reader");
                        if (cacheBr != null) {
                            cacheBr.close();
                        }

                        OutputStreamWriter cacheOsw = (OutputStreamWriter)cacheConn.get("stream_writer");
                        if (cacheOsw != null) {
                            cacheOsw.close();
                        }

                        InputStreamReader cacheIsr = (InputStreamReader)cacheConn.get("stream_reader");
                        if (cacheIsr != null) {
                            cacheIsr.close();
                        }

                        Socket cacheSoc = (Socket)cacheConn.get("socket");
                        if (cacheSoc != null) {
                            cacheSoc.close();
                            cacheSoc = null;
                        }
                    }
                    this.keyNodeConnectMap = null;
                }
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }

}