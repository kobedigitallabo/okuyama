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
import org.imdst.util.protocol.*;


//import com.sun.mail.util.BASE64DecoderStream;

/**
 * MasterNodeのメイン実行部分<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class MasterManagerHelper extends AbstractMasterManagerHelper {

    private HashMap keyNodeConnectMap = new HashMap();

    private HashMap keyNodeConnectTimeMap = new HashMap();

    // Subノードが存在する場合のデータ保存処理方式を並列処理にするかを指定
    // true:並列 false:順次
    // 並列化すると都度スレッド生成の手間がかかる為、方法を再考する必要がある
    private boolean multiSend = false;

    // 過去ルール
    private int[] oldRule = null;

    // プロトコルモード
    private String protocolMode = null;
    private IProtocolTaker porotocolTaker = null;
    private boolean isProtocolOkuyama = true;

    // 自身のモード(1=Key-Value, 2=DataSystem)
    private int mode = 1;

    private boolean loadBalancing = false;

    private boolean reverseAccess = false;

    // Transactionモードで起動するかを指定
    private boolean transactionMode = false;

    private String[] transactionManagerInfo = null;

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

        String[] retParams = null;
        String retParamStr = null;

        Object[] parameters = super.getParameters();

        String clientParametersStr = null;
        String[] clientParameterList = null;

        IProtocolTaker okuyamaPorotocolTaker = null;

        try{

            // Jobからの引数
            soc = (Socket)parameters[0];

            // 過去ルール
            this.oldRule = (int[])parameters[1];

            // プロトコルモードを取得
            // プロトコルに合わせてTakerを初期化
            this.protocolMode = (String)parameters[2];
            this.porotocolTaker = ProtocolTakerFactory.getProtocolTaker(this.protocolMode);

            // プロトコルがokuyamaではない場合はマネージメントコマンド用のokuyamaプロトコルTakerを作成
            if (!this.protocolMode.equals("okuyama")) {
                isProtocolOkuyama = false;
                okuyamaPorotocolTaker = ProtocolTakerFactory.getProtocolTaker("okuyama");
            }

            // ロードバランシング指定
            if (parameters[3] != null) {
                this.loadBalancing = true;
                reverseAccess = ((Boolean)parameters[3]).booleanValue();
            }

            // トランザクション設定
            this.transactionMode = ((Boolean)parameters[4]).booleanValue();
            if (this.transactionMode) {
                transactionManagerInfo = (String[])parameters[5];
            }

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

                    // 結果格納用String
                    retParamStr = "";


                    // クライアントからの要求を取得
                    // Takerで会話開始
                    clientParametersStr = this.porotocolTaker.takeRequestLine(br, pw);

                    if (this.porotocolTaker.nextExecution() != 1) {

                        // 処理をやり直し
                        if (this.porotocolTaker.nextExecution() == 2) continue;

                        // クライアントからの要求が接続切要求ではないか確認
                        if (this.porotocolTaker.nextExecution() == 3) {
                            closeFlg = true;
                            break;
                        }
                    }

                    // パラメータ分解
                    clientParameterList = clientParametersStr.split(ImdstDefine.keyHelperClientParamSep);

                    // 処理番号で処理を分岐
                    if(clientParameterList[0].equals("0")) {

                        // Client初期化情報
                        retParams = this.initClient();

                    } else if(clientParameterList[0].equals("1")) {
                        //System.out.println(new String(BASE64DecoderStream.decode(clientParameterList[1].getBytes())));

                        // Key値とValueを格納する
                        if (clientParameterList.length > 5) {
                            clientParameterList[4] = 
                                clientParameterList[4] + 
                                    ImdstDefine.keyHelperClientParamSep + 
                                        clientParameterList[5];
                        }

                        retParams = this.setKeyValue(clientParameterList[1], clientParameterList[2], clientParameterList[3], clientParameterList[4]);

                    } else if(clientParameterList[0].equals("2")) {
                        //System.out.println(new String(BASE64DecoderStream.decode(clientParameterList[1].getBytes())));

                        // Key値でValueを取得する
                        retParams = this.getKeyValue(clientParameterList[1]);

                    } else if(clientParameterList[0].equals("3")) {

                        // Tag値でキー値群を取得する
                        retParams = this.getTagKeys(clientParameterList[1]);
                    } else if(clientParameterList[0].equals("(4")) {

                        // Tag値で紐付くキーとValueのセット配列を返す

                    } else if(clientParameterList[0].equals("5")) {

                        // キー値でデータを消す
                        retParams = this.removeKeyValue(clientParameterList[1], clientParameterList[2]);
                    } else if(clientParameterList[0].equals("6")) {

                        // Key値とValueを格納する
                        // 既に登録されている場合は失敗する
                        if (clientParameterList.length > 5) {
                            clientParameterList[4] = 
                                clientParameterList[4] + 
                                    ImdstDefine.keyHelperClientParamSep + 
                                        clientParameterList[5];
                        }

                        retParams = this.setKeyValueOnlyOnce(clientParameterList[1], clientParameterList[2], clientParameterList[3], clientParameterList[4]);

                    } else if(clientParameterList[0].equals("8")) {

                        // Key値でValueを取得する(Scriptを実行する)
                        retParams = this.getKeyValueScript(clientParameterList[1], clientParameterList[2]);
                    } else if(clientParameterList[0].equals("10")) {

                        // データノードを指定することで現在の詳細を取得する
                        // ノードの指定フォーマットは"IP:PORT"
                        String[] nodeDt = new String[3];
                        nodeDt[0] = "10";
                        nodeDt[1] = "true";
                        nodeDt[2] = StatusUtil.getNodeStatusDt(clientParameterList[1]);
                        retParams = nodeDt;
                    } else if(clientParameterList[0].equals("12")) {

                        // 自身の生存結果を返す
                        retParams = new String[3];
                        retParams[0] = "12";
                        retParams[1] = "true";
                        retParams[2] = "";

                    } else if(clientParameterList[0].equals("30")) {

                        // 各キーノードへデータロック依頼
                        retParams = this.lockingData(clientParameterList[1], clientParameterList[2], clientParameterList[3], clientParameterList[4]);
                    } else if(clientParameterList[0].equals("31")) {

                        // 各キーノードへデータロック解除依頼
                        retParams = this.releaseLockingData(clientParameterList[1], clientParameterList[2]);
                    } else if(clientParameterList[0].equals("37")) {

                        // Transactionの開始を行う
                        retParams = this.startTransaction();
                    } else if(clientParameterList[0].equals("38")) {

                        // TransactionのCommitを行う
                        //retParams = this.commitTransaction(clientParameterList[1]);
                    } else if(clientParameterList[0].equals("39")) {

                        // TransactionのRollbackを行う
                        //retParams = this.rollbackTransaction(clientParameterList[1]);
                    } else if (clientParameterList[0].equals("90")) {

                        // KeyNodeの使用停止をマーク
                        retParams = this.pauseKeyNodeUse(clientParameterList[1]);
                    } else if (clientParameterList[0].equals("91")) {

                        // KeyNodeの使用再開をマーク
                        retParams = this.restartKeyNodeUse(clientParameterList[1]);
                    } else if (clientParameterList[0].equals("92")) {

                        // KeyNodeの復旧をマーク
                        retParams = this.arriveKeyNode(clientParameterList[1]);
                    } else if (clientParameterList[0].equals("93")) {

                        // 渡されたKeyNodeの使用停止をマーク
                        String[] nodes = clientParameterList[1].split("_");
                        for (int i = 0; i < nodes.length; i++) {
                            retParams = this.pauseKeyNodeUse(nodes[i]);
                        }
                    } else if (clientParameterList[0].equals("94")) {

                        // 渡されたKeyNodeの使用再開をマーク(複数を一度に)
                        String[] nodes = clientParameterList[1].split("_");
                        for (int i = 0; i < nodes.length; i++) {
                            retParams = this.restartKeyNodeUse(nodes[i]);
                        }
                    } else if (clientParameterList[0].equals("95")) {

                        // 渡されたKeyNodeの障害停止をマーク
                        StatusUtil.setDeadNode(clientParameterList[1]);
                        retParams = new String[3];
                        retParams[0] = "95";
                        retParams[1] = "true";
                        retParams[2] = "";
                    }

                    // Takerで返却値を作成
                    // プロトコルがマッチしていたかをチェック
                    // 設定通りのプロトコルの場合はそのまま処理。そうでない場合はokuyamaで処理
                    if (this.porotocolTaker.isMatchMethod()) {
                        retParamStr = this.porotocolTaker.takeResponseLine(retParams);
                    } else {
                        retParamStr = okuyamaPorotocolTaker.takeResponseLine(retParams);
                    }

                    // クライアントに結果送信
                    if (isProtocolOkuyama) {

                        // Okuyama
                        pw.println(retParamStr);
                    } else{

                        // Okuyama以外の場合
                        pw.print(retParamStr);
                        pw.print("\r\n");
                    }
                    pw.flush();

                } catch (SocketException se) {

                    // クライアントとの接続が強制的に切れた場合は切断要求とみなす
                    closeFlg = true;
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
     * Client初期化情報を返す.<br>
     *
     * @return String[] 結果 配列の3つ目以降が初期化情報
     * @throws BatchException
     */
    private String[] initClient() throws BatchException {
        //logger.debug("MasterManagerHelper - initClient - start");
        String[] retStrs = new String[3];

        retStrs[0] = "0";
        retStrs[1] = "true";
        retStrs[2] = new Integer(ImdstDefine.saveDataMaxSize).toString();
        //logger.debug("MasterManagerHelper - initClient - end");
        return retStrs;
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
    protected String[] setKeyValue(String keyStr, String tagStr, String transactionCode, String dataStr) throws BatchException {
        //logger.debug("MasterManagerHelper - setKeyValue - start");
        String[] retStrs = new String[3];

        // data部分はブランクの場合はブランク規定文字列で送られてくるのでそのまま保存する
        String[] tagKeyPair = null;
        String[] keyNodeSaveRet = null;
        String[] keyDataNodePair = null;

        // Tagは指定なしの場合はクライアントから規定文字列で送られてくるのでここでTagなしの扱いとする
        // ブランクなどでクライアントから送信するとsplit時などにややこしくなる為である。
        if (tagStr.equals(ImdstDefine.imdstBlankStrData)) {
            tagStr = null;
        }

        try {

            // Key値チェック
            if (!this.checkKeyLength(keyStr))  {
                // 保存失敗
                retStrs[0] = "1";
                retStrs[1] = "false";
                retStrs[2] = "Key Length Error";
                return retStrs;
            }

            // Tag値を保存
            if (tagStr != null && !tagStr.equals("")) {

                // Tag指定あり
                // タグとキーとのセットをタグ分保存する
                String[] tags = tagStr.split(ImdstDefine.imdstTagKeyAppendSep);

                for (int i = 0; i < tags.length; i++) {
                    if (!this.checkKeyLength(tags[i]))  {
                        // 保存失敗
                        retStrs[0] = "1";
                        retStrs[1] = "false";
                        throw new BatchException("Tag Data Length Error");

                    }
                    // Tag値保存先を問い合わせ
                    String[] tagKeyNodeInfo = DataDispatcher.dispatchKeyNode(tags[i]);
                    tagKeyPair = new String[2];

                    tagKeyPair[0] = tags[i];
                    tagKeyPair[1] = keyStr;

                    // KeyNodeに接続して保存 //
                    // スレーブKeyNodeの存在有無で値を変化させる
                    if (tagKeyNodeInfo.length == 3) {
                        keyNodeSaveRet = this.setKeyNodeValue(tagKeyNodeInfo[0], tagKeyNodeInfo[1], tagKeyNodeInfo[2], null, null, null, "3", tagKeyPair, transactionCode);
                    } else if (tagKeyNodeInfo.length == 6) {
                        keyNodeSaveRet = this.setKeyNodeValue(tagKeyNodeInfo[0], tagKeyNodeInfo[1], tagKeyNodeInfo[2], tagKeyNodeInfo[3], tagKeyNodeInfo[4], tagKeyNodeInfo[5], "3", tagKeyPair, transactionCode);
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
           if (keyNodeInfo.length == 3) {
                keyNodeSaveRet = this.setKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], null, null, null, "1", keyDataNodePair, transactionCode);
            } else if (keyNodeInfo.length == 6) {
                keyNodeSaveRet = this.setKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], keyNodeInfo[3], keyNodeInfo[4], keyNodeInfo[5], "1", keyDataNodePair, transactionCode);
            }

            // 保存結果確認
            if (keyNodeSaveRet[1].equals("false")) {
                // 保存失敗
                retStrs[0] = "1";
                retStrs[1] = "false";
                retStrs[2] = keyNodeSaveRet[2];

            } else if(keyNodeSaveRet[1].equals("true")) {

                retStrs[0] = "1";
                retStrs[1] = "true";
                retStrs[2] = "OK";
            } else {
                throw new BatchException("Key Data Save Error");
            }
        } catch (BatchException be) {
            logger.info("MasterManagerHelper - setKeyValue - Error", be);
        } catch (Exception e) {
            logger.info("MasterManagerHelper - setKeyValue - Error", e);
            retStrs[0] = "1";
            retStrs[1] = "false";
            retStrs[2] = "NG:MasterManagerHelper - setKeyValue - Exception - " + e.toString();
        }
        //logger.debug("MasterManagerHelper - setKeyValue - end");
        return retStrs;
    }


    /**
     * Key-Valueを保存する.<br>
     * 既に登録済みの場合は失敗する.<br>
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
    private String[] setKeyValueOnlyOnce(String keyStr, String tagStr, String transactionCode, String dataStr) throws BatchException {
        //logger.debug("MasterManagerHelper - setKeyValueOnlyOnce - start");
        String[] retStrs = new String[3];

        // data部分はブランクの場合はブランク規定文字列で送られてくるのでそのまま保存する
        String[] tagKeyPair = null;
        String[] keyNodeSaveRet = null;
        String[] keyDataNodePair = null;

        // Tagは指定なしの場合はクライアントから規定文字列で送られてくるのでここでTagなしの扱いとする
        // ブランクなどでクライアントから送信するとsplit時などにややこしくなる為である。
        if (tagStr.equals(ImdstDefine.imdstBlankStrData)) {
            tagStr = null;
        }

        try {
            // Key値チェック
            if (!this.checkKeyLength(keyStr))  {
                // 保存失敗
                retStrs[0] = "6";
                retStrs[1] = "false";
                retStrs[2] = "Key Length Error";
                return retStrs;
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
           if (keyNodeInfo.length == 3) {
                keyNodeSaveRet = this.setKeyNodeValueOnlyOnce(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], null, null, null, "1", keyDataNodePair, transactionCode);
            } else if (keyNodeInfo.length == 6) {
                keyNodeSaveRet = this.setKeyNodeValueOnlyOnce(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], keyNodeInfo[3], keyNodeInfo[4], keyNodeInfo[5], "1", keyDataNodePair, transactionCode);
            }

            // 保存結果確認
            if (keyNodeSaveRet[1].equals("false")) {
                // 保存失敗
                retStrs[0] = "6";
                retStrs[1] = "false";
                retStrs[2] = keyNodeSaveRet[2];

                return retStrs;
            }


            // Tag値を保存
            if (tagStr != null && !tagStr.equals("")) {

                // Tag指定あり
                // タグとキーとのセットをタグ分保存する
                String[] tags = tagStr.split(ImdstDefine.imdstTagKeyAppendSep);

                for (int i = 0; i < tags.length; i++) {
                    if (!this.checkKeyLength(tags[i]))  {
                        // 保存失敗
                        retStrs[0] = "6";
                        retStrs[1] = "false";
                        throw new BatchException("Tag Length Error");
                    }

                    // Tag値保存先を問い合わせ
                    String[] tagKeyNodeInfo = DataDispatcher.dispatchKeyNode(tags[i]);
                    tagKeyPair = new String[2];

                    tagKeyPair[0] = tags[i];
                    tagKeyPair[1] = keyStr;

                    // KeyNodeに接続して保存 //
                    // スレーブKeyNodeの存在有無で値を変化させる
                    if (tagKeyNodeInfo.length == 3) {
                        keyNodeSaveRet = this.setKeyNodeValue(tagKeyNodeInfo[0], tagKeyNodeInfo[1], tagKeyNodeInfo[2], null, null, null, "3", tagKeyPair, transactionCode);
                    } else if (tagKeyNodeInfo.length == 6) {
                        keyNodeSaveRet = this.setKeyNodeValue(tagKeyNodeInfo[0], tagKeyNodeInfo[1], tagKeyNodeInfo[2], tagKeyNodeInfo[3], tagKeyNodeInfo[4], tagKeyNodeInfo[5], "3", tagKeyPair, transactionCode);
                    }

                    // 保存結果確認
                    if (keyNodeSaveRet[1].equals("false")) {
                        // 保存失敗
                        retStrs[0] = "6";
                        retStrs[1] = "false";
                        retStrs[2] = keyNodeSaveRet[2];
                        throw new BatchException("Tag Data Save Error");
                    }
                }
            }


            retStrs[0] = "6";
            retStrs[1] = "true";
            retStrs[2] = "OK";

        } catch (BatchException be) {
            logger.info("MasterManagerHelper - setKeyValueOnlyOnce - Error", be);
        } catch (Exception e) {
            logger.info("MasterManagerHelper - setKeyValueOnlyOnce - Error", e);
            retStrs[0] = "6";
            retStrs[1] = "false";
            retStrs[2] = "NG:MasterManagerHelper - setKeyValueOnlyOnce - Exception - " + e.toString();
        }
        //logger.debug("MasterManagerHelper - setKeyValueOnlyOnce - end");
        return retStrs;
    }


    /**
     * KeyでValueを取得する.<br>
     * 処理フロー.<br>
     * 1.DataDispatcherにKeyを使用してValueの保存先を問い合わせる<br>
     * 2.KeyNodeに接続してValueを取得する<br>
     * 3.結果文字列の配列を作成(成功時は処理番号"2"と"true"とValue、失敗時は処理番号"2"と"false"とValue)<br>
     *
     * TODD:過去ルールで取得出来た情報はここで反映<br>
     *      一時的なものとして後で別サービス化する.<br>
     *
     * @param keyStr key値の文字列
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] getKeyValue(String keyStr) throws BatchException {
        //logger.debug("MasterManagerHelper - getKeyValue - start");
        String[] retStrs = new String[3];

        String[] keyNodeSaveRet = null;
        String[] keyNodeInfo = null;

        try {
            if (!this.checkKeyLength(keyStr))  {
                // 保存失敗
                retStrs[0] = "2";
                retStrs[1] = "false";
                retStrs[2] = "Key Length Error";
                return retStrs;
            }

            // キー値を使用して取得先を決定
            if (loadBalancing) {
                keyNodeInfo = DataDispatcher.dispatchReverseKeyNode(keyStr, reverseAccess);
            } else {
                keyNodeInfo = DataDispatcher.dispatchKeyNode(keyStr);
            }

            // 取得実行
            if (keyNodeInfo.length == 3) {
                keyNodeSaveRet = getKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], null, null, null,  "2", keyStr);
            } else {
                keyNodeSaveRet = getKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], keyNodeInfo[3], keyNodeInfo[4], keyNodeInfo[5], "2", keyStr);
            }

            // 過去に別ルールを設定している場合は過去ルール側でデータ登録が行われている可能性があるの
            // でそちらのルールでのデータ格納場所も調べる
            if (keyNodeSaveRet[1].equals("false")) {
                if (this.oldRule != null) {

                    //System.out.println("過去ルールを探索");
                    for (int i = 0; i < this.oldRule.length; i++) {

                        // キー値を使用して取得先を決定
                        if (loadBalancing) {
                            keyNodeInfo = DataDispatcher.dispatchReverseKeyNode(keyStr, reverseAccess, this.oldRule[i]);
                        } else {
                            keyNodeInfo = DataDispatcher.dispatchKeyNode(keyStr, this.oldRule[i]);
                        }

                        // 取得実行
                        if (keyNodeInfo.length == 3) {
                            keyNodeSaveRet = getKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], null, null, null, "2", keyStr);
                        } else {
                            keyNodeSaveRet = getKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], keyNodeInfo[3], keyNodeInfo[4], keyNodeInfo[5], "2", keyStr);
                        }

                        // 過去ルールからデータを発見
                        if (keyNodeSaveRet[1].equals("true")) {

                            // TODO:現在のノードへの反映はいずれ別サービス化する
                            // 過去ルールによって取得出来たデータを現在のルールのノードへ反映
                            try {
                                // 現在ルールのノードにデータ反映
                                setKeyValueOnlyOnce(keyStr, ImdstDefine.imdstBlankStrData, "0", keyNodeSaveRet[2]);

                                // 過去ルールの場所のデータを削除
                                if (keyNodeInfo.length == 3) {
                                    removeKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], null, null, null, keyStr, "0");
                                } else {
                                    removeKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], keyNodeInfo[3], keyNodeInfo[4], keyNodeInfo[5], keyStr, "0");
                                }
                            } catch (Exception e) {
                                logger.info("Old Rule Data Set Error" + e);
                            }
                            break;
                        }
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
            e.printStackTrace();
            retStrs[0] = "2";
            retStrs[1] = "error";
            retStrs[2] = "NG:MasterManagerHelper - getKeyValue - Exception - " + e.toString();
        }
        //logger.debug("MasterManagerHelper - getKeyValue - end");
        return retStrs;
    }


    /**
     * KeyでValueを取得する.<br>
     * Scriptも同時に実行する.<br>
     * 処理フロー.<br>
     * 1.DataDispatcherにKeyを使用してValueの保存先を問い合わせる<br>
     * 2.KeyNodeに接続してValueを取得する<br>
     * 3.結果文字列の配列を作成(成功時は処理番号"8"と"true"とValue、データが存在しない場合は処理番号"8"と"false"とValue、スクリプトエラー時は処理番号"8"と"error"とエラーメッセージ)<br>
     *
     *
     * @param keyStr key値の文字列
     * @param scriptStr 実行Scriptの文字列
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] getKeyValueScript(String keyStr, String scriptStr) throws BatchException {
        //logger.debug("MasterManagerHelper - getKeyValueScript - start");
        String[] retStrs = new String[3];

        String[] keyNodeSaveRet = null;
        String[] keyNodeInfo = null;

        try {
            if (!this.checkKeyLength(keyStr))  {
                // 保存失敗
                retStrs[0] = "8";
                retStrs[1] = "false";
                retStrs[2] = "Key Length Error";
                return retStrs;
            }

            // キー値を使用して取得先を決定
            if (loadBalancing) {
                keyNodeInfo = DataDispatcher.dispatchReverseKeyNode(keyStr, reverseAccess);
            } else {
                keyNodeInfo = DataDispatcher.dispatchKeyNode(keyStr);
            }

            // 取得実行
            if (keyNodeInfo.length == 3) {
                keyNodeSaveRet = getKeyNodeValueScript(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], null, null, null,  "8", keyStr, scriptStr);
            } else {
                keyNodeSaveRet = getKeyNodeValueScript(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], keyNodeInfo[3], keyNodeInfo[4], keyNodeInfo[5], "8", keyStr, scriptStr);
            }

            // 過去に別ルールを設定している場合は過去ルール側でデータ登録が行われている可能性があるの
            // でそちらのルールでのデータ格納場所も調べる
            if (keyNodeSaveRet[1].equals("false")) {
                if (this.oldRule != null) {

                    //System.out.println("過去ルールを探索");
                    for (int i = 0; i < this.oldRule.length; i++) {

                        // キー値を使用して取得先を決定
                        if (loadBalancing) {
                            keyNodeInfo = DataDispatcher.dispatchReverseKeyNode(keyStr, reverseAccess, this.oldRule[i]);
                        } else {
                            keyNodeInfo = DataDispatcher.dispatchKeyNode(keyStr, this.oldRule[i]);
                        }

                        // 取得実行
                        if (keyNodeInfo.length == 3) {
                            keyNodeSaveRet = getKeyNodeValueScript(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], null, null, null, "8", keyStr, scriptStr);
                        } else {
                            keyNodeSaveRet = getKeyNodeValueScript(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], keyNodeInfo[3], keyNodeInfo[4], keyNodeInfo[5], "8", keyStr, scriptStr);
                        }

                        // 過去ルールからデータを発見
                        if (keyNodeSaveRet[1].equals("true")) {
                            break;
                        }
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
                // trueもしくはerrorの可能性あり
                retStrs[0] = keyNodeSaveRet[0];
                retStrs[1] = keyNodeSaveRet[1];
                retStrs[2] = keyNodeSaveRet[2];
            }
        } catch (BatchException be) {
            logger.error("MasterManagerHelper - getKeyValueScript - Error", be);
        } catch (Exception e) {
            e.printStackTrace();
            retStrs[0] = "8";
            retStrs[1] = "error";
            retStrs[2] = "NG:MasterManagerHelper - getKeyValueScript - Exception - " + e.toString();
        }
        //logger.debug("MasterManagerHelper - getKeyValueScript - end");
        return retStrs;
    }


    /**
     * KeyでValueを削除する.<br>
     * 処理フロー.<br>
     * 1.DataDispatcherにKeyを使用してValueの保存先を問い合わせる<br>
     * 2.KeyNodeに接続してValueを削除する<br>
     * 3.結果文字列の配列を作成(成功時は処理番号"5"と"true"とValue、失敗時は処理番号"5"と"false")<br>
     *
     * @param keyStr key値の文字列
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] removeKeyValue(String keyStr, String transactionCode) throws BatchException {
        //logger.debug("MasterManagerHelper - removeKeyValue - start");
        String[] retStrs = new String[3];

        String[] keyNodeSaveRet = null;
        String[] keyNodeInfo = null;

        try {
            // Key値チェック
            if (!this.checkKeyLength(keyStr))  {
                retStrs[0] = "5";
                retStrs[1] = "false";
                retStrs[2] = "Key Length Error";
                return retStrs;
            }

            // キー値を使用して取得先を決定
            keyNodeInfo = DataDispatcher.dispatchKeyNode(keyStr);

            // 取得実行
            if (keyNodeInfo.length == 3) {
                keyNodeSaveRet = removeKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], null, null, null, keyStr, transactionCode);
            } else {
                keyNodeSaveRet = removeKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], keyNodeInfo[3], keyNodeInfo[4], keyNodeInfo[5], keyStr, transactionCode);
            }

            // 過去に別ルールを設定している場合は過去ルール側でデータ登録が行われている可能性があるので
            // そちらのルールでも削除する
            if (this.oldRule != null) {

                //System.out.println("過去ルールを探索");
                for (int i = 0; i < this.oldRule.length; i++) {
                    // キー値を使用して取得先を決定
                    keyNodeInfo = DataDispatcher.dispatchKeyNode(keyStr, this.oldRule[i]);

                    // 取得実行
                    if (keyNodeInfo.length == 3) {
                        removeKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], null, null, null, keyStr, transactionCode);
                    } else {
                        removeKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], keyNodeInfo[3], keyNodeInfo[4], keyNodeInfo[5], keyStr, transactionCode);
                    }
                }
            }

            // 取得結果確認
            if (keyNodeSaveRet[1].equals("false")) {


                // 削除失敗(元データなし)
                retStrs[0] = keyNodeSaveRet[0];
                retStrs[1] = "false";
                retStrs[2] = "";
                
            } else {
                retStrs[0] = keyNodeSaveRet[0];
                retStrs[1] = "true";
                retStrs[2] = keyNodeSaveRet[2];
            }
        } catch (BatchException be) {
            logger.error("MasterManagerHelper - removeKeyValue - Error", be);
        } catch (Exception e) {
            retStrs[0] = "5";
            retStrs[1] = "error";
            retStrs[2] = "NG:MasterManagerHelper - removeKeyValue - Exception - " + e.toString();
        }
        //logger.debug("MasterManagerHelper - removeKeyValue - end");
        return retStrs;
    }


    /**
     * Key値を指定してデータのロックを取得する.<br>
     * 処理フロー.<br>
     * 1.DataDispatcherに依頼してKeyの保存先を問い合わせる.<br>
     * 2.取得した保存先にロックを依頼.<br>
     * 3.全てのロックが完了した時点で終了.<br>
     * 4.結果文字列の配列を作成(成功時は処理番号"30"と"true"と"ロック番号"、失敗時は処理番号"30"と"false")<br>
     *
     * @param keyStrs key値
     * @param transactionCode ロック時のTransactionCode
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] lockingData(String keyStr, String transactionCode, String lockingTime, String lockingWaitTime) throws BatchException {
        //logger.debug("MasterManagerHelper - lockingData - start");

        String[] retStrs = new String[3];

        String[] keyNodeLockRet = null;

        try {

            if (!transactionMode) {
                retStrs = new String[2];
                retStrs[0] = "30";
                retStrs[1] = "false";
                return retStrs;
            }

            // Key値チェック
            if (!this.checkKeyLength(keyStr))  {
                retStrs[0] = "30";
                retStrs[1] = "false";
                retStrs[2] = "Key Length Error";
                return retStrs;
            }

            // TransactionManagerに処理を依頼
            keyNodeLockRet = lockKeyNodeValue(transactionManagerInfo[0], transactionManagerInfo[1], keyStr, transactionCode, lockingTime, lockingWaitTime);


            // 取得結果確認
            if (keyNodeLockRet[1].equals("false")) {

                // Lock失敗
                retStrs[0] = keyNodeLockRet[0];
                retStrs[1] = "false";
                retStrs[2] = "";
                
            } else {

                // Lock成功
                retStrs[0] = keyNodeLockRet[0];
                retStrs[1] = "true";
                retStrs[2] = keyNodeLockRet[2];
            }
        } catch (BatchException be) {
            logger.error("MasterManagerHelper - lockingData - Error", be);
        } catch (Exception e) {
            retStrs[0] = "30";
            retStrs[1] = "error";
            retStrs[2] = "NG:MasterManagerHelper - lockingData - Exception - " + e.toString();
        }
        //logger.debug("MasterManagerHelper - lockingData - end");;
        return retStrs;
    }


    /**
     * Key値を指定してデータのロックを解除する.<br>
     * 処理フロー.<br>
     * 2.DataDispatcherに依頼してKeyの保存先を問い合わせる.<br>
     * 3.取得した保存先にロックを依頼.<br>
     * 4.全てのロックが完了した時点で終了.<br>
     * 5.結果文字列の配列を作成(成功時は処理番号"31"と"true"と"ロック番号"、失敗時は処理番号"31"と"false")<br>
     *
     * @param keyStr key値の文字列
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] releaseLockingData(String keyStr, String transactionCode) throws BatchException {
        //logger.debug("MasterManagerHelper - releaseLockingData - start");
        String[] retStrs = new String[3];

        String[] keyNodeReleaseRet = null;

        try {

            if (!transactionMode) {
                retStrs = new String[2];
                retStrs[0] = "31";
                retStrs[1] = "false";
                return retStrs;
            }

            // Key値チェック
            if (!this.checkKeyLength(keyStr))  {
                retStrs[0] = "31";
                retStrs[1] = "false";
                retStrs[2] = "Key Length Error";
                return retStrs;
            }

            // TransactionManagerに処理を依頼
            keyNodeReleaseRet = releaseLockKeyNodeValue(transactionManagerInfo[0], transactionManagerInfo[1], keyStr, transactionCode);


            // リリース結果確認
            if (keyNodeReleaseRet[1].equals("false")) {

                // リリース失敗
                retStrs[0] = "31";
                retStrs[1] = "false";
                retStrs[2] = "";
            } else {

                // リリース成功
                retStrs[0] = "31";
                retStrs[1] = "true";
                retStrs[2] = keyNodeReleaseRet[2];
            }
        } catch (BatchException be) {
            logger.error("MasterManagerHelper - releaseLockingData - Error", be);
        } catch (Exception e) {
            retStrs[0] = "31";
            retStrs[1] = "error";
            retStrs[2] = "NG:MasterManagerHelper - releaseLockingData - Exception - " + e.toString();
        }
        //logger.debug("MasterManagerHelper - releaseLockingData - end");
        return retStrs;
    }


    /**
     * Transactionを開始する.<br>
     *
     * @return String[] 結果 配列の3つめがTransactionCode
     * @throws BatchException
     */
    private String[] startTransaction() throws BatchException {
        //logger.debug("MasterManagerHelper - removeKeyValue - start");
        String[] retStrs = new String[3];
        if (!transactionMode) {
            retStrs[0] = "37";
            retStrs[1] = "false";
            retStrs[2] = "No Transaction Mode";
            return retStrs;
        }

        retStrs[0] = "37";
        retStrs[1] = "true";
        retStrs[2] = ((Long)System.nanoTime()).toString() + new Integer(this.hashCode()).toString();
        //logger.debug("MasterManagerHelper - removeKeyValue - end");
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

        String[] keyNodeSaveRet = null;

        try {

            // Key値チェック
            if (!this.checkKeyLength(tagStr))  {
                retStrs[0] = "4";
                retStrs[1] = "false";
                retStrs[2] = "Tag Length Error";
                return retStrs;
            }

            // キー値を使用して取得先を決定
            String[] keyNodeInfo = DataDispatcher.dispatchKeyNode(tagStr);


            // 取得実行
            if (keyNodeInfo.length == 3) {
                keyNodeSaveRet = getKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], null, null, null, "4", tagStr);
            } else {
                keyNodeSaveRet = getKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], keyNodeInfo[3], keyNodeInfo[4], keyNodeInfo[5], "4", tagStr);
            }

            // 過去に別ルールを設定している場合は過去ルール側でデータ登録が行われている可能性があるので
            // そちらのルールでのデータ格納場所も調べる
            if (keyNodeSaveRet[1].equals("false")) {
                if (this.oldRule != null) {

                    //System.out.println("過去ルールを探索");
                    for (int i = 0; i < this.oldRule.length; i++) {

                        // キー値を使用して取得先を決定
                        if (loadBalancing) {
                            keyNodeInfo = DataDispatcher.dispatchReverseKeyNode(tagStr, this.reverseAccess, this.oldRule[i]);
                        } else {
                            keyNodeInfo = DataDispatcher.dispatchKeyNode(tagStr, this.oldRule[i]);
                        }

                        // 取得実行
                        if (keyNodeInfo.length == 3) {
                            keyNodeSaveRet = getKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], null, null, null, "4", tagStr);
                        } else {
                            keyNodeSaveRet = getKeyNodeValue(keyNodeInfo[0], keyNodeInfo[1], keyNodeInfo[2], keyNodeInfo[3], keyNodeInfo[4], keyNodeInfo[5], "4", tagStr);
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
    private String[] getKeyNodeValue(String keyNodeName, String keyNodePort, String keyNodeFullName, String subKeyNodeName, String subKeyNodePort, String subKeyNodeFullName, String type, String key) throws BatchException {
        PrintWriter pw = null;
        BufferedReader br = null;
        HashMap dtMap = null;

        String[] retParams = null;

        boolean slaveUse = false;
        boolean mainRetry = false;

        String nowUseNodeInfo = null;

        SocketException se = null;
        IOException ie = null;
        try {

            // KeyNodeとの接続を確立
            dtMap = this.createKeyNodeConnection(keyNodeName, keyNodePort, keyNodeFullName, false);

            while (true) {

                // 戻り値がnullの場合は何だかの理由で接続に失敗しているのでスレーブの設定がある場合は接続する
                // スレーブの設定がない場合は、エラーとしてExceptionをthrowする
                if (dtMap == null) {
                    if (subKeyNodeName != null) dtMap = this.createKeyNodeConnection(subKeyNodeName, subKeyNodePort, subKeyNodeFullName, false);

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
                        buf.append(this.stringCnv(key));

                        // 送信
                        pw.println(buf.toString());
                        pw.flush();

                        // 返却値取得
                        String retParam = br.readLine();
                        // 返却値を分解
                        // 処理番号, true or false, valueの想定
                        // value値にセパレータが入っていても無視する
                        retParams = retParam.split(ImdstDefine.keyHelperClientParamSep, 3);
                    } else if (type.equals("4")) {

                        // Tag値でキー値群を取得
                        StringBuffer buf = new StringBuffer();
                        // パラメータ作成 処理タイプ[セパレータ]キー値のハッシュ値文字列
                        buf.append("4");
                        buf.append(ImdstDefine.keyHelperClientParamSep);
                        buf.append(this.stringCnv(key));

                        // 送信
                        pw.println(buf.toString());
                        pw.flush();

                        // 返却値取得
                        String retParam = br.readLine();

                        // 返却値を分解
                        // 処理番号, true or false, valueの想定
                        // value値にセパレータが入っていても無視する
                        retParams = retParam.split(ImdstDefine.keyHelperClientParamSep, 3);
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
                        dtMap = this.createKeyNodeConnection(keyNodeName, keyNodePort, keyNodeFullName, true);
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
        } finally {
            // ノードの使用終了をマーク
            super.execNodeUseEnd(keyNodeFullName);

            if (subKeyNodeName != null) 
                super.execNodeUseEnd(subKeyNodeFullName);
        }
        return retParams;
    }


    /**
     * KeyNodeからデータを取得する.<br>
     * 
     * @param keyNodeName マスターデータノードの名前(IPなど)
     * @param keyNodePort マスターデータノードのアクセスポート番号
     * @param subKeyNodeName スレーブデータノードの名前(IPなど)
     * @param subKeyNodePort スレーブデータノードのアクセスポート番号
     * @param type 処理タイプ(8=Keyでデータを取得)
     * @param scriptStr Script文字列
     * @param key Key値
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] getKeyNodeValueScript(String keyNodeName, String keyNodePort, String keyNodeFullName, String subKeyNodeName, String subKeyNodePort, String subKeyNodeFullName, String type, String key, String scriptStr) throws BatchException {
        PrintWriter pw = null;
        BufferedReader br = null;
        HashMap dtMap = null;

        String[] retParams = null;

        boolean slaveUse = false;
        boolean mainRetry = false;

        String nowUseNodeInfo = null;

        SocketException se = null;
        IOException ie = null;
        try {

            // KeyNodeとの接続を確立
            dtMap = this.createKeyNodeConnection(keyNodeName, keyNodePort, keyNodeFullName, false);

            while (true) {

                // 戻り値がnullの場合は何だかの理由で接続に失敗しているのでスレーブの設定がある場合は接続する
                // スレーブの設定がない場合は、エラーとしてExceptionをthrowする
                if (dtMap == null) {
                    if (subKeyNodeName != null) dtMap = this.createKeyNodeConnection(subKeyNodeName, subKeyNodePort, subKeyNodeFullName, false);

                    if (dtMap == null) throw new BatchException("Key Node IO Error: detail info for log file");
                    slaveUse = true;
                }

                // writerとreaderを取り出し
                pw = (PrintWriter)dtMap.get("writer");
                br = (BufferedReader)dtMap.get("reader");


                try {
                    // 処理種別判別
                    if (type.equals("8")) {

                        // Key値でValueを取得
                        StringBuffer buf = new StringBuffer();
                        // パラメータ作成 処理タイプ[セパレータ]キー値のハッシュ値文字列
                        buf.append("8");
                        buf.append(ImdstDefine.keyHelperClientParamSep);
                        buf.append(this.stringCnv(key));
                        buf.append(ImdstDefine.keyHelperClientParamSep);
                        buf.append(scriptStr);

                        // 送信
                        pw.println(buf.toString());
                        pw.flush();

                        // 返却値取得
                        String retParam = br.readLine();

                        // 返却値を分解
                        // 処理番号, true or false, valueの想定
                        // value値にセパレータが入っていても無視する
                        retParams = retParam.split(ImdstDefine.keyHelperClientParamSep, 3);
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
                        dtMap = this.createKeyNodeConnection(keyNodeName, keyNodePort, keyNodeFullName, true);
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
        } finally {
            // ノードの使用終了をマーク
            super.execNodeUseEnd(keyNodeFullName);

            if (subKeyNodeName != null) 
                super.execNodeUseEnd(subKeyNodeFullName);
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
     * @param type 処理タイプ(1=Keyとデータノード設定, 3=Tagにキーを追加, 30=ロックを取得, 31=ロックを解除)
     * @param values 送信データ
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] setKeyNodeValue(String keyNodeName, String keyNodePort, String keyNodeFullName, String subKeyNodeName, String subKeyNodePort, String subKeyNodeFullName, String type, String[] values, String transactionCode) throws BatchException {

        // 並列処理指定の場合は分岐(試験的に導入(デフォルト無効))
        if (multiSend) return this.setKeyNodeValueMultiSend(keyNodeName, keyNodePort, keyNodeFullName, subKeyNodeName, subKeyNodePort, subKeyNodeFullName, type, values, transactionCode);

        PrintWriter pw = null;
        BufferedReader br = null;
        HashMap dtMap = null;

        String nodeName = keyNodeName;
        String nodePort = keyNodePort;
        String nodeFullName = keyNodeFullName;

        String[] retParams = null;

        int counter = 0;

        String tmpSaveHost = null;
        String[] tmpSaveData = null;
        String retParam = null;

        boolean mainNodeSave = false;
        boolean subNodeSave = false;

        StringBuffer buf = new StringBuffer();
        String sendData = null;
        boolean subNodeConnect = false;

        try {

            // TransactionModeの状態に合わせてLock状態を確かめる
            if (transactionMode) {
                while (true) {

                    // TransactionMode時
                    // TransactionManagerに処理を依頼
                    String[] keyNodeLockRet = hasLockKeyNode(transactionManagerInfo[0], transactionManagerInfo[1], values[0]);

                    // 取得結果確認
                    if (keyNodeLockRet[1].equals("true")) {

                        if (keyNodeLockRet[2].equals(transactionCode)) break;
                    } else {
                        break;
                    }
                }
            }


            // 送信パラメータ作成 処理タイプ[セパレータ]キー値のハッシュ値文字列[セパレータ]データノード名
            if (type.equals("1")) {

                // Key値でValueを保存
                buf.append("1");
            } else if (type.equals("3")) {

                // Tag値でキー値を保存
                buf.append("3");
            } else {
                throw new BatchException("setKeyNodeValue No Type Method [" + type + "]");
            }

            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append(this.stringCnv(values[0]));               // Key値
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append(transactionCode);                    // Transaction値
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append(values[1]);                          // Value値
            sendData = buf.toString();

            // KeyNodeとの接続を確立
            dtMap = this.createKeyNodeConnection(nodeName, nodePort, nodeFullName, false);

            // DataNodeに送信
            do {

                // 接続結果と、現在の保存先状況で処理を分岐
                if (dtMap != null) {
                    try {
                        // writerとreaderを取り出し
                        pw = (PrintWriter)dtMap.get("writer");
                        br = (BufferedReader)dtMap.get("reader");

//long start1 = System.nanoTime();
                        // 送信
                        pw.println(buf.toString());
                        pw.flush();

                        // SlaveNodeに保存する必要がある場合はDataNodeからの処理返却待ちの間に接続を済ませておく
                        if (counter == 0 && subKeyNodeName != null) {
                            dtMap = this.createKeyNodeConnection(subKeyNodeName, subKeyNodePort, subKeyNodeFullName, false);
                            subNodeConnect = true;
                        }

                        // 返却値取得
                        retParam = br.readLine();


                        // 処理種別判別
                        if (type.equals("1")) {

                            // Key値でValueを保存
                            // splitは遅いので特定文字列で返却値が始まるかをチェックし始まる場合は登録成功
                            //retParams = retParam.split(ImdstDefine.keyHelperClientParamSep);
                            if (retParam != null && retParam.indexOf(ImdstDefine.keyNodeKeyRegistSuccessStr) == 0) {
                                if (counter == 0) mainNodeSave = true;
                                if (counter == 1) subNodeSave = true;
                            } else {
                                // 論理的に登録失敗
                                super.setDeadNode(nodeName + ":" + nodePort, 3, null);
                                logger.error("setKeyNodeValue Logical Error Node =["  + nodeName + ":" + nodePort + "] retParam=[" + retParam + "]");
                            }
                        } else if (type.equals("3")) {

                            // Tag値でキー値を保存

                            // splitは遅いので特定文字列で返却値が始まるかをチェックし始まる場合は登録成功
                            //retParams = retParam.split(ImdstDefine.keyHelperClientParamSep);
                            if (retParam != null && retParam.indexOf(ImdstDefine.keyNodeTagRegistSuccessStr) == 0) {
                                if (counter == 0) mainNodeSave = true;
                                if (counter == 1) subNodeSave = true;
                            } else {
                                // 論理的に登録失敗
                                super.setDeadNode(nodeName + ":" + nodePort, 4, null);
                                logger.error("setKeyNodeValue Logical Error Node =["  + nodeName + ":" + nodePort + "] retParam=[" + retParam + "]");
                            }
                        }

                    } catch (SocketException se) {

                        super.setDeadNode(nodeName + ":" + nodePort, 5, se);
                        logger.debug(se);
                    } catch (IOException ie) {

                        super.setDeadNode(nodeName + ":" + nodePort, 6, ie);
                        logger.debug(ie);
                    } catch (Exception ee) {
                        super.setDeadNode(nodeName + ":" + nodePort, 7, ee);
                        logger.debug(ee);
                    }

                } 

                // SubNodeの指定は存在するが接続前に処理を抜けた場合はここで接続
                // 原因はMainNodeの接続に失敗したか、接続後Exceptionが発生した場合
                if (subNodeConnect == false && subKeyNodeName != null) {
                    dtMap = this.createKeyNodeConnection(subKeyNodeName, subKeyNodePort, subKeyNodeFullName, false);
                }

                // スレーブデータノードの名前を代入
                nodeName = subKeyNodeName;
                nodePort = subKeyNodePort;
                nodeFullName = subKeyNodeFullName;

                counter++;
                // スレーブデータノードが存在しない場合もしくは、既に2回保存を実施した場合は終了
            } while(nodeName != null && counter < 2);

            // ノードへの保存状況を確認
            if (mainNodeSave == false && subNodeSave == false) {
                if (retParam == null) {
                    throw new BatchException("Key Node IO Error: detail info for log file");
                }
            }

        } catch (BatchException be) {

            throw be;
        } catch (Exception e) {

            throw new BatchException(e);
        } finally {
            // ノードの使用終了をマーク
            super.execNodeUseEnd(keyNodeFullName);

            if (subKeyNodeName != null) 
                super.execNodeUseEnd(subKeyNodeFullName);

            // 返却地値をパースする
            if (retParam != null) {
                retParams = retParam.split(ImdstDefine.keyHelperClientParamSep);
            }
        }

        return retParams;
    }


    /**
     * KeyNodeに対してデータを保存する.<br>
     * 既に登録されている場合は失敗する.<br>
     * 
     * @param keyNodeName マスターデータノードの名前(IPなど)
     * @param keyNodePort マスターデータノードのアクセスポート番号
     * @param subKeyNodeName スレーブデータノードの名前(IPなど)
     * @param subKeyNodePort スレーブデータノードのアクセスポート番号
     * @param type 処理タイプ(1=Keyとデータノード設定, 3=Tagにキーを追加, 30=ロックを取得, 31=ロックを解除)
     * @param values 送信データ
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] setKeyNodeValueOnlyOnce(String keyNodeName, String keyNodePort, String keyNodeFullName, String subKeyNodeName, String subKeyNodePort, String subKeyNodeFullName, String type, String[] values, String transactionCode) throws BatchException {

        PrintWriter pw = null;
        BufferedReader br = null;
        HashMap dtMap = null;

        String nodeName = keyNodeName;
        String nodePort = keyNodePort;
        String nodeFullName = keyNodeFullName;

        String[] retParams = null;
        String[] mainRetParams = null;
        String[] subRetParams = null;

        int counter = 0;

        String tmpSaveHost = null;
        String[] tmpSaveData = null;
        String retParam = null;

        boolean mainNodeSave = false;
        boolean mainNodeNetworkError = false;
        boolean subNodeSave = false;
        boolean subNodeNetworkError = false;

        try {

            // TransactionModeの状態に合わせてLock状態を確かめる
            if (transactionMode) {
                while (true) {

                    // TransactionMode時
                    // TransactionManagerに処理を依頼
                    String[] keyNodeLockRet = hasLockKeyNode(transactionManagerInfo[0], transactionManagerInfo[1], values[0]);

                    // 取得結果確認
                    if (keyNodeLockRet[1].equals("true")) {

                        if (keyNodeLockRet[2].equals(transactionCode)) break;
                    } else {
                        break;
                    }
                }
            }

            // まずメインデータノードへデータ登録

            // KeyNodeとの接続を確立
            dtMap = this.createKeyNodeConnection(keyNodeName, keyNodePort, keyNodeFullName, false);

            if (dtMap != null) {
                try {
                    // writerとreaderを取り出し
                    pw = (PrintWriter)dtMap.get("writer");
                    br = (BufferedReader)dtMap.get("reader");

                    // Key値でデータノード名を保存
                    StringBuffer buf = new StringBuffer();
                    // パラメータ作成 処理タイプ[セパレータ]キー値のハッシュ値文字列[セパレータ]データノード名
                    buf.append("6");
                    buf.append(ImdstDefine.keyHelperClientParamSep);
                    buf.append(this.stringCnv(values[0]));               // Key値
                    buf.append(ImdstDefine.keyHelperClientParamSep);
                    buf.append(transactionCode);                    // Transaction値
                    buf.append(ImdstDefine.keyHelperClientParamSep);
                    buf.append(values[1]);                          // Value値

                    // 送信
                    pw.println(buf.toString());
                    pw.flush();

                    // 返却値取得
                    retParam = br.readLine();

                    // splitは遅いので特定文字列で返却値が始まるかをチェックし始まる場合は登録成功
                    //retParams = retParam.split(ImdstDefine.keyHelperClientParamSep);
                    if (retParam.indexOf(ImdstDefine.keyNodeKeyNewRegistSuccessStr) == 0) {

                        mainNodeSave = true;
                        mainRetParams = retParam.split(ImdstDefine.keyHelperClientParamSep);
                    } else {

                        mainNodeSave = false;
                        mainRetParams = retParam.split(ImdstDefine.keyHelperClientParamSep);
                    }
                } catch (SocketException se) {
                    mainNodeNetworkError = true;
                    super.setDeadNode(keyNodeName + ":" + keyNodePort, 8, se);
                    logger.debug(se);
                } catch (IOException ie) {
                    mainNodeNetworkError = true;
                    super.setDeadNode(keyNodeName + ":" + keyNodePort, 9, ie);
                    logger.debug(ie);
                } catch (Exception ee) {
                    super.setDeadNode(keyNodeName + ":" + keyNodePort, 10, ee);
                    logger.debug(ee);
                }

            } else {
                mainNodeNetworkError = true;
            }


            if (subKeyNodeName != null) {
                // Subノードで実施
                if (mainNodeSave == true || (mainNodeSave == false && mainNodeNetworkError == true)) {
                    // Mainノードが処理成功もしくは、ネットワークエラーの場合はSubノードを処理を行う。
                    // KeyNodeとの接続を確立
                    dtMap = this.createKeyNodeConnection(subKeyNodeName, subKeyNodePort, subKeyNodeFullName, false);

                    if (dtMap != null) {
                        try {
                            // writerとreaderを取り出し
                            pw = (PrintWriter)dtMap.get("writer");
                            br = (BufferedReader)dtMap.get("reader");

                            // Key値でデータノード名を保存
                            StringBuffer buf = new StringBuffer();
                            // パラメータ作成 処理タイプ[セパレータ]キー値のハッシュ値文字列[セパレータ]データノード名
                            if (!mainNodeSave) {
                                buf.append("6");
                            } else {
                                buf.append("1");
                            }
                            buf.append(ImdstDefine.keyHelperClientParamSep);
                            buf.append(this.stringCnv(values[0]));               // Key値
                            buf.append(ImdstDefine.keyHelperClientParamSep);
                            buf.append(transactionCode);                    // Transaction値
                            buf.append(ImdstDefine.keyHelperClientParamSep);
                            buf.append(values[1]);                          // Value値

                            // 送信
                            pw.println(buf.toString());
                            pw.flush();

                            // 返却値取得
                            retParam = br.readLine();


                            // splitは遅いので特定文字列で返却値が始まるかをチェックし始まる場合は登録成功
                            //retParams = retParam.split(ImdstDefine.keyHelperClientParamSep);
                            if (retParam.indexOf(ImdstDefine.keyNodeKeyNewRegistSuccessStr) == 0) {

                                subNodeSave = true;
                                subRetParams = retParam.split(ImdstDefine.keyHelperClientParamSep);
                            } else {

                                subNodeSave = false;
                                subRetParams = retParam.split(ImdstDefine.keyHelperClientParamSep);
                            }
                        } catch (SocketException se) {
                            subNodeNetworkError = true;
                            super.setDeadNode(subKeyNodeName + ":" + subKeyNodePort, 11, se);
                            logger.debug(se);
                        } catch (IOException ie) {
                            subNodeNetworkError = true;
                            super.setDeadNode(subKeyNodeName + ":" + subKeyNodePort, 12, ie);
                            logger.debug(ie);
                        } catch (Exception ee) {
                            super.setDeadNode(subKeyNodeName + ":" + subKeyNodePort, 13, ee);
                            logger.debug(ee);
                        }

                    } else {
                        subNodeNetworkError = true;
                    }
                }
            }

            // Main、Sub両方ともネットワークでのエラーがであるか確認
            if (mainNodeNetworkError == true && subNodeNetworkError == true) {
                // ネットワークエラー
                throw new BatchException("Key Node IO Error: detail info for log file");
            }

            // ノードへの保存状況を確認
            if (mainNodeSave == false) {

                // MainNode保存失敗
                if (mainNodeNetworkError == false) {

                    // 既に書き込み済みでの失敗 
                    retParams = mainRetParams;
                }
            } else {

                // MainNode保存成功
                retParams = mainRetParams;
            }

    
            if (subKeyNodeName != null) {
                // スレーブノードが存在する場合のみ
                // MainNodeが既にデータ有りで失敗せずに、成功もしていない
                if (retParams == null) {

                    if (subNodeSave == false) {

                        // SubNode保存失敗
                        if (subNodeNetworkError == false) {

                            // 既に書き込み済みでの失敗 
                            retParams = subRetParams;
                        }
                    } else {

                        // SubNode保存成功
                        retParams = subRetParams;
                    }
                }
            }
        } catch (BatchException be) {

            throw be;
        } catch (Exception e) {

            throw new BatchException(e);
        } finally {
            // ノードの使用終了をマーク
            super.execNodeUseEnd(keyNodeFullName);

            if (subKeyNodeName != null) 
                super.execNodeUseEnd(subKeyNodeFullName);
        }

        return retParams;
    }


    /**
     * KeyNodeに対してデータを削除する.<br>
     * 
     * @param keyNodeName マスターデータノードの名前(IPなど)
     * @param keyNodePort マスターデータノードのアクセスポート番号
     * @param subKeyNodeName スレーブデータノードの名前(IPなど)
     * @param subKeyNodePort スレーブデータノードのアクセスポート番号
     * @param keyStr Keyデータ
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] removeKeyNodeValue(String keyNodeName, String keyNodePort, String keyNodeFullName, String subKeyNodeName, String subKeyNodePort, String subKeyNodeFullName, String key, String transactionCode) throws BatchException {

        PrintWriter pw = null;
        BufferedReader br = null;
        HashMap dtMap = null;

        String nodeName = keyNodeName;
        String nodePort = keyNodePort;
        String nodeFullName = keyNodeFullName;

        String[] retParams = null;

        int counter = 0;

        String tmpSaveHost = null;
        String[] tmpSaveData = null;
        String retParam = null;

        boolean mainNodeSave = false;
        boolean subNodeSave = false;
        try {
            // TransactionModeの状態に合わせてLock状態を確かめる
            if (transactionMode) {
                while (true) {
                    // TransactionMode時

                    // TransactionManagerに処理を依頼
                    String[] keyNodeLockRet = hasLockKeyNode(transactionManagerInfo[0], transactionManagerInfo[1], key);

                    // 取得結果確認
                    if (keyNodeLockRet[1].equals("true")) {
                        if (keyNodeLockRet[2].equals(transactionCode)) break;
                    } else {
                        break;
                    }
                }
            }

            do {
                // KeyNodeとの接続を確立
                dtMap = this.createKeyNodeConnection(nodeName, nodePort, nodeFullName, false);


                // 接続結果と、現在の保存先状況で処理を分岐
                if (dtMap != null) {
                    try {
                        // writerとreaderを取り出し
                        pw = (PrintWriter)dtMap.get("writer");
                        br = (BufferedReader)dtMap.get("reader");


                        // Key値でデータノード名を保存
                        StringBuffer buf = new StringBuffer();
                        // パラメータ作成 処理タイプ[セパレータ]キー値のハッシュ値文字列[セパレータ]データノード名
                        buf.append("5");
                        buf.append(ImdstDefine.keyHelperClientParamSep);
                        buf.append(this.stringCnv(key));
                        buf.append(ImdstDefine.keyHelperClientParamSep);
                        buf.append(transactionCode);

//long start1 = System.nanoTime();


                        // 送信
                        pw.println(buf.toString());
                        pw.flush();

                        // 返却値取得
                        retParam = br.readLine();
//long end1 = System.nanoTime();
//System.out.println("[" + (end1 - start1) + "]");


                        // splitは遅いので特定文字列で返却値が始まるかをチェックし始まる場合は登録成功
                        if (retParam != null && retParam.indexOf(ImdstDefine.keyNodeKeyRemoveSuccessStr) == 0) {
                            if (counter == 0) mainNodeSave = true;
                            if (counter == 1) subNodeSave = true;
                        } else if (retParam == null || retParam.indexOf(ImdstDefine.keyNodeKeyRemoveNotFoundStr) != 0){
                            // 論理的に削除失敗
                            super.setDeadNode(nodeName + ":" + nodePort, 14, null);
                            logger.error("removeKeyNodeValue Logical Error Node =["  + nodeName + ":" + nodePort + "] retParam=[" + retParam + "]");

                        }
                    } catch (SocketException se) {

                        super.setDeadNode(nodeName + ":" + nodePort, 15, se);
                        logger.debug(se);
                    } catch (IOException ie) {
                        super.setDeadNode(nodeName + ":" + nodePort, 16, ie);
                        logger.debug(ie);
                    } catch (Exception ee) {
                        super.setDeadNode(nodeName + ":" + nodePort, 17, ee);
                        logger.debug(ee);
                    }
                }

                // スレーブデータノードの名前を代入
                nodeName = subKeyNodeName;
                nodePort = subKeyNodePort;
                nodeFullName = subKeyNodeFullName;

                counter++;
                // スレーブデータノードが存在しない場合もしくは、既に2回保存を実施した場合は終了
            } while(nodeName != null && counter < 2);

        } catch (BatchException be) {

            throw be;
        } catch (Exception e) {

            throw new BatchException(e);
        } finally {
            // ノードの使用終了をマーク
            super.execNodeUseEnd(keyNodeFullName);

            if (subKeyNodeName != null) 
                super.execNodeUseEnd(subKeyNodeFullName);

            // 返却地値をパースする
            if (retParam != null) {
                retParams = retParam.split(ImdstDefine.keyHelperClientParamSep);
            }
        }

        return retParams;
    }


    /**
     * KeyNodeに対してデータLockを依頼する.<br>
     * 
     * 
     * @param transactionManagerName TransactonManagerの名前(IPなど)
     * @param transactionManagerPort TransactonManagerのアクセスポート番号
     * @param key 対象Key
     * @param transactionCode TransactionCode
     * @param lockingTime Lock維持時間(この時間を過ぎると自動的に解除される。0は無限)(秒)
     * @param lockingWaitTime Lock取得までの待ち時間(Lockを取得するのに待つ時間。0は無限)(秒)
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] lockKeyNodeValue(String transactionManagerName, String transactionManagerPort, String key, String transactionCode, String lockingTime, String lockingWaitTime) throws BatchException {

        PrintWriter pw = null;
        BufferedReader br = null;
        HashMap dtMap = null;

        String retParam = null;
        String[] retParams = null;

        try {
            if (!transactionMode) {
                retParams = new String[2];
                retParams[0] = "30";
                retParams[1] = "false";
            }


            // KeyNodeとの接続を確立
            dtMap = this.createTransactionManagerConnection(transactionManagerName, transactionManagerPort, transactionManagerName + ":" + transactionManagerPort);


            // 接続結果と、現在の保存先状況で処理を分岐
            if (dtMap != null) {
                try {
                    // writerとreaderを取り出し
                    pw = (PrintWriter)dtMap.get("writer");
                    br = (BufferedReader)dtMap.get("reader");

                    // Key値でLockを取得
                    StringBuffer buf = new StringBuffer();
                    buf.append("30");
                    buf.append(ImdstDefine.keyHelperClientParamSep);
                    buf.append(this.stringCnv(key));               // Key値
                    buf.append(ImdstDefine.keyHelperClientParamSep);
                    buf.append(transactionCode);                    // Transaction値
                    buf.append(ImdstDefine.keyHelperClientParamSep);
                    buf.append(lockingTime);                        // lockingTime値
                    buf.append(ImdstDefine.keyHelperClientParamSep);
                    buf.append(lockingWaitTime);                    // lockingWaitTime値

                    // 送信
                    pw.println(buf.toString());
                    pw.flush();

                    // 返却値取得
                    retParam = br.readLine();
                } catch (SocketException se) {

                    logger.error("TransactionManager - Error " + se);
                } catch (IOException ie) {

                    // Nodeの通信失敗を記録
                    logger.error("TransactionManager - Error " + ie);
                }
            }

            // Lockの成功を判定
            if (retParam != null) {

                // splitは遅いので特定文字列で返却値が始まるかをチェックし始まる場合はLock成功
                if (retParam.indexOf(ImdstDefine.keyNodeLockingSuccessStr) == 0) {

                    // 成功
                    retParams = new String[3];
                    retParams[0] = "30";
                    retParams[1] = "true";
                    retParams[2] = transactionCode;
                } else {

                    // 失敗
                    retParams = new String[2];
                    retParams[0] = "30";
                    retParams[1] = "false";
                }
            } else {

                // 失敗
                retParams = new String[2];
                retParams[0] = "30";
                retParams[1] = "false";
            }
        } catch (BatchException be) {

            throw be;
        } catch (Exception e) {

            throw new BatchException(e);
        } 

        return retParams;
    }


    /**
     * KeyNodeに対してデータLockを解除依頼する.<br>
     * 
     * @param transactionManagerName TransactonManagerの名前(IPなど)
     * @param transactionManagerPort TransactonManagerのアクセスポート番号
     * @param key 対象Key
     * @param transactionCode TransactionCode
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] releaseLockKeyNodeValue(String transactionManagerName, String transactionManagerPort, String key, String transactionCode) throws BatchException {

        PrintWriter pw = null;
        BufferedReader br = null;
        HashMap dtMap = null;

        String retParam = null;
        String[] retParams = null;

        try {
            if (!transactionMode) {
                retParams = new String[2];
                retParams[0] = "31";
                retParams[1] = "false";
            }


            // KeyNodeとの接続を確立
            dtMap = this.createTransactionManagerConnection(transactionManagerName, transactionManagerPort, transactionManagerName + ":" + transactionManagerPort);


            // 接続結果と、現在の保存先状況で処理を分岐
            if (dtMap != null) {
                try {
                    // writerとreaderを取り出し
                    pw = (PrintWriter)dtMap.get("writer");
                    br = (BufferedReader)dtMap.get("reader");

                    // Key値でLockを取得
                    StringBuffer buf = new StringBuffer();
                    buf.append("31");
                    buf.append(ImdstDefine.keyHelperClientParamSep);
                    buf.append(this.stringCnv(key));               // Key値
                    buf.append(ImdstDefine.keyHelperClientParamSep);
                    buf.append(transactionCode);              // Transaction値

                    // 送信
                    pw.println(buf.toString());
                    pw.flush();

                    // 返却値取得
                    retParam = br.readLine();
                } catch (SocketException se) {

                    logger.error("TransactionManager - Error " + se);
                } catch (IOException ie) {

                    // Nodeの通信失敗を記録
                    logger.error("TransactionManager - Error " + ie);
                }
            }

            // Lock解除の成功を判定
            if (retParam != null) {

                // splitは遅いので特定文字列で返却値が始まるかをチェックし始まる場合はLock成功
                if (retParam.indexOf(ImdstDefine.keyNodeReleaseSuccessStr) == 0) {

                    // 成功
                    retParams = new String[3];
                    retParams[0] = "31";
                    retParams[1] = "true";
                    retParams[2] = transactionCode;
                } else {

                    // 失敗
                    retParams = new String[2];
                    retParams[0] = "31";
                    retParams[1] = "false";
                }
            } else {

                // 失敗
                retParams = new String[2];
                retParams[0] = "31";
                retParams[1] = "false";
            }
        } catch (BatchException be) {

            throw be;
        } catch (Exception e) {

            throw new BatchException(e);
        } 

        return retParams;
    }


    /**
     * KeyNodeに対してデータLockを依頼する.<br>
     * 
     * 
     * @param transactionManagerName TransactonManagerの名前(IPなど)
     * @param transactionManagerPort TransactonManagerのアクセスポート番号
     * @param key 対象Key
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] hasLockKeyNode(String transactionManagerName, String transactionManagerPort, String key) throws BatchException {

        PrintWriter pw = null;
        BufferedReader br = null;
        HashMap dtMap = null;

        String retParam = null;
        String[] retParams = null;

        try {
            if (!transactionMode) {
                retParams = new String[2];
                retParams[0] = "32";
                retParams[1] = "false";
            }


            // KeyNodeとの接続を確立
            dtMap = this.createTransactionManagerConnection(transactionManagerName, transactionManagerPort, transactionManagerName + ":" + transactionManagerPort);


            // 接続結果と、現在の保存先状況で処理を分岐
            if (dtMap != null) {
                try {
                    // writerとreaderを取り出し
                    pw = (PrintWriter)dtMap.get("writer");
                    br = (BufferedReader)dtMap.get("reader");

                    // Key値でLockを取得
                    StringBuffer buf = new StringBuffer();
                    buf.append("32");
                    buf.append(ImdstDefine.keyHelperClientParamSep);
                    buf.append(this.stringCnv(key));               // Key値

                    // 送信
                    pw.println(buf.toString());
                    pw.flush();

                    // 返却値取得
                    retParam = br.readLine();
                } catch (SocketException se) {

                    logger.error("TransactionManager - Error " + se);
                } catch (IOException ie) {

                    // Nodeの通信失敗を記録
                    logger.error("TransactionManager - Error " + ie);
                }
            }

            // Lockの成功を判定
            if (retParam != null) {

                // splitは遅いので特定文字列で返却値が始まるかをチェックし始まる場合はLock成功
                if (retParam.indexOf(ImdstDefine.hasKeyNodeLockSuccessStr) == 0) {

                    // 成功
                    retParams = retParam.split(ImdstDefine.keyHelperClientParamSep);
                } else {

                    // 失敗
                    retParams = new String[2];
                    retParams[0] = "32";
                    retParams[1] = "false";
                }
            } else {

                // 失敗
                retParams = new String[2];
                retParams[0] = "32";
                retParams[1] = "false";
            }
        } catch (BatchException be) {

            throw be;
        } catch (Exception e) {

            throw new BatchException(e);
        } 

        return retParams;
    }


    /**
     * KeyNodeの一時使用停止をマークする.<br>
     * 
     * @param keyNodeFullName データノードのフルネーム(IP,Port)
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] pauseKeyNodeUse(String keyNodeFullName) throws BatchException {

        String[] retParams = new String[3];

        try {

            // ノードの使用中断を要求
            super.setNodeWaitStatus(keyNodeFullName);
            while(true) {
                // 使用停止まで待機
                if(super.getNodeUseStatus(keyNodeFullName) == 0) break;
                Thread.sleep(5);
            }

            // 停止成功
            retParams[0] = "90";
            retParams[1] = "true";
            retParams[2] = "";

        } catch (Exception e) {

            throw new BatchException(e);
        } 

        return retParams;
    }


    /**
     * KeyNodeの一時使用停止解除をマークする.<br>
     * 
     * @param keyNodeFullName データノードのフルネーム(IP,Port)
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] restartKeyNodeUse(String keyNodeFullName) throws BatchException {

        String[] retParams = new String[3];

        try {

            // ノードの一時使用停止解除を要求
            super.removeNodeWaitStatus(keyNodeFullName);

            // 再開成功
            retParams[0] = "91";
            retParams[1] = "true";
            retParams[2] = "";

        } catch (Exception e) {

            throw new BatchException(e);
        } 

        return retParams;
    }


    /**
     * KeyNodeの復旧をマークする.<br>
     * 
     * @param keyNodeFullName データノードのフルネーム(IP,Port)
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] arriveKeyNode(String keyNodeFullName) throws BatchException {

        String[] retParams = new String[3];

        try {

            // ノードの復旧を要求
            super.setArriveNode(keyNodeFullName);

            // 再開成功
            retParams[0] = "92";
            retParams[1] = "true";
            retParams[2] = "";

        } catch (Exception e) {

            throw new BatchException(e);
        } 

        return retParams;
    }


    /**
     * KeyNodeに対して並列処理でデータを保存を行う.<br>
     * 
     * @param keyNodeName マスターデータノードの名前(IPなど)
     * @param keyNodePort マスターデータノードのアクセスポート番号
     * @param subKeyNodeName スレーブデータノードの名前(IPなど)
     * @param subKeyNodePort スレーブデータノードのアクセスポート番号
     * @param type 処理タイプ(1=Keyとデータノード設定, 3=Tagにキーを追加, 30=ロックを取得, 31=ロックを解除)
     * @param values 送信データ
     * @return String[] 結果
     * @throws BatchException
     */
    private String[] setKeyNodeValueMultiSend(String keyNodeName, String keyNodePort, String keyNodeFullName, String subKeyNodeName, String subKeyNodePort, String subKeyNodeFullName, String type, String[] values, String transactionCode) throws BatchException {
        PrintWriter pw = null;
        BufferedReader br = null;
        HashMap dtMap = null;

        String nodeName = keyNodeName;
        String nodePort = keyNodePort;
        String nodeFullName = keyNodeFullName;

        String[] retParams = null;

        int counter = 0;

        String tmpSaveHost = null;
        String[] tmpSaveData = null;
        String retParam = null;

        DataNodeSender mainNodeSender = null;
        DataNodeSender subNodeSender = null;

        boolean mainNodeSave = false;
        boolean subNodeSave = false;

        String successRet = null;
        String errorRet = null;

        try {
            // TransactionModeの状態に合わせてLock状態を確かめる
            if (transactionMode) {
                while (true) {
                    // TransactionMode時

                    // TransactionManagerに処理を依頼
                    String[] keyNodeLockRet = hasLockKeyNode(transactionManagerInfo[0], transactionManagerInfo[1], values[0]);

                    // 取得結果確認
                    if (keyNodeLockRet[1].equals("true")) {
                        if (keyNodeLockRet[2].equals(transactionCode)) break;
                    } else {
                        break;
                    }
                }
            }

            do {
                // KeyNodeとの接続を確立
                dtMap = this.createKeyNodeConnection(nodeName, nodePort, nodeFullName, false);


                // 接続結果と、現在の保存先状況で処理を分岐
                if (dtMap != null) {

                    // writerとreaderを取り出し
                    pw = (PrintWriter)dtMap.get("writer");
                    br = (BufferedReader)dtMap.get("reader");

//long start1 = System.nanoTime();
                    // 処理種別判別
                    if (type.equals("1") || type.equals("3")) {
                        // Key値でデータを保存 or TagでKey値を保存
                        if (counter == 0) {
                            // Mainノード
                            mainNodeSender = new DataNodeSender();
                            mainNodeSender.setSendInfo(type,values[0], values[1], transactionCode, pw, br);
                            mainNodeSender.start();
                        } else {
                            // Subノード
                            subNodeSender = new DataNodeSender();
                            subNodeSender.setSendInfo(type,values[0], values[1], transactionCode, pw, br);
                            subNodeSender.start();
                        }
                    }
//long end1 = System.nanoTime();
//System.out.println("[" + (end1 - start1) + "]");
                }

                // スレーブデータノードの名前を代入
                nodeName = subKeyNodeName;
                nodePort = subKeyNodePort;
                nodeFullName = subKeyNodeFullName;

                counter++;
                // スレーブデータノードが存在しない場合もしくは、既に2回保存を実施した場合は終了
            } while(nodeName != null && counter < 2);

            // Mainノード保存スレッドを確認
            if (mainNodeSender != null) {
                mainNodeSender.join();
                if (mainNodeSender.isError()) {
                    if (mainNodeSender.getErrorType() == -1) {

                        errorRet = mainNodeSender.getRetParam();
                    } else if (mainNodeSender.getErrorType() != -1) {

                        super.setDeadNode(keyNodeFullName, 18, null);
                        logger.debug(mainNodeSender.getErrorMsg());
                    }
                    mainNodeSave = false;
                } else {

                    successRet = mainNodeSender.getRetParam();
                    mainNodeSave = true;
                }
                mainNodeSender = null;
            }

            // Subノード保存スレッドを確認
            if (subNodeSender != null) {
                subNodeSender.join();
                if (subNodeSender.isError()) {
                    if (subNodeSender.getErrorType() == -1) {

                        errorRet = subNodeSender.getRetParam();
                    } else if (subNodeSender.getErrorType() != -1) {

                        super.setDeadNode(subKeyNodeFullName, 19, null);
                        logger.debug(subNodeSender.getErrorMsg());
                    }
                    subNodeSave = false;
                } else {

                    successRet = subNodeSender.getRetParam();
                    subNodeSave = true;
                }
                subNodeSender = null;
            }

            // ノードへの保存状況を確認
            if (mainNodeSave == false && subNodeSave == false) {
                // 返却地値をパースする
                if (errorRet != null) {
                    retParams = errorRet.split(ImdstDefine.keyHelperClientParamSep);
                }
                throw new BatchException("Key Node IO Error: detail info for log file");
            } else {
                // 返却地値をパースする
                if (successRet != null) {
                    retParams = successRet.split(ImdstDefine.keyHelperClientParamSep);
                }
            }
        } catch (BatchException be) {
            throw be;
        } catch (Exception e) {
            throw new BatchException(e);
        } finally {
            // ノードの使用終了をマーク
            super.execNodeUseEnd(keyNodeFullName);

            if (subKeyNodeName != null) 
                super.execNodeUseEnd(subKeyNodeFullName);
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
    private HashMap createKeyNodeConnection(String keyNodeName, String keyNodePort, String keyNodeFullName, boolean retryFlg) throws BatchException {
        PrintWriter pw = null;
        BufferedReader br = null;
        HashMap dtMap = null;

        String connectionFullName = keyNodeFullName;
        Long connectTime = new Long(0);

        try {

            if (!super.isNodeArrival(connectionFullName)) {
                System.out.println("EEEEEEEEEEEEEEEEEEEEEEEEEEEE_[" + connectionFullName + "]");
                return null;
            }

            // フラグがtrueの場合はキャッシュしている接続を破棄してやり直す
            if (retryFlg) {
                if (this.keyNodeConnectMap.containsKey(connectionFullName)) this.keyNodeConnectMap.remove(connectionFullName);
            }

            // 既にKeyNodeに対するコネクションが確立出来ている場合は使いまわす
            if (this.keyNodeConnectMap.containsKey(connectionFullName) && 
                super.checkConnectionEffective(connectionFullName, (Long)this.keyNodeConnectTimeMap.get(connectionFullName))) {
                dtMap = (HashMap)this.keyNodeConnectMap.get(connectionFullName);
            } else {
                // 新規接続
                // 親クラスから既に接続済みの接続をもらう
                HashMap connectMap = super.getActiveConnection(connectionFullName);


                // 接続が存在しない場合は自身で接続処理を行う
                if (connectMap == null) {

                    InetSocketAddress inetAddr = new InetSocketAddress(keyNodeName, Integer.parseInt(keyNodePort));
                    Socket socket = new Socket();
                    socket.connect(inetAddr, ImdstDefine.nodeConnectionOpenTimeout);
                    socket.setSoTimeout(ImdstDefine.nodeConnectionTimeout);

                    OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding);
                    pw = new PrintWriter(new BufferedWriter(osw));

                    InputStreamReader isr = new InputStreamReader(socket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding);
                    br = new BufferedReader(isr);

                    dtMap = new HashMap();

                    // Socket, Writer, Readerをキャッシュ
                    dtMap.put(ImdstDefine.keyNodeSocketKey, socket);
                    dtMap.put(ImdstDefine.keyNodeStreamWriterKey, osw);
                    dtMap.put(ImdstDefine.keyNodeStreamReaderKey, isr);

                    dtMap.put(ImdstDefine.keyNodeWriterKey, pw);
                    dtMap.put(ImdstDefine.keyNodeReaderKey, br);
                    connectTime = new Long(System.currentTimeMillis());
                } else {

                    dtMap = (HashMap)connectMap.get(ImdstDefine.keyNodeConnectionMapKey);
                    connectTime = (Long)connectMap.get(ImdstDefine.keyNodeConnectionMapTime);
                }

                this.keyNodeConnectMap.put(connectionFullName, dtMap);
                this.keyNodeConnectTimeMap.put(connectionFullName, connectTime);
            }
        } catch (Exception e) {
            logger.error(connectionFullName + " " + e);
            dtMap = null;

            // 一度接続不慮が発生した場合はこのSESSIONでは接続しない設定とする
            super.setDeadNode(connectionFullName, 20, e);
        }

        return dtMap;
    }


    /**
     *
     *
     */
    private HashMap createTransactionManagerConnection(String keyNodeName, String keyNodePort, String keyNodeFullName) throws BatchException {
        PrintWriter pw = null;
        BufferedReader br = null;
        HashMap dtMap = null;

        String connectionFullName = keyNodeFullName;
        Long connectTime = new Long(0);

        try {

            // 既にKeyNodeに対するコネクションが確立出来ている場合は使いまわす
            if (this.keyNodeConnectMap.containsKey(connectionFullName) && 
                super.checkConnectionEffective(connectionFullName, (Long)this.keyNodeConnectTimeMap.get(connectionFullName))) {

                dtMap = (HashMap)this.keyNodeConnectMap.get(connectionFullName);
            } else {

                // 新規接続
                // 親クラスから既に接続済みの接続をもらう
                HashMap connectMap = super.getActiveConnection(connectionFullName);


                // 接続が存在しない場合は自身で接続処理を行う
                if (connectMap == null) {

                    Socket socket = new Socket(keyNodeName, Integer.parseInt(keyNodePort));
                    socket.setSoTimeout(ImdstDefine.nodeConnectionTimeout);

                    OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding);
                    pw = new PrintWriter(new BufferedWriter(osw));

                    InputStreamReader isr = new InputStreamReader(socket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding);
                    br = new BufferedReader(isr);

                    dtMap = new HashMap();

                    // Socket, Writer, Readerをキャッシュ
                    dtMap.put(ImdstDefine.keyNodeSocketKey, socket);
                    dtMap.put(ImdstDefine.keyNodeStreamWriterKey, osw);
                    dtMap.put(ImdstDefine.keyNodeStreamReaderKey, isr);

                    dtMap.put(ImdstDefine.keyNodeWriterKey, pw);
                    dtMap.put(ImdstDefine.keyNodeReaderKey, br);
                    connectTime = new Long(System.currentTimeMillis());
                } else {
                    dtMap = (HashMap)connectMap.get(ImdstDefine.keyNodeConnectionMapKey);
                    connectTime = (Long)connectMap.get(ImdstDefine.keyNodeConnectionMapTime);
                }

                this.keyNodeConnectMap.put(connectionFullName, dtMap);
                this.keyNodeConnectTimeMap.put(connectionFullName, connectTime);
            }
        } catch (Exception e) {
            logger.error(connectionFullName + " " + e);
            dtMap = null;
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

                        /*HashMap connMap = new HashMap();
                        connMap.put(ImdstDefine.keyNodeConnectionMapKey, cacheConn);
                        connMap.put(ImdstDefine.keyNodeConnectionMapTime, (Long)this.keyNodeConnectTimeMap.get(connKeyStr));*/

                        // キャッシュ層に登録
                        //super.setActiveConnection(connKeyStr, connMap);
                        
                        PrintWriter cachePw = (PrintWriter)cacheConn.get(ImdstDefine.keyNodeWriterKey);
                        if (cachePw != null) {
                            // 切断要求を送る
                            cachePw.println(ImdstDefine.imdstConnectExitRequest);
                            cachePw.flush();
                            cachePw.close();
                        }

                        BufferedReader cacheBr = (BufferedReader)cacheConn.get(ImdstDefine.keyNodeReaderKey);
                        if (cacheBr != null) {
                            cacheBr.close();
                        }

                        OutputStreamWriter cacheOsw = (OutputStreamWriter)cacheConn.get(ImdstDefine.keyNodeStreamWriterKey);
                        if (cacheOsw != null) {
                            cacheOsw.close();
                        }

                        InputStreamReader cacheIsr = (InputStreamReader)cacheConn.get(ImdstDefine.keyNodeStreamReaderKey);
                        if (cacheIsr != null) {
                            cacheIsr.close();
                        }

                        Socket cacheSoc = (Socket)cacheConn.get(ImdstDefine.keyNodeSocketKey);
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


    private String stringCnv(String str) {
        return str;
        //return str.hashCode();
    }

    private boolean checkKeyLength(String key) {
        if (key == null) return false;
        if (key.length() >= ImdstDefine.saveKeyMaxSize) return false;
        return true;
    }

    /**
     * データをノードへ反映する内部スレッド.<br>
     *
     */
    private class DataNodeSender extends Thread {

        private boolean isError = false;

        private String ret = null;

        // 1:SocketException 2:IOException 3:それ以外 -1:論理エラー
        private int errorType = 0;

        private String errorMsg = null;


        private String type = null;

        private String key = null;

        private String value = null;

        private String transactionCode = null;

        private PrintWriter pw = null;

        private BufferedReader br = null;

        public void run() {
            try {
                StringBuffer buf = new StringBuffer();
                buf.append(type);
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append(stringCnv(key));
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append(transactionCode);
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append(value);

                // 送信
                pw.println(buf.toString());
                pw.flush();

                // 返却値取得
                ret = br.readLine();

                if (type.equals("1")) {
                    if (ret.indexOf(ImdstDefine.keyNodeKeyRegistSuccessStr) != 0) {
                        isError = true;
                        errorType = -1;
                    }
                }

                if (type.equals("3")) {
                    if (ret.indexOf(ImdstDefine.keyNodeTagRegistSuccessStr) != 0) {
                        isError = true;
                        errorType = -1;
                    }
                }

            } catch (SocketException se) {

                errorType = 1;
                errorMsg = se.getMessage();
                isError = true;
            } catch (IOException ie) {

                errorType = 2;
                errorMsg = ie.getMessage();
                isError = true;
            } catch (Exception e) {

                errorType = 3;
                errorMsg = e.getMessage();
                isError = true;
            }
        }

        public void setSendInfo(String type, String key, String value, String transactionCode, PrintWriter pw, BufferedReader br) {
            this.type = type;
            this.key = key;
            this.value = value;
            this.transactionCode = transactionCode;
            this.pw = pw;
            this.br = br;
        }

        public String getRetParam() {
            return this.ret;
        }

        public boolean isError() {
            return this.isError;
        }

        public int getErrorType() {
            return this.errorType;
        }

        public String getErrorMsg() {
            return this.errorMsg;
        }
    }
}