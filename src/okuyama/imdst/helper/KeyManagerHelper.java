package okuyama.imdst.helper;

import java.io.*;
import java.util.*;
import java.net.*;
import javax.script.*;
import java.util.concurrent.atomic.AtomicInteger;

import okuyama.base.lang.BatchException;
import okuyama.base.job.AbstractHelper;
import okuyama.base.job.IJob;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.imdst.util.KeyMapManager;
import okuyama.imdst.util.ImdstDefine;
import okuyama.imdst.util.StatusUtil;
import okuyama.imdst.util.SystemUtil;
import okuyama.imdst.util.protocol.*;
import okuyama.imdst.util.JavaSystemApi;

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
 * クライアントへの返却値は1行文字列で返される.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyManagerHelper extends AbstractHelper {

    // KeyMapManagerインスタンス
    private KeyMapManager keyMapManager = null;

    private String queuePrefix = null;


    // 自身と論理的に同じQueueに紐付いているHelperの現在待機カウンター
    private AtomicInteger numberOfQueueBindWaitCounter = null;

    private static final int returnProccessingCount = 2;


    // プロトコルモード
    private String protocolMode = null;
    private IProtocolTaker porotocolTaker = null;


    // 保存可能なデータの最大サイズ
    private static int setDatanodeMaxSize = new Double(ImdstDefine.saveDataMaxSize * 1.38).intValue();

    // Scriptエンジン
    private ScriptEngineManager manager = null;
    private ScriptEngine engine = null;


    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(KeyManagerHelper.class);

    // 初期化メソッド定義
    public void initHelper(String initValue) {
        this.protocolMode = initValue;
    }


    // Jobメイン処理定義
    public String executeHelper(String optionParam) throws BatchException {
        //logger.debug("KeyManagerHelper - executeHelper - start");

        String ret = null;

        boolean closeFlg = false;
        boolean serverRunning = true;

        String bindQueueWaitHelperCountKey = "";
        boolean reloopSameClient = false;


        Socket soc = null;
        PrintWriter pw = null;
        BufferedReader br = null;

        String[] retParams = null;
        StringBuilder retParamBuf = new StringBuilder(ImdstDefine.stringBufferSmallSize);


        try{

            Object[] parameters = super.getParameters();

            String clientParametersStr = null;
            String[] clientParameterList = null;

            String requestHashCode = null;
            String requestDataNode = null;

            String requestTag = null;
            String requestKey = null;

            String transactionCode = null;

            String removeTag = null;
            String removeTagForKey = null;


            String accessQueueName = null;

            String updateVersionNo = null;

            String execCheckStr = null;

            String clientInfo = null;

            // Jobからの引数
            this.keyMapManager = (KeyMapManager)parameters[0];
            String pollQueueName = (String)parameters[1];
            String[] addQueueNames = (String[])parameters[2];


            // Helperの全体数と現在処理中の数を知るためのKey値
            bindQueueWaitHelperCountKey = (String)parameters[3];

            // 全体処理数を取得
            numberOfQueueBindWaitCounter = (AtomicInteger)super.getHelperShareParam(bindQueueWaitHelperCountKey);

            // プロトコル決定
            if (this.protocolMode != null && !this.protocolMode.trim().equals("") && !this.protocolMode.equals("okuyama")) {
                this.porotocolTaker = ProtocolTakerFactory.getProtocolTaker(this.protocolMode + "_datanode");
            }


            Object[] queueParam = null;
            Object[] queueMap = null;

            while(serverRunning) {
                try {
                    // 切断確認
                    if (closeFlg) 
                        this.closeClientConnect(pw, br, soc);


                    // 結果文字列バッファ初期化
                    retParamBuf.delete(0, Integer.MAX_VALUE);

                    // 結果クリア
                    retParams = null;

                    // 既にクローズしている場合は、もしくは同一クライアント処理がtrueの場合意外はキュー待ち処理
                    if (closeFlg == true || reloopSameClient == false) {

                        // キューを待ち受ける
                        queueParam = super.pollSpecificationParameterQueue(pollQueueName);

                        queueMap = (Object[])queueParam[0];

                        pw = (PrintWriter)queueMap[ImdstDefine.paramPw];
                        br = (BufferedReader)queueMap[ImdstDefine.paramBr];
                        soc = (Socket)queueMap[ImdstDefine.paramSocket];
                        soc.setSoTimeout(0);
                        clientInfo = soc.toString();
                        closeFlg = false;
                    }

                    // 処理中のため待機カウンターを減算
                    if (!reloopSameClient) 
                        numberOfQueueBindWaitCounter.getAndDecrement();

                    // 同一クライアント処理フラグ初期化
                    reloopSameClient = false;



                    // プロトコルに合わせて処理を分岐
                    if (this.porotocolTaker != null) {

                        this.porotocolTaker = ProtocolTakerFactory.getProtocolTaker(this.protocolMode + "_datanode");
                        this.porotocolTaker.setClientInfo(clientInfo);

                        // Takerで会話開始
                        clientParametersStr = this.porotocolTaker.takeRequestLine(br, pw);

                        if (this.porotocolTaker.nextExecution() != 1) {

                            // 処理をやり直し
                            if (this.porotocolTaker.nextExecution() == 2) { 
                                // 処理が完了したらキューに戻す
                                queueMap[ImdstDefine.paramLast] = new Long(JavaSystemApi.currentTimeMillis);
                                queueParam[0] = queueMap;
                                super.addSmallSizeParameterQueue(addQueueNames, queueParam);
                                continue;
                            }

                            // クライアントからの要求が接続切要求ではないか確認
                            if (this.porotocolTaker.nextExecution() == 3) {

                                clientParametersStr = ImdstDefine.imdstConnectExitRequest;
                            }
                        }
                    } else {

                        // okuyamaプロトコル
                        clientParametersStr = br.readLine();

                        // Debugログ書き出し
                        if (StatusUtil.getDebugOption()) 
                            SystemUtil.debugLine(clientInfo + " : Request  : " + clientParametersStr);
                    }


                    // クライアントからの要求が接続切要求ではないか確認
                    if (clientParametersStr == null || 
                            clientParametersStr.equals("") || 
                                clientParametersStr.equals(ImdstDefine.imdstConnectExitRequest)) {

                        // 切断要求
                        //logger.debug("Client Connect Exit Request");
                        closeFlg = true;
                        continue;
                    }


                    // クライアントからのパラメータ分解
                    clientParameterList = clientParametersStr.split(ImdstDefine.keyHelperClientParamSep);


                    // 処理番号を取り出し
                    if(clientParameterList[0] == null ||  clientParameterList[0].equals("")) clientParameterList[0] = "-1";
                    switch (Integer.parseInt(clientParameterList[0])) {

                        case 1 :

                            // Key値とDataNode名を格納する
                            requestHashCode = clientParameterList[1];
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
                            break;
                        case -1 :

                            // 応答を返さない
                            // Key値とDataNode名を格納する
                            requestHashCode = clientParameterList[1];
                            transactionCode = clientParameterList[2];
                            requestDataNode = clientParameterList[3];

                            // 値の中にセパレータ文字列が入っている場合もデータとしてあつかう
                            if (clientParameterList.length > 4) {
                                requestDataNode = requestDataNode + 
                                    ImdstDefine.keyHelperClientParamSep + 
                                        clientParameterList[4];
                            }

                            // メソッド呼び出し
                            this.setDatanode(requestHashCode, requestDataNode, transactionCode);
                            break;
                        case 2 :

                            // Key値でDataNode名を返す
                            requestHashCode = clientParameterList[1];

                            // メソッド呼び出し
                            retParams = this.getDatanode(requestHashCode);
                            retParamBuf.append(retParams[0]);
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(retParams[1]);
                            if (retParams.length > 2) {
                                retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                                retParamBuf.append(retParams[2]);
                            }
                            break;
                        case 3 :

                            // Tag値とキー値を格納する
                            requestTag = clientParameterList[1];
                            transactionCode = clientParameterList[2];         // TransactionCode
                            requestKey = clientParameterList[3];

                            // メソッド呼び出し
                            retParams = this.setTagdata(requestTag, requestKey, transactionCode);
                            retParamBuf.append(retParams[0]);
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(retParams[1]);
                            break;
                        case -3 :
                            // 応答を返さない
                            // Tag値とキー値を格納する
                            requestTag = clientParameterList[1];
                            transactionCode = clientParameterList[2];         // TransactionCode
                            requestKey = clientParameterList[3];

                            // メソッド呼び出し
                            this.setTagdata(requestTag, requestKey, transactionCode);
                            break;
                        case 4 :

                            // Tag値でKey値を返す
                            requestHashCode = clientParameterList[1];
                            // メソッド呼び出し
                            retParams = this.getTagdata(requestHashCode);
                            retParamBuf.append(retParams[0]);
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(retParams[1]);
                            if (retParams.length > 2) {
                                retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                                retParamBuf.append(retParams[2]);
                            }
                            break;
                        case 5 :

                            // Key値を指定する事でデータを削除する
                            requestHashCode = clientParameterList[1];
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
                            break;
                        case 6 :

                            // Key値とDataNode名を格納する
                            // 既に登録されている場合は失敗する
                            requestHashCode = clientParameterList[1];
                            transactionCode = clientParameterList[2];
                            requestDataNode = clientParameterList[3];

                            // 値の中にセパレータ文字列が入っている場合もデータとしてあつかう
                            if (clientParameterList.length > 4) {
                                requestDataNode = requestDataNode + 
                                    ImdstDefine.keyHelperClientParamSep + 
                                        clientParameterList[4];
                            }

                            // メソッド呼び出し
                            retParams = this.setDatanodeOnlyOnce(requestHashCode, requestDataNode, transactionCode);
                            retParamBuf.append(retParams[0]);
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(retParams[1]);
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(retParams[2]);
                            break;
                        case 8 :

                            // Key値でDataNode名を返す(Script実行バージョン)
                            requestHashCode = clientParameterList[1];
                            // メソッド呼び出し
                            retParams = this.getDatanodeScriptExec(requestHashCode,clientParameterList[2]);
                            retParamBuf.append(retParams[0]);
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(retParams[1]);
                            if (retParams.length > 2) {
                                retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                                retParamBuf.append(retParams[2]);
                            }
                            break;
                        case 10 :

                            // ServerConnect Test Ping
                            retParamBuf.append("10");
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append("true");
                            // エラーの場合は以下でエラーメッセメッセージも連結
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(StatusUtil.getNowMemoryStatus());
                            retParamBuf.append(";");
                            retParamBuf.append("Save Data Count=[" + keyMapManager.getSaveDataCount() + "]");
                            retParamBuf.append(";");
                            retParamBuf.append("Last Data Change Time=[" + keyMapManager.getLastDataChangeTime() + "]");
                            retParamBuf.append(";");
                            retParamBuf.append("Save Data Size=[");

                             String[] allSaveSize = this.keyMapManager.getAllSaveDataSize();

                            StringBuilder sizeListStr = new StringBuilder(40);
                            if (allSaveSize != null) {
                                String sep = "";
                                for (int i = 0; i < allSaveSize.length; i++) {
                                    if (allSaveSize[i] != null && !allSaveSize[i].equals("")) {
                                        sizeListStr.append(sep);
                                        sizeListStr.append(allSaveSize[i]);
                                        sep = ":";
                                    }
                                }
                            }
                            retParamBuf.append(sizeListStr.toString());
                            retParamBuf.append("]");
                            break;
                        case 11 :

                            // 最終データ更新時間を返す
                            retParamBuf.append("11");
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append("true");
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(keyMapManager.getLastDataChangeTime());
                            break;

                        case 13 :

                            // Key値とDataNode名を格納する
                            requestHashCode = clientParameterList[1];
                            transactionCode = clientParameterList[2];
                            requestDataNode = clientParameterList[3];

                            // メソッド呼び出し
                            retParams = this.calcValue(requestHashCode, requestDataNode, transactionCode);
                            retParamBuf.append(retParams[0]);
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(retParams[1]);
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(retParams[2]);

                            break;
                        case 16 :

                            // Key値とDataNode名を格納する
                            // バージョン番号が異なる場合は失敗する
                            requestHashCode = clientParameterList[1];
                            transactionCode = clientParameterList[2];
                            updateVersionNo = clientParameterList[3];
                            execCheckStr    = clientParameterList[4];
                            requestDataNode = clientParameterList[5];

                            // 値の中にセパレータ文字列が入っている場合もデータとしてあつかう
                            if (clientParameterList.length > 6) {
                                requestDataNode = requestDataNode + 
                                    ImdstDefine.keyHelperClientParamSep + 
                                        clientParameterList[6];
                            }

                            // メソッド呼び出し
                            retParams = this.setDatanodeVersionCheck(requestHashCode, requestDataNode, transactionCode, updateVersionNo, new Boolean(execCheckStr).booleanValue());
                            retParamBuf.append(retParams[0]);
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(retParams[1]);
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(retParams[2]);
                            break;

                        case 20 :

                            // KeyMapManager Direct Connection
                            // KeyMapObjectを読み込んで渡す
                            this.keyMapManager.outputKeyMapObj2Stream(pw);
                            pw.flush();

                            //retParamBuf = null;
                            break;
                        case 21 :

                            // KeyMapManager Direct Connection
                            // KeyMapObjectを読み込んで書き出す
                            this.keyMapManager.inputKeyMapObj2Stream(br, pw, Integer.parseInt(clientParameterList[1]));

                            //retParamBuf = null;
                            break;
                        case 22 :

                            // KeyManagerの差分取得モードをONにする
                            // !! MasterManagerでDataNodeの一時停止状態になってから呼び出される前提 !!
                            this.keyMapManager.diffDataMode(true, pw);
                            //retParamBuf = null;
                            break;
                        case 23 :

                            // KeyManagerの差分取得モードをOFFにする
                            // !! MasterManagerでDataNodeの一時停止状態になってから呼び出される前提 !!
                            this.keyMapManager.diffDataModeOff();
                            //retParamBuf = null;
                            break;
                        case 24 :

                            // KeyManagerの差分データを読み込んで渡す
                            // !! MasterManagerでDataNodeの一時停止状態になってから呼び出される前提 !!
                            this.keyMapManager.outputDiffKeyMapObj2Stream(pw, br);
                            pw.flush();
                            //retParamBuf = null;
                            break;
                        case 25 :

                            // KeyMapManagerに差分データを登録する
                            // !! MasterManagerでDataNodeの一時停止状態になってから呼び出される前提 !!
                            this.keyMapManager.inputDiffKeyMapObj2Stream(br, pw);
                            //retParamBuf = null;
                            break;
                        case 26 :

                            // KeyMapManager Direct Connection
                            // KeyMapObjectから自身が管理するべてきではないデータのKey値を返す
                            this.keyMapManager.outputNoMatchKeyMapKey2Stream(pw, Integer.parseInt(clientParameterList[2]), clientParameterList[3]);
                            pw.flush();
                            //retParamBuf = null;
                            break;
                        case 27 :

                            // KeyMapManager Direct Connection
                            // ConsistentHash時のデータ移動(抽出)
                            this.keyMapManager.outputConsistentHashMoveData2Stream(pw, clientParameterList[2]);
                            pw.flush();
                            //retParamBuf = null;
                            break;
                        case 28 :

                            // KeyMapManager Direct Connection
                            // ConsistentHash時のデータ移動(登録)
                            this.keyMapManager.inputConsistentHashMoveData2Stream(pw, br);
                            pw.flush();
                            //retParamBuf = null;
                            break;
                        case 29 :

                            // KeyMapManager Direct Connection
                            // ConsistentHash時のデータ移動(削除)
                            this.keyMapManager.removeConsistentHashMoveData2Stream(pw, clientParameterList[2]);
                            pw.flush();
                            //retParamBuf = null;
                            break;
                        case 30 :

                            // KeyMapManager Direct Connection
                            // Mod時のデータ移動(登録)
                            this.keyMapManager.inputNoMatchKeyMapKey2Stream(pw, br);
                            pw.flush();
                            //retParamBuf = null;
                            break;

                        case 31 :

                            // KeyMapManager Direct Connection
                            // Mod時のデータ移動(登録)
                            this.keyMapManager.removeModMoveData2Stream(pw, br);
                            pw.flush();
                            //retParamBuf = null;
                            break;
                        case 40 :

                            // Key値に紐づいている指定のTagを消す
                            removeTag = clientParameterList[1];
                            removeTagForKey = clientParameterList[2];
                            transactionCode = clientParameterList[3];

                            // メソッド呼び出し
                            retParams = this.removeTargetTagInKey(removeTag, removeTagForKey, transactionCode);
                            retParamBuf.append(retParams[0]);
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(retParams[1]);
                            break;
                        case 41 :

                            // Tga値を指定する事でTagデータを削除する
                            removeTag = clientParameterList[1];
                            transactionCode = clientParameterList[2];

                            // メソッド呼び出し
                            retParams = this.removeTagdata(removeTag, transactionCode);
                            retParamBuf.append(retParams[0]);
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(retParams[1]);
                            if (retParams.length > 2) {
                                retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                                retParamBuf.append(retParams[2]);
                            }
                            break;
                        case 60 :

                            // KeyMapManagerのデータサイズを返す
                            String unique = null;
                            if (clientParameterList.length > 1 && !clientParameterList[1].trim().equals("")) {
                                unique = clientParameterList[1];
                            }

                            long saveSize = this.keyMapManager.getSaveDataSize(unique);

                            retParamBuf.append("60");
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append("true");
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(unique);
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(saveSize);
                            break;
                        case 61 :

                            // 特定のIsolation単位のデータを全て削除する
                            // Isolationキーを先頭"#"付で指定する.6文字固定となる
                            // もしくは、"#all"を指定する.これは全てのデータを削除する
                            String truncateKey = null;

                            if (clientParameterList.length > 1 && !clientParameterList[1].trim().equals("")) {

                                truncateKey = clientParameterList[1];

                                if ((truncateKey.indexOf("#") == 0 && truncateKey.length() == 6) || truncateKey.equals("#all")) {

                                    long truncateCount = this.keyMapManager.truncateData(truncateKey);
                                    retParamBuf.append("61");
                                    retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                                    retParamBuf.append("true");
                                    retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                                    retParamBuf.append(truncateKey);
                                    retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                                    retParamBuf.append(truncateCount);
                                } else {

                                    retParamBuf.append("61");
                                    retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                                    retParamBuf.append("false");
                                    retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                                    retParamBuf.append(truncateKey);
                                    retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                                    retParamBuf.append("0");
                                }
                            } else {

                                retParamBuf.append("61");
                                retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                                retParamBuf.append("false");
                                retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                                retParamBuf.append(truncateKey);
                                retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                                retParamBuf.append("0");
                            }
                            break;
                        case 62 : 

                             String[] mAllSaveSize = this.keyMapManager.getAllSaveDataSize();

                            StringBuilder mSizeListStr = new StringBuilder(40);
                            if (mAllSaveSize != null) {
                                String sep = "";
                                for (int i = 0; i < mAllSaveSize.length; i++) {
                                    if (mAllSaveSize[i] != null && !mAllSaveSize[i].equals("")) {
                                        mSizeListStr.append(sep);
                                        mSizeListStr.append(mAllSaveSize[i]);
                                        sep = ",";
                                    }
                                }
                            } 
                            retParamBuf.append("62");
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append("true");
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(mSizeListStr.toString());
                            break;
                        case 100 :

                            // KeyMapManager Dump
                            this.keyMapManager.dump();
                            //retParamBuf = null;
                            break;
                        case 101 :

                            // KeyMapManager DataExport(For Backup)
                            this.keyMapManager.dataExport(pw);

                            //retParamBuf = null;
                            break;
                        default :

                            logger.debug("KeyManagerHelper No Method =[" + clientParameterList[0] + "]");
                            break;
                    }


                    if (retParamBuf.length() > 0) {

                        // Debugログ書き出し
                        if (StatusUtil.getDebugOption()) 
                            SystemUtil.debugLine(clientInfo + " : Response : " + retParamBuf);

                        // プロトコルに合わせて処理を分岐
                        if (this.porotocolTaker != null) {

                            // Takerで会話開始
                            pw.println(this.porotocolTaker.takeResponseLine(retParamBuf.toString().split(ImdstDefine.keyHelperClientParamSep)));
                            pw.flush();

                        } else {
                            pw.println(retParamBuf.toString());
                            pw.flush();
                        }
                    }


                    // 処理待機中のHelper数が閾値と同じかもしくは大きい場合は同様のクライアントを処理
                    if (ImdstDefine.retryClientReadFlg == true && numberOfQueueBindWaitCounter.get() >= returnProccessingCount) {

                        try {

                            if(!br.ready()) {

                                br.mark(1);
                                soc.setSoTimeout(200);
                                int readCheck = br.read();
                                br.reset();
                                reloopSameClient = true;
                                soc.setSoTimeout(0);
                                closeFlg = false;
                            } else {

                                reloopSameClient = true;
                                soc.setSoTimeout(0);
                                closeFlg = false;
                            }
                        } catch (SocketTimeoutException ste) {

                            // 読み込みタイムアウトなら読み出し待機Queueに戻す
                            queueMap[ImdstDefine.paramLast] = new Long(JavaSystemApi.currentTimeMillis);
                            queueParam[0] = queueMap;
                            super.addSmallSizeParameterQueue(addQueueNames, queueParam);
                            reloopSameClient = false;
                        } catch (Throwable te) {

                            // エラーの場合はクローズ
                            this.closeClientConnect(pw, br, soc);
                            reloopSameClient = false;
                        }
                    } else {

                        // 処理が完了したら読み出し確認キュー(KeyManagerAcceptHelper)に戻す
                        queueMap[ImdstDefine.paramLast] = new Long(JavaSystemApi.currentTimeMillis);
                        queueParam[0] = queueMap;
                        super.addSmallSizeParameterQueue(addQueueNames, queueParam);
                        reloopSameClient = false;
                    }


                } catch (SocketException se) {
                    closeFlg = true;
                    reloopSameClient = false;
                } catch (ArrayIndexOutOfBoundsException aie) {
                    logger.error("KeyManagerHelper No Method_1 =[" + clientParameterList[0] + "]");
                    reloopSameClient = false;
                } catch (NumberFormatException nfe) {
                    logger.error("KeyManagerHelper No Method_2 =[" + clientParameterList[0] + "]");
                    reloopSameClient = false;
                } finally {

                    // 処理待機を加算
                    if (!reloopSameClient)
                        numberOfQueueBindWaitCounter.getAndIncrement();
                }
            }

            ret = super.SUCCESS;
        } catch(Exception e) {

            logger.error("KeyManagerHelper - executeHelper - Error", e);
            ret = super.ERROR;
            //throw new BatchException(e);
        } finally {

            numberOfQueueBindWaitCounter.getAndIncrement();

            try {
                if (pw != null) {
                    pw.close();
                    pw = null;
                }

                if (br != null) {
                    br.close();
                    br = null;
                }

                if (soc != null) {
                    soc.close();
                    soc = null;
                }
            } catch(Exception e2) {
                logger.error("KeyManagerHelper - executeHelper - Error2", e2);
                ret = super.ERROR;
                //throw new BatchException(e2);
            }
        }

        //System.out.println("KeyManagerHelper - Loop END");
        //logger.debug("KeyManagerHelper - executeHelper - end");
        return ret;
    }

    /**
     * 初期化メソッド定義
     */
    public void endHelper() {
    }

    // KeyとDataNode値を格納する
    private String[] setDatanode(String key, String dataNodeStr, String transactionCode) {
        //logger.debug("KeyManagerHelper - setDatanode - start = [" + new String(BASE64DecoderStream.decode(key.getBytes())) + "]");
        String[] retStrs = new String[3];

        try {
            if (dataNodeStr.length() < setDatanodeMaxSize) {
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
            } else {

                retStrs[0] = "1";
                retStrs[1] = "false";
                retStrs[2] = "NG:Max Data Size Over";
            }
        } catch (BatchException be) {

            logger.debug("KeyManagerHelper - setDatanode - Error = [" + key + "]", be);
            //logger.debug("KeyManagerHelper - setDatanode - Error", be);
            retStrs[0] = "1";
            retStrs[1] = "false";
            retStrs[2] = "NG:KeyManagerHelper - setDatanode - Exception - " + be.toString();
        }

        //logger.debug("KeyManagerHelper - setDatanode - end = [" + new String(BASE64DecoderStream.decode(key.getBytes())) + "]");

        return retStrs;
    }


    // KeyとDataNode値を格納する
    // 既にデータが登録されている場合は失敗する。
    private String[] setDatanodeOnlyOnce(String key, String dataNodeStr, String transactionCode) {
        //logger.debug("KeyManagerHelper - setDatanodeOnlyOnce - start");
        String[] retStrs = new String[3];
        try {
            if (dataNodeStr.length() < setDatanodeMaxSize) {
                if(!this.keyMapManager.checkError()) {
                    if(this.keyMapManager.setKeyPairOnlyOnce(key, dataNodeStr, transactionCode)) {

                        retStrs[0] = "6";
                        retStrs[1] = "true";
                        retStrs[2] = "OK";
                    } else {

                        retStrs[0] = "6";
                        retStrs[1] = "false";
                        retStrs[2] = ImdstDefine.keyNodeKeyNewRegistErrMsg;
                    }
                } else {

                    retStrs[0] = "6";
                    retStrs[1] = "false";
                    retStrs[2] = "NG:KeyMapManager - setDatanodeOnlyOnce - CheckError - NG";
                }
            } else {

                retStrs[0] = "6";
                retStrs[1] = "false";
                retStrs[2] = "NG:Max Data Size Over";
            }
        } catch (BatchException be) {

            logger.debug("KeyManagerHelper - setDatanodeOnlyOnce - Error", be);
            retStrs[0] = "6";
            retStrs[1] = "false";
            retStrs[2] = "NG:KeyManagerHelper - setDatanodeOnlyOnce - Exception - " + be.toString();
        }
        //logger.debug("KeyManagerHelper - insertDatanode - end");
        return retStrs;
    }


    // KeyとDataNode値を格納する
    // バージョン値が異なる場合は失敗する
    private String[] setDatanodeVersionCheck(String key, String dataNodeStr, String transactionCode, String versionNo, boolean execCheck) {
        //logger.debug("KeyManagerHelper - setDatanodeVersionCheck - start");
        String[] retStrs = new String[3];
        try {
            if (dataNodeStr.length() < setDatanodeMaxSize) {
                if(!this.keyMapManager.checkError()) {

                    if(this.keyMapManager.containsKeyPair(key) && this.keyMapManager.setKeyPairVersionCheck(key, dataNodeStr, transactionCode, versionNo, execCheck)) {

                        retStrs[0] = "16";
                        retStrs[1] = "true";
                        retStrs[2] = "OK";
                    } else {

                        retStrs[0] = "16";
                        retStrs[1] = "false";
                        retStrs[2] = ImdstDefine.keyNodeKeyUpdatedErrMsg;
                    }
                } else {

                    retStrs[0] = "16";
                    retStrs[1] = "false";
                    retStrs[2] = "NG:KeyMapManager - setDatanodeVersionCheck - CheckError - NG";
                }
            } else {

                retStrs[0] = "16";
                retStrs[1] = "false";
                retStrs[2] = "NG:Max Data Size Over";
            }
        } catch (BatchException be) {

            logger.debug("KeyManagerHelper - setDatanodeVersionCheck - Error", be);
            retStrs[0] = "16";
            retStrs[1] = "false";
            retStrs[2] = "NG:KeyManagerHelper - setDatanodeVersionCheck - Exception - " + be.toString();
        }
        //logger.debug("KeyManagerHelper - setDatanodeVersionCheck - end");
        return retStrs;
    }


    // Key値で特定した値を渡された値で計算する
    // 計算ルールは以下
    // 1.送信された値をintに変換する。変換できない場合はfalseで返す.<br>
    // 2.送信された値をintに変換する。変換した値を足しこむ.<br>
    // 3.送信されたKey値で値がとれない。falseで返す.<br>
    // 4.送信されたKey値で取得した値が数値ではない。0で更新.<br>
    private String[] calcValue(String key, String dataNodeStr, String transactionCode) {
        //logger.debug("KeyManagerHelper - calcValue - start");
        String[] retStrs = new String[3];
        try {
            int calcVal = Integer.parseInt(new String(BASE64DecoderStream.decode(dataNodeStr.getBytes())));
            if(!this.keyMapManager.checkError()) {
                String retVal = null;
                if((retVal = this.keyMapManager.calcValue(key, calcVal, transactionCode)) != null) {

                    retStrs[0] = "13";
                    retStrs[1] = "true";
                    retStrs[2] = retVal;
                } else {

                    retStrs[0] = "13";
                    retStrs[1] = "false";
                    retStrs[2] = "";
                }
            } else {

                retStrs[0] = "13";
                retStrs[1] = "false";
                retStrs[2] = "NG:KeyMapManager - calcValue - Check Error - NG";
            }
        } catch (NumberFormatException nfe) {

            retStrs[0] = "13";
            retStrs[1] = "false";
            retStrs[2] = "NG:KeyManagerHelper - calcValue - NumberFormatException";
        } catch (BatchException be) {

            logger.debug("KeyManagerHelper - calcValue - Error", be);
            retStrs[0] = "13";
            retStrs[1] = "false";
            retStrs[2] = "NG:KeyManagerHelper - calcValue - Exception - " + be.toString();
        }
        //logger.debug("KeyManagerHelper - calcValue - end");
        return retStrs;
    }


    // KeyでDataNode値を取得する
    private String[] getDatanode(String key) {
        //logger.debug("KeyManagerHelper - getDatanode - start");
        String[] retStrs = null;
        try {
            if(!this.keyMapManager.checkError()) {
                if (this.keyMapManager.containsKeyPair(key)) {
                    String ret = this.keyMapManager.getKeyPair(key);

                    if (ret != null) {
                        retStrs = new String[3];
                        retStrs[0] = "2";
                        retStrs[1] = "true";
                        retStrs[2] = ret;
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
    //
    // スクリプトには必ずdataValueという変数が定義されているものとして、該当変数に取得したValue値が格納
    // されてスクリプトが実行される。
    // スクリプトには必ずdataKeyという変数が定義されているものとして、該当変数に取得に使用したKey値が格納
    // されてスクリプトが実行される。
    //
    // もどり値はScript内にexecRetという0 or 1 or 2で表す値とretValueという返却値が
    // 定義されているものとする
    // スクリプト実行後、execRetの値で状態を判断し、retValueが返却されるかが決定される。
    // execRet=1値を返す、execRet=0値を返さない、2=retValueの値で当該Valueを更新後、値を返却
    // retValue=Value返却される値となる
    // 返却値の配列の2番目の値がtrueならスクリプト実行後結果あり、
    // falseならスクリプト実行後結果なし、errorならスクリプト実行エラー
    private String[] getDatanodeScriptExec(String key, String scriptStr) {
        //logger.debug("KeyManagerHelper - getDatanode - start");

        String[] retStrs = null;

        try {
            if(!this.keyMapManager.checkError()) {
                if (this.keyMapManager.containsKeyPair(key)) {

                    retStrs = new String[3];
                    retStrs[0] = "8";
                    retStrs[1] = "true";
                    String workValueStr = this.keyMapManager.getKeyPair(key);
                    String[] workValues = null;
                    String[] setTimeValues = null;
                    String setTimeStr = "";
                    String tmpValue = null;
                    String updateValue = null;
                    String targetKey = null;
                    String flgStr = "";

                    // 取得した値からScriptを実行する部分だけ取り出し
                    if (workValueStr != null) {

                        // memcachedのプロトコルにより、フラグデータが格納されている可能性があるので左辺のみ取り出し

                        workValues = workValueStr.split(ImdstDefine.keyHelperClientParamSep);
                        if (workValues.length > 1) {
                            flgStr = ImdstDefine.keyHelperClientParamSep + ((String[])workValues[1].split(ImdstDefine.setTimeParamSep))[0];
                        }

                        // データ保存日時が記録されている場合があるので左辺のみ取り出し
                        setTimeValues = workValues[0].split(ImdstDefine.setTimeParamSep);
                        tmpValue = setTimeValues[0];

                        // 返却用に事前に更新日付を作成しておく
                        if (setTimeValues.length > 1) setTimeStr = ImdstDefine.setTimeParamSep + setTimeValues[1];

                        if (scriptStr != null && !scriptStr.trim().equals("") &&
                            !(new String(BASE64DecoderStream.decode(scriptStr.getBytes())).equals(ImdstDefine.imdstBlankStrData))) {

                            // TODO:エンジンの初期化に時間がかかるので他の影響を考えここで初期化
                            if (manager == null) {
                                manager = new ScriptEngineManager();
                                engine = manager.getEngineByName("JavaScript");
                            }

                            // 引数設定
                            // Key値を設定
                            if(key.indexOf("#") == 0) {
                                engine.put("dataKey", new String(BASE64DecoderStream.decode(key.substring(6).getBytes()), ImdstDefine.keyWorkFileEncoding));
                            } else {
                                engine.put("dataKey", new String(BASE64DecoderStream.decode(key.getBytes()), ImdstDefine.keyWorkFileEncoding));
                            }

                            // Value値を設定
                            if (tmpValue == null) {
                                engine.put("dataValue", "");
                            } else if (tmpValue.equals(ImdstDefine.imdstBlankStrData)) {
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

                                    // 最後に更新日付を結合する
                                    String retValue = (String)engine.get("retValue");

                                    if (retValue != null && !retValue.equals("")) {
                                        retStrs[2] = new String(BASE64EncoderStream.encode(retValue.getBytes()),ImdstDefine.keyWorkFileEncoding) + setTimeStr;
                                    } else {
                                        retStrs[2] = ImdstDefine.imdstBlankStrData + setTimeStr;
                                    }
                                } else if (execRet != null && execRet.equals("2")) {

                                    // データを更新して、返す

                                    // 最後に更新日付を結合する 
                                    String retValue = (String)engine.get("retValue");

                                    if (retValue != null && !retValue.equals("")) {

                                        retStrs[2] = new String(BASE64EncoderStream.encode(retValue.getBytes()),ImdstDefine.keyWorkFileEncoding) + flgStr + setTimeStr;
                                    } else {

                                        retStrs[2] = ImdstDefine.imdstBlankStrData + flgStr + setTimeStr;
                                    }

                                    // 値を更新
                                    this.setDatanode(key, retStrs[2], "-1");
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
    private String[] setTagdata(String tag, String key, String transactionCode) {
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
    private String[] removeDatanode(String key, String transactionCode) {
        //logger.debug("KeyManagerHelper - removeDatanode - start");
        String[] retStrs = null;
        try {
            if(!this.keyMapManager.checkError()) {
                if (this.keyMapManager.containsKeyPair(key)) {
                    String ret = this.keyMapManager.removeKeyPair(key, transactionCode);

                    if (ret != null) {
                        retStrs = new String[3];
                        retStrs[0] = "5";
                        retStrs[1] = "true";
                        retStrs[2] = ret;
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
    private String[] getTagdata(String tag) {
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


    // Keyに紐付いている指定のTagを外す
    private String[] removeTargetTagInKey(String tag, String key, String transactionCode) {
        //logger.debug("KeyManagerHelper - removeTargetTagInKey - start");
        String[] retStrs = null;
        try {
            if(!this.keyMapManager.checkError()) {

                boolean ret = this.keyMapManager.removeTargetTagInKey(tag, key, transactionCode);

                if (ret == true) {
                    retStrs = new String[2];
                    retStrs[0] = "40";
                    retStrs[1] = "true";

                } else {
                    retStrs = new String[2];
                    retStrs[0] = "40";
                    retStrs[1] = "false";
                }
            } else {
                    retStrs = new String[2];
                    retStrs[0] = "40";
                    retStrs[1] = "error";
            }
        } catch (Exception e) {
            logger.error("KeyManagerHelper - removeTargetTagInKey - Error", e);
            retStrs = new String[2];
            retStrs[0] = "40";
            retStrs[1] = "error";
        }
        //logger.debug("KeyManagerHelper - removeTargetTagInKey - end");
        return retStrs;
    }


    // 指定のTagを削除する
    private String[] removeTagdata(String tag, String transactionCode) {
        //logger.debug("KeyManagerHelper - removeTagdata - start");
        String[] retStrs = null;
        try {
            if(!this.keyMapManager.checkError()) {
                if (this.keyMapManager.containsTagPair(tag)) {

                    retStrs = new String[3];
                    retStrs[0] = "41";
                    retStrs[1] = "true";
                    retStrs[2] = this.keyMapManager.removeTagRelation(tag, transactionCode);
                } else {
                    retStrs = new String[2];
                    retStrs[0] = "41";
                    retStrs[1] = "false";
                }
            } else {
                    retStrs = new String[2];
                    retStrs[0] = "41";
                    retStrs[1] = "false";
            }
        } catch (Exception e) {
            logger.error("KeyManagerHelper - removeTagdata - Error", e);
            retStrs = new String[2];
            retStrs[0] = "41";
            retStrs[1] = "false";
        }
        //logger.debug("KeyManagerHelper - removeTagdata - end");
        return retStrs;
    }


    /**
     * Clientとの接続を切断する.<br>
     *
     */
    private void closeClientConnect(PrintWriter pw, BufferedReader br, Socket socket) {
        try {
            if(pw != null) {
                pw.close();
                pw = null;
            }

            if(br != null) {
                br.close();
                br = null;
            }

            if(socket != null) {
                socket.close();
                socket = null;
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }

}