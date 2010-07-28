package org.imdst.helper;

import java.io.*;
import java.util.*;
import java.net.*;

import org.batch.lang.BatchException;
import org.batch.job.AbstractHelper;
import org.batch.job.IJob;
import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.imdst.util.ImdstDefine;
import org.imdst.util.DataDispatcher;
import org.imdst.util.StatusUtil;
import org.imdst.client.ImdstKeyValueClient;

/**
 * KeyNodeのデータを最適化するHelperクラス.<br>
 * ConsistentHash用.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyNodeOptimizationConsistentHashHelper extends AbstractMasterManagerHelper {

    // ノードの監視サイクル時間(ミリ秒)
    private int checkCycle = 10000 * 1;

    private ArrayList mainNodeList = null;

    private OutputStreamWriter osw = null;
    private PrintWriter pw = null;
    private InputStreamReader isr = null;
    private BufferedReader br = null;
    private Socket socket = null;

    private int nextData = 1;

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(KeyNodeOptimizationConsistentHashHelper.class);

    // 初期化メソッド定義
    public void initHelper(String initValue) {
        // 監視サイクル初期化
        if (initValue != null) {
            // 単位は秒
            try {
                this.checkCycle = Integer.parseInt(initValue);
            } catch (Exception e) {
                // 変換失敗
            }
        }
    }

    // Jobメイン処理定義
    public String executeHelper(String optionParam) throws BatchException {
        logger.debug("KeyNodeOptimizationConsistentHashHelper - executeHelper - start");
        String ret = SUCCESS;
        String serverStopMarkerFileName = null;
        File serverStopMarkerFile = null;

        boolean serverRunning = true;

        ImdstKeyValueClient imdstKeyValueClient = null;

        HashMap moveTargetData = null;


        String addMainDataNodeInfo = null;
        HashMap mainMoveTargetMap = null;
        String[] toMainDataNodeDt = null;
        Set mainSet = null;
        Iterator mainIterator = null;

        Socket toMainSocket = null;
        OutputStreamWriter toMainOsw = null;
        PrintWriter toMainPw = null;
        InputStreamReader toMainIsr = null;
        BufferedReader toMainBr = null;


        String addSubDataNodeInfo = null;
        HashMap subMoveTargetMap = null;
        String[] toSubDataNodeDt = null;
        Set subSet = null;
        Iterator subIterator = null;

        Socket toSubSocket = null;
        OutputStreamWriter toSubOsw = null;
        PrintWriter toSubPw = null;
        InputStreamReader toSubIsr = null;
        BufferedReader toSubBr = null;


        String[] optimizeTargetKeys = null;
        String myInfo = null;
        String[] myInfoDt = null;

        myInfo = StatusUtil.getMyNodeInfo();
        if (myInfo == null || myInfo.trim().equals("")) {
            myInfo = "127.0.0.1:8888";
        }

        myInfoDt = myInfo.split(":");

        try {
            while (serverRunning) {
                Thread.sleep(checkCycle);

                if (StatusUtil.isMainMasterNode()) {

                    moveTargetData = super.getConsistentHashMoveData();

                    StringBuffer sendRequestBuf = new StringBuffer();

                    // 送信リクエスト文字列作成
                    sendRequestBuf.append()
                    // 処理番号28
                    sendRequestBuf.append("28");
                    sendRequestBuf.append(ImdstDefine.keyHelperClientParamSep);
                    sendRequestBuf.append("true");

                    if (moveTargetData != null) {
                        while (true) {
                            try {

                                // 全て初期化
                                toMainSocket = null;
                                toMainOsw = null;
                                toMainPw = null;
                                mainSet = null;
                                mainIterator = null;

                                toSubSocket = null;
                                toSubOsw = null;
                                toSubPw = null;
                                subSet = null;
                                subIterator = null;


                                // データ移動先のメインデータノードに接続
                                addMainDataNodeInfo = (String)moveTargetData.get("tomain");
                                toMainDataNodeDt = addMainDataNodeInfo.split(":");
                                mainMoveTargetMap = (HashMap)moveTargetData.get("main");

                                try {
                                    toMainSocket = new Socket(toMainDataNodeDt[0], Integer.parseInt(toMainDataNodeDt[1]));
                                    toMainSocket.setSoTimeout(ImdstDefine.recoverConnectionTimeout);

                                    toMainOsw = new OutputStreamWriter(toMainSocket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding);
                                    toMainPw = new PrintWriter(new BufferedWriter(toMainOsw));

                                    mainSet = mainMoveTargetMap.keySet();
                                    mainIterator = mainSet.iterator();

                                    // 移行先メインデータノードにデータ移行開始を送信
                                    toMainPw.println(sendRequestBuf.toString());
                                    toMainPw.flush();
                                } catch (Exception e) {
                                    toMainPw = null;
                                }


                                // スレーブノード処理
                                addSubDataNodeInfo = (String)moveTargetData.get("tosub");
                                subMoveTargetMap = (HashMap)moveTargetData.get("sub");

                                if (addSubDataNodeInfo != null) {
                                    try {
                                        toSubDataNodeDt = addSubDataNodeInfo.split(":");
                                        toSubSocket = new Socket(toSubDataNodeDt[0], Integer.parseInt(toSubDataNodeDt[1]));
                                        toSubSocket.setSoTimeout(ImdstDefine.recoverConnectionTimeout);

                                        toSubOsw = new OutputStreamWriter(toSubSocket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding);
                                        toSubPw = new PrintWriter(new BufferedWriter(toSubOsw));

                                        subSet = subMoveTargetMap.keySet();
                                        subIterator = subSet.iterator();

                                        // 移行先スレーブデータノードにデータ移行開始を送信
                                        toSubPw.println(sendRequestBuf.toString());
                                        toSubPw.flush();
                                    } catch (Exception e) {
                                        toSubPw = null;
                                    }
                                }


                                // 全ての移動対象のノードを処理
                                // 対象データノード1ノードづつ処理
                                while(mainIterator.hasNext()) {
                                    Map.Entry obj = (Map.Entry)mainIterator.next();
                                    String dataNodeStr = null;
                                    String[] dataNodeDetail = null;
                                    String rangStr = null;
                                    String targetDataStr = null;

                                    // キー値を取り出し
                                    dataNodeStr = (String)obj.getKey();
                                    dataNodeDetail = dataNodeStr.split(":");

                                    // Rangの文字列を取り出し
                                    rangStr = mainMoveTargetMap.get(dataNodeStr);

                                    // 対象ノードからデータ取り出し
                                    this.getTargetData(dataNodeDetail[0], Integer.parseInt(dataNodeDetail[1]), rangStr);


                                    // 対象のデータを順次対象のノードに移動
                                    while((targetDataStr = this.nextData()) != null) {
                                        toMainPw.println(targetDataStr);
                                        toMainPw.flush();

                                        if (toSubPw != null) {
                                            toSubPw.println(targetDataStr);
                                            toSubPw.flush();
                                        }
                                    }

                                    this.closeConnect();
                                }

                                // 全てのデータの移行が完了
                                toMainPw.println("-1");
                                toMainPw.flush();
                                toMainPw.println(ImdstDefine.imdstConnectExitRequest);
                                toMainPw.flush();
                                toMainPw.close();
                                toMainSocket.close();

                                if (toSubPw != null) {

                                    toSubPw.println("-1");
                                    toSubPw.flush();
                                    toSubPw.println(ImdstDefine.imdstConnectExitRequest);
                                    toSubPw.flush();
                                    toSubPw.close();
                                    toSubSocket.close();
                                }

                                // 移動もとのデータを消す処理をここに追加
                                // データ消す

                                // メモリ上から依頼を消す
                                super.removeConsistentHashMoveData();
                                break
                            } catch (Exception e) {
                                // もしエラーが発生した場合はリトライ
                                logger.error("Data shift Error =[" + e.toString() + "]");
                                e.printStackTrace();
                                logger.error("Data shift Error Detail =[" + moveTargetData + "]");
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("KeyNodeOptimizationConsistentHashHelper - executeHelper - error", e);
        }
-------------------------------- ここまで -------------------------------------------------------------
ここからIterator回しながらMain,Slaveそれぞれのレンジ対象データを移行する
その後 super.removeConsistentHashMoveData()呼び出して、dataNode上から移行依頼のデータも消す

        logger.debug("KeyNodeOptimizationConsistentHashHelper - executeHelper - end");
        return ret;
    }


    /**
     * Helper後処理.<br>
     *
     */
    public void endHelper() {
    }


    private void getTargetData(String nodeName, int nodePort, String rangStr) throws BatchException {
        StringBuffer buf = null;

        try {
            this.socket = new Socket(nodeName, nodePort);
            this.socket.setSoTimeout(ImdstDefine.recoverConnectionTimeout);

            this.osw = new OutputStreamWriter(this.socket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding);
            this.pw = new PrintWriter(new BufferedWriter(this.osw));

            this.isr = new InputStreamReader(this.socket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding);
            this.br = new BufferedReader(this.isr);

            // 移動元からデータ読み込み
            buf = new StringBuffer();
            // 処理番号27
            buf.append("27");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append("true");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append(rangStr);

            // 送信
            pw.println(buf.toString());
            pw.flush();


        } catch(Exception e) {
            throw new BatchException(e);
        } 
    }


    private String nextData() throws BatchException {
        String ret = null;
        String line = null;

        try {
            while((line = this.br.readLine()) != null) {

                if (line.length() > 0) {
                    if (line.length() == 2 && line.equals("-1")) {

                        break;
                    } else {

                        ret = line;
                        break;
                    }
                }
            }
        } catch(Exception e) {
            throw new BatchException(e);
        }
        return ret;
    }

    private void closeConnect() {
        try {

            // コネクション切断
            if (this.pw != null) {
                this.pw.println(ImdstDefine.imdstConnectExitRequest);
                this.pw.flush();
                this.pw.close();
                this.pw = null;
            }

            // コネクション切断
            if (this.br != null) {
                this.br.close();
                this.br = null;
            }


            if (this.socket != null) {
                this.socket.close();
                this.socket = null;
            }

        } catch(Exception e2) {
            // 無視
            logger.error(e2);
        }
    }
}