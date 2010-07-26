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
    private int checkCycle = 60000 * 3;

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

        String[] optimizeTargetKeys = null;
        String myInfo = null;
        String[] myInfoDt = null;

        myInfo = StatusUtil.getMyNodeInfo();
        if (myInfo == null || myInfo.trim().equals("")) {
            myInfo = "127.0.0.1:8888";
        }

        myInfoDt = myInfo.split(":");

        while (serverRunning) {

            HashMap moveTargetData = super.getConsistentHashMoveData();

            if (moveTargetData != null) {
                try {

                    // 全ての移動対象のノードを処理
                    Set set = moveTargetData.keySet();
                    Iterator iterator = set.iterator();

                    String[] keyList = new String[convertMap.size()];
                    for (int idx = 0; idx < keyList.length; idx++) {
                        keyList[idx] = (String)iterator.next();
                    }

                    for (int idx = 0; idx < keyList.length; idx++) {

                        // レンジを分解
                        String allConvertRang = (String)convertMap.get(keyList[idx]);
                        String[] convertRangs = allConvertRang.split(",");
                        int[][] rangs = new int[convertRangs.length][2];

                        // レンジのstartとendをセット単位でintの配列に落とす
                        for (int ii = 0; ii < convertRangs.length; ii++) {

                            String[] workRangs = convertRangs[ii].split("-");
                            rangs[ii][0] = Integer.parseInt(workRangs[0]);
                            rangs[ii][1] = Integer.parseInt(workRangs[1]);
                        }

                    }












                    HashMap allNodeInfo = DataDispatcher.getAllDataNodeInfo();
                    this.mainNodeList = (ArrayList)allNodeInfo.get("main");

                    // ノード数分チェック
                    for (int i = 0; i < mainNodeList.size(); i++) {
                        Thread.sleep(checkCycle);


                        // 停止ファイル関係チェック
                        if (StatusUtil.getStatus() == 1) {
                            serverRunning = false;
                            logger.info("KeyNodeOptimizationConsistentHashHelper - 状態異常です");
                        }

                        if (StatusUtil.getStatus() == 2) {
                            serverRunning = false;
                            logger.info("KeyNodeOptimizationConsistentHashHelper - 終了状態です");
                        }

                        serverStopMarkerFileName = super.getPropertiesValue("ServerStopFile");

                        serverStopMarkerFile = new File(new File(serverStopMarkerFileName).getAbsolutePath());
                        if (serverStopMarkerFile.exists()) {
                            serverRunning = false;
                            logger.info("KeyNodeOptimizationConsistentHashHelper - Server停止ファイルが存在します");
                            StatusUtil.setStatus(2);
                        }

                        // MainのMasterNodeの場合のみ実行
                        if (StatusUtil.isMainMasterNode()) {
                            imdstKeyValueClient = new ImdstKeyValueClient();

                            imdstKeyValueClient.connect(myInfoDt[0], Integer.parseInt(myInfoDt[1]));

                            // ノードチェック(メイン)
                            String nodeInfo = (String)mainNodeList.get(i);

                            logger.info("************************************************************");
                            logger.info(nodeInfo + " Optimization Start");


                            String[] nodeDt = nodeInfo.split(":");
                            optimizeTargetKeys = null;
                            
                            this.closeConnect();
                            this.searchTargetData(nodeDt[0], Integer.parseInt(nodeDt[1]), i);
                            while((optimizeTargetKeys = this.nextData()) != null) {
                                for (int idx = 0; idx < optimizeTargetKeys.length; idx++) {
                                    if (!optimizeTargetKeys[i].trim().equals("")) {
                                        imdstKeyValueClient.getValueNoEncode(optimizeTargetKeys[idx]);
                                    }
                                }
                            }

                            imdstKeyValueClient.close();
                            imdstKeyValueClient = null;

                            logger.info(nodeInfo + " Optimization End");    
                            logger.info("************************************************************");
                        }
                    }
                } catch(Exception e) {
                    logger.error("KeyNodeOptimizationConsistentHashHelper - executeHelper - Error", e);
                } finally {
                    try {
                        if (imdstKeyValueClient != null) imdstKeyValueClient.close();
                    } catch (Exception e2) {}
                }
            }
        }

        logger.debug("KeyNodeOptimizationConsistentHashHelper - executeHelper - end");
        return ret;
    }


    /**
     * Helper後処理.<br>
     *
     */
    public void endHelper() {
    }


    private void searchTargetData(String nodeName, int nodePort, int dataNodeMatchNo) throws BatchException {
        StringBuffer buf = null;

        try {
            this.socket = new Socket(nodeName, nodePort);
            this.socket.setSoTimeout(ImdstDefine.recoverConnectionTimeout);

            this.osw = new OutputStreamWriter(this.socket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding);
            this.pw = new PrintWriter(new BufferedWriter(this.osw));

            this.isr = new InputStreamReader(this.socket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding);
            this.br = new BufferedReader(this.isr);

            // コピー元からデータ読み込み
            buf = new StringBuffer();
            // 処理番号20
            buf.append("26");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append("true");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append(dataNodeMatchNo);
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append(DataDispatcher.ruleInt);

            // 送信
            pw.println(buf.toString());
            pw.flush();


        } catch(Exception e) {
            throw new BatchException(e);
        } 
    }


    private String[] nextData() throws BatchException {

        String[] ret = null;
        String line = null;

        try {
            while((line = this.br.readLine()) != null) {

                if (line.length() > 0) {
                    if (line.length() == 2 && line.equals("-1")) {
                      

                        break;
                    } else {

                        ret = line.split(ImdstDefine.imdstConnectAllDataSendDataSep);
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