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
import org.imdst.util.io.*;

/**
 * KeyNodeのデータを最適化するHelperクラス<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyNodeOptimizationHelper extends AbstractMasterManagerHelper {

    // ノードの監視サイクル時間(ミリ秒)
    private int checkCycle = 6000 * 3;

    private ArrayList mainNodeList = null;

    private OutputStreamWriter osw = null;
    private PrintWriter pw = null;
    private InputStreamReader isr = null;
    private BufferedReader br = null;
    private Socket socket = null;

    private OutputStreamWriter removeOsw = null;
    private PrintWriter removePw = null;
    private InputStreamReader removeIsr = null;
    private BufferedReader removeBr = null;
    private Socket removeSocket = null;

    private int nextData = 1;

    private HashMap connectMap = null;
    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(KeyNodeOptimizationHelper.class);

    // 初期化メソッド定義
    public void initHelper(String initValue) {
        connectMap = new HashMap();
        // 監視サイクル初期化
        if (initValue != null && !initValue.equals("")) {
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
        logger.debug("KeyNodeOptimizationHelper - executeHelper - start");
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
            try {
                Thread.sleep(checkCycle);
                // 停止ファイル関係チェック
                if (StatusUtil.getStatus() == 1) {
                    serverRunning = false;
                    logger.info("KeyNodeOptimizationHelper - 状態異常です");
                }

                if (StatusUtil.getStatus() == 2) {
                    serverRunning = false;
                    logger.info("KeyNodeOptimizationHelper - 終了状態です");
                }

                if (!super.isExecuteKeyNodeOptimization()) continue;

                HashMap allNodeInfo = DataDispatcher.getAllDataNodeInfo();
                this.mainNodeList = (ArrayList)allNodeInfo.get("main");

                // ノード数分チェック
                for (int i = 0; i < mainNodeList.size(); i++) {
                    Thread.sleep(checkCycle);


                    // 停止ファイル関係チェック
                    if (StatusUtil.getStatus() == 1) {
                        serverRunning = false;
                        logger.info("KeyNodeOptimizationHelper - 状態異常です");
                    }

                    if (StatusUtil.getStatus() == 2) {
                        serverRunning = false;
                        logger.info("KeyNodeOptimizationHelper - 終了状態です");
                    }

                    serverStopMarkerFileName = super.getPropertiesValue("ServerStopFile");

                    serverStopMarkerFile = new File(new File(serverStopMarkerFileName).getAbsolutePath());
                    if (serverStopMarkerFile.exists()) {
                        serverRunning = false;
                        logger.info("KeyNodeOptimizationHelper - Server停止ファイルが存在します");
                        StatusUtil.setStatus(2);
                    }

                    // MainのMasterNodeの場合のみ実行
                    if (StatusUtil.isMainMasterNode()) {
                        //imdstKeyValueClient = new ImdstKeyValueClient();

                        //imdstKeyValueClient.connect(myInfoDt[0], Integer.parseInt(myInfoDt[1]));

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

                                if (optimizeTargetKeys[idx] != null && !optimizeTargetKeys[idx].trim().equals("")) {
                                    this.sendTargetData(optimizeTargetKeys[idx]);
                                    //imdstKeyValueClient.getValueNoEncode(optimizeTargetKeys[idx]);
                                }
                            }
                        }

                        this.closeConnect();
                        //imdstKeyValueClient.close();
                        //imdstKeyValueClient = null;

                        logger.info(nodeInfo + " Optimization End");    
                        logger.info("************************************************************");
                    }
                }

                super.executeKeyNodeOptimization(false);
            } catch(Exception e) {
                logger.error("KeyNodeOptimizationHelper - executeHelper - Error", e);
            } finally {
                try {
                    if (imdstKeyValueClient != null) imdstKeyValueClient.close();
                } catch (Exception e2) {}
            }
        }

        logger.debug("KeyNodeOptimizationHelper - executeHelper - end");
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


            this.removeSocket = new Socket(nodeName, nodePort);
            this.removeSocket.setSoTimeout(ImdstDefine.recoverConnectionTimeout);

            this.removeOsw = new OutputStreamWriter(this.removeSocket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding);
            this.removePw = new PrintWriter(new BufferedWriter(this.removeOsw));

            this.removeIsr = new InputStreamReader(this.removeSocket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding);
            this.removeBr = new BufferedReader(this.removeIsr);

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


            // 転送済みデータ削除用
            buf = new StringBuffer();
            // 処理番号31
            buf.append("31");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append("true");

            // 送信
            removePw.println(buf.toString());
            removePw.flush();

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


    private boolean sendTargetData(String targetDataLine) {
        boolean ret = false;

        KeyNodeConnector mainKeyNodeConnector = null;
        KeyNodeConnector subKeyNodeConnector = null;
        KeyNodeConnector thirdKeyNodeConnector = null;

        String[] targetDatas = null;
        String[] keyNodeInfo = null;

        try {
            targetDatas = targetDataLine.split(ImdstDefine.keyHelperClientParamSep);
            keyNodeInfo = DataDispatcher.dispatchKeyNode(targetDatas[1], false);
            // 取得実行
            if (keyNodeInfo.length > 2) {

                if(connectMap.containsKey(keyNodeInfo[2])) {

                    mainKeyNodeConnector = (KeyNodeConnector)connectMap.get(keyNodeInfo[2]);
                } else {

                    mainKeyNodeConnector = new KeyNodeConnector(keyNodeInfo[0], Integer.parseInt(keyNodeInfo[1]), keyNodeInfo[2]);
                    mainKeyNodeConnector.connect();
                    mainKeyNodeConnector.println("30" + ImdstDefine.keyHelperClientParamSep + "true");
                    mainKeyNodeConnector.flush();
                    connectMap.put(keyNodeInfo[2], mainKeyNodeConnector);
                }
            }

            if (keyNodeInfo.length > 5) {
                if(connectMap.containsKey(keyNodeInfo[5])) {

                    subKeyNodeConnector = (KeyNodeConnector)connectMap.get(keyNodeInfo[5]);
                } else {

                    subKeyNodeConnector = new KeyNodeConnector(keyNodeInfo[3], Integer.parseInt(keyNodeInfo[4]), keyNodeInfo[5]);
                    subKeyNodeConnector.connect();
                    subKeyNodeConnector.println("30" + ImdstDefine.keyHelperClientParamSep + "true");
                    subKeyNodeConnector.flush();
                    connectMap.put(keyNodeInfo[5], subKeyNodeConnector);
                }
            }

            if (keyNodeInfo.length > 8) {
                if(connectMap.containsKey(keyNodeInfo[8])) {

                    thirdKeyNodeConnector = (KeyNodeConnector)connectMap.get(keyNodeInfo[8]);
                } else {

                    thirdKeyNodeConnector = new KeyNodeConnector(keyNodeInfo[6], Integer.parseInt(keyNodeInfo[7]), keyNodeInfo[8]);
                    thirdKeyNodeConnector.connect();
                    thirdKeyNodeConnector.println("30" + ImdstDefine.keyHelperClientParamSep + "true");
                    thirdKeyNodeConnector.flush();
                    connectMap.put(keyNodeInfo[8], thirdKeyNodeConnector);
                }
            }


            if (mainKeyNodeConnector != null) {
                mainKeyNodeConnector.println(targetDataLine);
                mainKeyNodeConnector.flush();
                String sendRet = mainKeyNodeConnector.readLine(targetDataLine);
                if(sendRet != null && sendRet.equals("next")) ret = true;
            }

            if (subKeyNodeConnector != null) {
                subKeyNodeConnector.println(targetDataLine);
                subKeyNodeConnector.flush();
                String sendRet = subKeyNodeConnector.readLine(targetDataLine);
                if(sendRet != null && sendRet.equals("next")) ret = true;
            }

            if (thirdKeyNodeConnector != null) {
                thirdKeyNodeConnector.println(targetDataLine);
                thirdKeyNodeConnector.flush();
                String sendRet = thirdKeyNodeConnector.readLine(targetDataLine);
                if(sendRet != null && sendRet.equals("next")) ret = true;
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e);
        } finally {
            try {
                // 正常に転送出来ていれば、データを消しこむ
                removePw.println(targetDatas[0] + ImdstDefine.keyHelperClientParamSep + targetDatas[1]);
                removePw.flush();
                String removeRet = removeBr.readLine();
            } catch (Exception ee) {
                ee.printStackTrace();
                logger.error(ee);
                ret = false;
            }
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


            // コネクション切断
            if (this.removePw != null) {
                this.removePw.println("-1");
                this.removePw.flush();
                this.removePw.println(ImdstDefine.imdstConnectExitRequest);
                this.removePw.flush();
                this.removePw.close();
                this.removePw = null;
            }

            // コネクション切断
            if (this.removeBr != null) {
                this.removeBr.close();
                this.removeBr = null;
            }


            if (this.removeSocket != null) {
                this.removeSocket.close();
                this.removeSocket = null;
            }


        } catch(Exception e2) {
            // 無視
            logger.error(e2);
        }
    }
}