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


    private OutputStreamWriter osw = null;
    private PrintWriter pw = null;
    private InputStreamReader isr = null;
    private BufferedReader br = null;
    private Socket socket = null;

    private ArrayList removeDataKeys = null;

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

                serverStopMarkerFileName = super.getPropertiesValue("ServerStopFile");

                serverStopMarkerFile = new File(new File(serverStopMarkerFileName).getAbsolutePath());
                if (serverStopMarkerFile.exists()) {
                    serverRunning = false;
                    logger.info("KeyNodeOptimizationHelper - Server停止ファイルが存在します");
                    StatusUtil.setStatus(2);
                }

                if (!super.isExecuteKeyNodeOptimization()) continue;

                HashMap allNodeInfo = DataDispatcher.getAllDataNodeInfo();
                ArrayList mainNodeList = (ArrayList)allNodeInfo.get("main");
                ArrayList subNodeList = (ArrayList)allNodeInfo.get("sub");
                ArrayList thirdNodeList = (ArrayList)allNodeInfo.get("third");

                Thread.sleep(checkCycle);

                // MainのMasterNodeの場合のみ実行
                if (StatusUtil.isMainMasterNode()) {

                    // ノード数分チェック
                    for (int i = 0; i < mainNodeList.size(); i++) {

                        //imdstKeyValueClient = new ImdstKeyValueClient();

                        //imdstKeyValueClient.connect(myInfoDt[0], Integer.parseInt(myInfoDt[1]));

                        // ノードチェック(メイン)
                        String nodeInfo = (String)mainNodeList.get(i);
                        String subNodeInfo = null;
                        String thirdNodeInfo = null;

                        if (subNodeList != null) subNodeInfo = (String)subNodeList.get(i);
                        if (thirdNodeList != null) thirdNodeInfo = (String)thirdNodeList.get(i);

                        logger.info("************************************************************");
                        logger.info(nodeInfo + " Optimization Start");


                        String[] nodeDt = nodeInfo.split(":");
                        String[] subNodeDt = new String[2];
                        String[] thirdNodeDt = new String[2];
                        if (subNodeInfo != null) subNodeDt = subNodeInfo.split(":");
                        if (thirdNodeInfo != null) thirdNodeDt = thirdNodeInfo.split(":");

                        optimizeTargetKeys = null;
                        this.closeGetConnect();

                        this.searchTargetData(nodeDt[0], Integer.parseInt(nodeDt[1]), i);
                        removeDataKeys = new ArrayList(10000);

                        while((optimizeTargetKeys = this.nextData()) != null) {

                            for (int idx = 0; idx < optimizeTargetKeys.length; idx++) {

                                if (optimizeTargetKeys[idx] != null && !optimizeTargetKeys[idx].trim().equals("")) {
                                    this.sendTargetData(optimizeTargetKeys[idx]);
                                    //imdstKeyValueClient.getValueNoEncode(optimizeTargetKeys[idx]);
                                }
                            }
                        }

                        this.closeGetConnect();
                        this.removeTargetData(nodeDt[0], nodeDt[1], subNodeDt[0], subNodeDt[1], thirdNodeDt[0], thirdNodeDt[1]);

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
System.out.println("searchTargetData - start");
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
System.out.println("searchTargetData - end");
    }


    private String[] nextData() throws BatchException {

        String[] ret = null;
        String line = null;

        try {
System.out.println("nextData - start");
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
        } catch(SocketException se) {
            // 切断とみなす
            logger.error(se);
        } catch(Exception e) {
            throw new BatchException(e);
        }
System.out.println("nextData - end");
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
System.out.println("sendTargetData - start");
            targetDatas = targetDataLine.split(ImdstDefine.keyHelperClientParamSep);

            // タグの場合はKey値からインデッス文字を外して振り分け先を決定
            if (targetDatas[0].equals("1")) {

                // 通常データ
                keyNodeInfo = DataDispatcher.dispatchKeyNode(targetDatas[1], false);
            } else if (targetDatas[0].equals("2")) {
                // タグ
                keyNodeInfo = DataDispatcher.dispatchKeyNode(targetDatas[1].substring(0, (targetDatas[1].lastIndexOf("=") +1)), false);
            }


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

            // 正常に転送出来ていれば、データを消しこむ
            if (ret) removeDataKeys.add(targetDatas[0] + ImdstDefine.keyHelperClientParamSep + targetDatas[1]);
        }
System.out.println("sendTargetData - end");
        return ret;
    }


    private void removeTargetData(String mainNodeName, String mainNodePort, String subNodeName, String subNodePort, String thirdNodeName, String thirdNodePort) {

        PrintWriter[] removePw = new PrintWriter[3];
        BufferedReader[] removeBr = new BufferedReader[3];
        Socket[] removeSocket = new Socket[3];

        try {
System.out.println("removeTargetData - start");
            removeSocket[0] = new Socket(mainNodeName, Integer.parseInt(mainNodePort));
            removeSocket[0].setSoTimeout(ImdstDefine.recoverConnectionTimeout);
            removePw[0] = new PrintWriter(new BufferedWriter(new OutputStreamWriter(removeSocket[0].getOutputStream(), ImdstDefine.keyHelperClientParamEncoding)));
            removeBr[0] = new BufferedReader(new InputStreamReader(removeSocket[0].getInputStream(), ImdstDefine.keyHelperClientParamEncoding));

            if (subNodeName != null) {
                removeSocket[1] = new Socket(subNodeName, Integer.parseInt(subNodePort));
                removeSocket[1].setSoTimeout(ImdstDefine.recoverConnectionTimeout);
                removePw[1] = new PrintWriter(new BufferedWriter(new OutputStreamWriter(removeSocket[1].getOutputStream(), ImdstDefine.keyHelperClientParamEncoding)));
                removeBr[1] = new BufferedReader(new InputStreamReader(removeSocket[1].getInputStream(), ImdstDefine.keyHelperClientParamEncoding));
            }

            if (thirdNodeName != null) {
                removeSocket[2] = new Socket(thirdNodeName, Integer.parseInt(thirdNodePort));
                removeSocket[2].setSoTimeout(ImdstDefine.recoverConnectionTimeout);
                removePw[2] = new PrintWriter(new BufferedWriter(new OutputStreamWriter(removeSocket[2].getOutputStream(), ImdstDefine.keyHelperClientParamEncoding)));
                removeBr[2] = new BufferedReader(new InputStreamReader(removeSocket[2].getInputStream(), ImdstDefine.keyHelperClientParamEncoding));
            }


            for (int idx = 0; idx < removeSocket.length; idx++) {
                if (removeSocket[idx] == null) break;
                // 転送済みデータ削除用
                StringBuffer buf = new StringBuffer();
                // 処理番号31
                buf.append("31");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append("true");

                // 送信
                removePw[idx].println(buf.toString());
                removePw[idx].flush();
            }

            for (int idx = 0; idx < removeDataKeys.size(); idx++) {
                for (int nodeIdx = 0; nodeIdx < removeSocket.length; nodeIdx++) {

                    if (removeSocket[nodeIdx] == null) break;
                    // 正常に転送出来ていれば、データを消しこむ
                    removePw[nodeIdx].println((String)removeDataKeys.get(idx));
                    removePw[nodeIdx].flush();
                    String removeRet = removeBr[nodeIdx].readLine();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                for (int idx = 0; idx < removeSocket.length; idx++) {
                    if (removeSocket[idx] == null) break;
                    // コネクション切断

                    removePw[idx].println("-1");
                    removePw[idx].flush();
                    removePw[idx].println(ImdstDefine.imdstConnectExitRequest);
                    removePw[idx].flush();
                    removePw[idx].close();
                    removePw[idx] = null;

                    removeBr[idx].close();
                    removeBr[idx] = null;

                    removeSocket[idx].close();
                    removeSocket[idx] = null;
                }
            } catch (Exception ee) {
            }
        }
System.out.println("removeTargetData - end");
    }


    private void closeGetConnect() {
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

            // TODO:ここにconnectMap閉じる実装を入れる
            Set set = this.connectMap.keySet();
            Iterator iterator = set.iterator();

            while(iterator.hasNext()) {
                String key = (String)iterator.next();
                KeyNodeConnector keyNodeConnector = (KeyNodeConnector)connectMap.get(key);
                keyNodeConnector.println("-1");
                keyNodeConnector.flush();
                String endMsg = keyNodeConnector.readLine();
                keyNodeConnector.close();
            }
            this.connectMap = new HashMap();
        } catch(Exception e2) {
            // 無視
            logger.error(e2);
        }
    }
}