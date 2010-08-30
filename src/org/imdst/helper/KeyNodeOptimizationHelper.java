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
import org.imdst.util.io.KeyNodeConnector;

/**
 * KeyNodeのデータを最適化するHelperクラス<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyNodeOptimizationHelper extends AbstractMasterManagerHelper {

    // ノードの監視サイクル時間(ミリ秒)
    private int checkCycle = 6000 * 3;


    private BufferedReader br = null;
    private KeyNodeConnector keyNodeConnector = null;
    private String searchNodeInfo = null;

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

                // 移動依頼がある場合のみ実行
                if (!super.isExecuteKeyNodeOptimization()) continue;

                HashMap allNodeInfo = DataDispatcher.getAllDataNodeInfo();
                ArrayList mainNodeList = (ArrayList)allNodeInfo.get("main");
                ArrayList subNodeList = (ArrayList)allNodeInfo.get("sub");
                ArrayList thirdNodeList = (ArrayList)allNodeInfo.get("third");

                // MainのMasterNodeの場合のみ実行
                if (StatusUtil.isMainMasterNode()) {

                    // ノード数分チェック
                    for (int i = 0; i < mainNodeList.size(); i++) {

                        // ノードチェック(メイン)
                        String nodeInfo = (String)mainNodeList.get(i);
                        String subNodeInfo = null;
                        String thirdNodeInfo = null;

                        if (subNodeList != null) subNodeInfo = (String)subNodeList.get(i);
                        if (thirdNodeList != null) thirdNodeInfo = (String)thirdNodeList.get(i);

                        logger.info("************************************************************");
                        logger.info(nodeInfo + " Optimization Start");

                        optimizeTargetKeys = null;
                        this.closeGetConnect();

                        String[] searchNodeDt = null;
                        String[] mainNodeDt = nodeInfo.split(":");
                        String[] subNodeDt = new String[2];
                        String[] thirdNodeDt = new String[2];

                        if (subNodeInfo != null) subNodeDt = subNodeInfo.split(":");
                        if (thirdNodeInfo != null) thirdNodeDt = thirdNodeInfo.split(":");


                        // 生存しているノードを元にデータを取得する
                        if (!super.isNodeArrival(nodeInfo)) {

                            if (subNodeInfo != null && super.isNodeArrival(subNodeInfo)) {

                                // Subを使用
                                searchNodeDt = subNodeInfo.split(":");
                            } else if (thirdNodeInfo != null && super.isNodeArrival(thirdNodeInfo)) {
                                
                                // Thirdを使用
                                searchNodeDt = thirdNodeInfo.split(":");
                            }
                        } else {

                            // Mainを使用
                            searchNodeDt = nodeInfo.split(":");
                        }


                        // ノードが生存している場合のみ実行
                        if (searchNodeDt != null) {

                            // 移動対象データを検索
                            this.searchTargetData(searchNodeDt[0], Integer.parseInt(searchNodeDt[1]), i);
                            // 移動完了後に削除するデータ保管用
                            removeDataKeys = new ArrayList(100000);

                            // 移動データ量に合わせて転送を繰り返す
                            while((optimizeTargetKeys = this.nextData()) != null) {

                                for (int idx = 0; idx < optimizeTargetKeys.length; idx++) {

                                    if (optimizeTargetKeys[idx] != null && !optimizeTargetKeys[idx].trim().equals("")) {
                                        this.sendTargetData(optimizeTargetKeys[idx]);
                                    }
                                }
                            }

                            // 検索コネクションを切断
                            this.closeGetConnect();
                            // 移動完了データを元ノードから削除
                            this.removeTargetData(mainNodeDt[0], mainNodeDt[1], subNodeDt[0], subNodeDt[1], thirdNodeDt[0], thirdNodeDt[1]);
                        }
                        logger.info(nodeInfo + " Optimization End");    
                        logger.info("************************************************************");
                    }
                }

                // データ移動依頼フラグを落とす
                super.executeKeyNodeOptimization(false);
            } catch(Exception e) {
                // 検索コネクションを切断
                this.closeGetConnect();
                logger.error("KeyNodeOptimizationHelper - executeHelper - Error", e);
            } finally {
                try {
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


    /**
     * 対象データを検索
     *
     */
    private void searchTargetData(String nodeName, int nodePort, int dataNodeMatchNo) throws BatchException {
        StringBuffer buf = null;

        try {

            // 使用開始してよいかをチェック
            StatusUtil.waitNodeUseStatus(nodeName+":"+nodePort, null, null);

            // 使用開始をマーク
            StatusUtil.addNodeUse(nodeName+":"+nodePort);
            this.searchNodeInfo = nodeName+":"+nodePort;

            if (this.keyNodeConnector != null) this.keyNodeConnector.close();
            this.keyNodeConnector = new KeyNodeConnector(nodeName, nodePort, nodeName+":"+nodePort);
            this.keyNodeConnector.connect();
            this.keyNodeConnector.setSoTimeout(ImdstDefine.recoverConnectionTimeout);

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
            this.keyNodeConnector.println(buf.toString());
            this.keyNodeConnector.flush();

        } catch(SocketException se) {
            super.setDeadNode(nodeName+":"+nodePort, 31, se);
            throw new BatchException(se);
        } catch(IOException ie) {
            super.setDeadNode(nodeName+":"+nodePort, 32, ie);
            throw new BatchException(ie);
        } catch(Exception e) {
            throw new BatchException(e);
        } 
    }


    private String[] nextData() throws BatchException {

        String[] ret = null;
        String line = null;

        try {

            while((line = this.keyNodeConnector.readLine()) != null) {

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
            throw new BatchException(se);
        } catch(Exception e) {
            throw new BatchException(e);
        }

        return ret;
    }


    /**
     * データ移動先にデータを送信.<br>
     * Main,Sub,Third全てを対象とする.<br>
     */
    private boolean sendTargetData(String targetDataLine) {
        boolean ret = false;

        KeyNodeConnector mainKeyNodeConnector = null;
        KeyNodeConnector subKeyNodeConnector = null;
        KeyNodeConnector thirdKeyNodeConnector = null;

        String[] targetDatas = null;
        String[] keyNodeInfo = null;

        String sendTargetNodeInfo = null;

        try {
            logger.info("sendTargetData - start");

            targetDatas = targetDataLine.split(ImdstDefine.keyHelperClientParamSep);

            // タグの場合はKey値からインデッス文字を外して振り分け先を決定
            if (targetDatas[0].equals("1")) {

                // 通常データ
                keyNodeInfo = DataDispatcher.dispatchKeyNode(targetDatas[1], false);
            } else if (targetDatas[0].equals("2")) {

                // タグ
                keyNodeInfo = DataDispatcher.dispatchKeyNode(targetDatas[1].substring(0, (targetDatas[1].lastIndexOf("=") +1)), false);
            }


            // コネクション確立

            // Main
            if (keyNodeInfo.length > 2) 
                mainKeyNodeConnector = this.createSendTargetConnection(keyNodeInfo[0], Integer.parseInt(keyNodeInfo[1]), keyNodeInfo[2]);

            // Sub
            if (keyNodeInfo.length > 5) 
                subKeyNodeConnector = this.createSendTargetConnection(keyNodeInfo[3], Integer.parseInt(keyNodeInfo[4]), keyNodeInfo[5]);

            // Third
            if (keyNodeInfo.length > 8) 
                thirdKeyNodeConnector = this.createSendTargetConnection(keyNodeInfo[6], Integer.parseInt(keyNodeInfo[7]), keyNodeInfo[8]);


            // 送信処理
            // Main
            if (mainKeyNodeConnector != null) {
                if (this.sendDataLine(mainKeyNodeConnector, targetDataLine)) ret = true;
            }

            // Sub
            if (subKeyNodeConnector != null) {
                if (this.sendDataLine(subKeyNodeConnector, targetDataLine)) ret = true;
            }

            // Third
            if (thirdKeyNodeConnector != null) {
                if (this.sendDataLine(thirdKeyNodeConnector, targetDataLine)) ret = true;
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e);
        } finally {

            // 使用終了をマーク
            if (keyNodeInfo.length > 2) 
                super.execNodeUseEnd(keyNodeInfo[2]);

            if (keyNodeInfo.length > 5) 
                super.execNodeUseEnd(keyNodeInfo[5]);

            if (keyNodeInfo.length > 8) 
                super.execNodeUseEnd(keyNodeInfo[8]);
        
            // 正常に転送出来ていれば、データを消しこむ
            if (ret) removeDataKeys.add(targetDatas[0] + ImdstDefine.keyHelperClientParamSep + targetDatas[1]);
        } 
        logger.info("sendTargetData - end");
        return ret;
    }


    /**
     * データ移動先ノードとのコネクションを確立
     *
     */
    private KeyNodeConnector createSendTargetConnection (String nodeName, int nodePort, String nodeFullName) throws Exception {
        KeyNodeConnector keyNodeConnector = null;
        try {

            // ノードが生存している場合のみ実行
            if (super.isNodeArrival(nodeFullName)) {

                if (this.connectMap.containsKey(nodeFullName)) {

                    keyNodeConnector = (KeyNodeConnector)this.connectMap.get(nodeFullName);
                } else {

                    keyNodeConnector = new KeyNodeConnector(nodeName, nodePort, nodeFullName);
                    keyNodeConnector.connect();
                    keyNodeConnector.println("30" + ImdstDefine.keyHelperClientParamSep + "true");
                    keyNodeConnector.flush();
                    this.connectMap.put(nodeFullName, keyNodeConnector);
                }
            } else {
                connectMap.remove(nodeFullName);
            }
        } catch (SocketException se) {
            super.setDeadNode(nodeFullName, 31, se);
        } catch (IOException ie) {
            super.setDeadNode(nodeFullName, 32, ie);
        } catch (Exception e) {
            throw e;
        }

        return keyNodeConnector;
    }


    /**
     * データを移動先データノードに送信
     *
     */
    private boolean sendDataLine (KeyNodeConnector keyNodeConnector, String dataLine) throws Exception {
        boolean ret = false;
        try {

            keyNodeConnector.println(dataLine);
            keyNodeConnector.flush();
            String sendRet = keyNodeConnector.readLine();
            if(sendRet != null && sendRet.equals("next")) ret = true;

        } catch (SocketException se) {
            super.setDeadNode(keyNodeConnector.getNodeFullName(), 33, se);
        } catch (IOException ie) {
            super.setDeadNode(keyNodeConnector.getNodeFullName(), 34, ie);
        } catch (Exception e) {
            throw e;
        }
        return ret;
    }


    /**
     *
     */
    private void removeTargetData(String mainNodeName, String mainNodePort, String subNodeName, String subNodePort, String thirdNodeName, String thirdNodePort) {

        PrintWriter[] removePw = new PrintWriter[3];
        BufferedReader[] removeBr = new BufferedReader[3];
        Socket[] removeSocket = new Socket[3];

        try {
            logger.info("removeTargetData - start");

            // 使用開始してよいかをチェック
            if (subNodeName != null && thirdNodeName != null) {
                StatusUtil.waitNodeUseStatus(mainNodeName+":"+mainNodePort,  subNodeName+":"+subNodePort, thirdNodeName+":"+thirdNodePort);
            } else if (subNodeName != null) {
                StatusUtil.waitNodeUseStatus(mainNodeName+":"+mainNodePort,  subNodeName+":"+subNodePort, null);
            } else {
                StatusUtil.waitNodeUseStatus(mainNodeName+":"+mainNodePort, null, null);
            }

            // 使用開始をマーク
            // Main
            StatusUtil.addNodeUse(mainNodeName+":"+mainNodePort);
            // Sub
            if (subNodeName != null)
                StatusUtil.addNodeUse(subNodeName+":"+subNodePort);
            // Third
            if (thirdNodeName != null) 
                StatusUtil.addNodeUse(thirdNodeName+":"+thirdNodePort);


            // Mainノード削除
            this.removeData(mainNodeName, Integer.parseInt(mainNodePort), mainNodeName+":"+mainNodePort);

            // Subノード削除
            if (subNodeName != null) 
                this.removeData(subNodeName, Integer.parseInt(subNodePort), subNodeName+":"+subNodePort);

            // Thirdノード削除
            if (thirdNodeName != null) 
                this.removeData(thirdNodeName, Integer.parseInt(thirdNodePort), thirdNodeName+":"+thirdNodePort);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {

                // 使用終了をマーク
                super.execNodeUseEnd(mainNodeName+":"+mainNodePort);
                
                if (subNodeName != null) 
                    super.execNodeUseEnd(subNodeName+":"+subNodePort);

                if (thirdNodeName != null) 
                    super.execNodeUseEnd(thirdNodeName+":"+thirdNodePort);
            } catch (Exception ee) {
            }
        }
        logger.info("removeTargetData - end");
    }


    /**
     * 転送完了後のデータを消しこむ.<br>
     * 1ノードが対象.<br>
     */
    private void removeData(String nodeName, int nodePort, String nodeFullName) {
        KeyNodeConnector keyNodeConnector = null;

        try {
            logger.info("removeData - start");

            keyNodeConnector = new KeyNodeConnector(nodeName, nodePort, nodeFullName);
            keyNodeConnector.connect();

            // 転送済みデータ削除用
            // 処理番号31
            keyNodeConnector.println("31" + ImdstDefine.keyHelperClientParamSep + "true");
            keyNodeConnector.flush();

            for (int idx = 0; idx < removeDataKeys.size(); idx++) {

                // 正常に転送出来ていれば、データを消しこむ
                keyNodeConnector.println((String)removeDataKeys.get(idx));
                keyNodeConnector.flush();
                String removeRet = keyNodeConnector.readLine();
            }

        } catch (SocketException se) {
            super.setDeadNode(keyNodeConnector.getNodeFullName(), 35, se);
        } catch (IOException ie) {
            super.setDeadNode(keyNodeConnector.getNodeFullName(), 36, ie);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {

                // コネクション切断
                keyNodeConnector.println("-1");
                keyNodeConnector.flush();
                keyNodeConnector.close();
            } catch (Exception ee) {
            }
        }
        logger.info("removeData - end");
    }


    /**
     *
     */  
    private void closeGetConnect() {
        try {

            // 使用終了をマーク
            if (this.searchNodeInfo != null) {
                super.execNodeUseEnd(this.searchNodeInfo);
                this.searchNodeInfo = null;
            }

            // コネクション切断
            if (this.keyNodeConnector != null) {
                this.keyNodeConnector.println(ImdstDefine.imdstConnectExitRequest);
                this.keyNodeConnector.flush();
                this.keyNodeConnector.close();
                this.keyNodeConnector = null;
            }


            // connectMap閉じる
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
        } finally {
        }
    }
}