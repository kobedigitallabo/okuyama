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
/**
 * KeyNodeの監視を行うHelperクラス<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyNodeWatchHelper extends AbstractMasterManagerHelper {

    // ノードの監視サイクル時間(ミリ秒)
    private int checkCycle = 1000;

    private ArrayList mainNodeList = null;

    private ArrayList subNodeList = null;

    private String nodeStatusStr = null;
    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(KeyNodeWatchHelper.class);

    // 初期化メソッド定義
    public void initHelper(String initValue) {
    }

    // Jobメイン処理定義
    public String executeHelper(String optionParam) throws BatchException {
        logger.debug("KeyNodeWatchHelper - executeHelper - start");
        String ret = SUCCESS;
        String serverStopMarkerFileName = null;
        File serverStopMarkerFile = null;

        boolean serverRunning = true;

        Object[] shareKeys = null;
        ServerSocket serverSocket = null;

        String[] pingRet = null;

        try{
            while (serverRunning) {
                HashMap allNodeInfo = DataDispatcher.getAllDataNodeInfo();
                this.mainNodeList = (ArrayList)allNodeInfo.get("main");
                this.subNodeList = (ArrayList)allNodeInfo.get("sub");

                // ノード数分チェック
                for (int i = 0; i < mainNodeList.size(); i++) {
                    Thread.sleep(checkCycle);


                    // 停止ファイル関係チェック
                    if (StatusUtil.getStatus() == 1) {
                        serverRunning = false;
                        logger.info("KeyNodeWatchHelper - 状態異常です");
                    }

                    if (StatusUtil.getStatus() == 2) {
                        serverRunning = false;
                        logger.info("KeyNodeWatchHelper - 終了状態です");
                    }

                    serverStopMarkerFileName = super.getPropertiesValue("ServerStopFile");

                    serverStopMarkerFile = new File(new File(serverStopMarkerFileName).getAbsolutePath());
                    if (serverStopMarkerFile.exists()) {
                        serverRunning = false;
                        logger.info("KeyNodeWatchHelper - Server停止ファイルが存在します");
                        StatusUtil.setStatus(2);
                    }

                    // MainのMasterNodeの場合のみ実行
                    if (StatusUtil.isMainMasterNode()) {

                        // ノードチェック(メイン)
                        String nodeInfo = (String)mainNodeList.get(i);

                        String[] nodeDt = nodeInfo.split(":");

                        logger.info("************************************************************");
                        logger.info(nodeDt[0] + ":" +  nodeDt[1] + " Node Check Start");

                        pingRet = this.execNodePing(nodeDt[0], new Integer(nodeDt[1]).intValue(), logger);
                        if (pingRet[1] != null) this.nodeStatusStr = pingRet[1];

                        if(pingRet[0].equals("false")) {
                            // ノードダウン
                            logger.info(nodeDt[0] + ":" +  nodeDt[1] + " Node Check Dead");
                            super.setDeadNode(nodeInfo);
                            StatusUtil.setNodeStatusDt(nodeDt[0] + ":" +  nodeDt[1], "Node Check Dead");
                        } else if (!super.isNodeArrival(nodeInfo)) {

                            // ノードが復旧
                            logger.info("Node Name [" + nodeInfo +"] Reboot");

                            // Subノードが存在する場合はデータ復元、存在しない場合はそのまま起動
                            if (subNodeList != null && (subNodeList.get(i) != null)) {

                                // 停止していたノードが復帰した場合
                                // 停止中に登録予定であったデータを登録する
                                logger.info("Node Name [" + nodeInfo +"] Use Wait Start");

                                // ノードの使用中断を要求
                                super.setNodeWaitStatus(nodeInfo);
                                while(true) {
                                    // 使用停止まで待機
                                    if(super.getNodeUseStatus(nodeInfo) == 0) break;
                                    Thread.sleep(10);
                                }

                                // 該当ノードのデータをリカバーする場合は該当ノードと対になるノードの使用停止を要求し、
                                // 遂になるノードの使用数が0になってからリカバーを開始する。
                                if (subNodeList != null && i < subNodeList.size()) {
                                    super.setNodeWaitStatus((String)subNodeList.get(i));
                                    logger.info("Node Name [" + (String)subNodeList.get(i) +"] Use Wait Start");
                                    while(true) {
                                        // 使用停止まで待機
                                        if(super.getNodeUseStatus((String)subNodeList.get(i)) == 0) break;
                                        Thread.sleep(10);
                                    }
                                }

                                logger.info(nodeInfo + " - Recover Start");
                                StatusUtil.setNodeStatusDt(nodeInfo, "Recover Start");

                                // 復旧開始
                                if(super.nodeDataRecover(nodeInfo, (String)subNodeList.get(i), DataDispatcher.ruleInt, DataDispatcher.oldRules, i, logger)) {

                                    // リカバー成功
                                    // 該当ノードの復帰を登録
                                    logger.info(nodeInfo + " - Recover Success");
                                    StatusUtil.setNodeStatusDt(nodeInfo, "Recover Success");
                                    super.setArriveNode(nodeInfo);
                                } else {
                                    logger.info(nodeInfo + " - Recover Miss");
                                    StatusUtil.setNodeStatusDt(nodeInfo, "Recover Miss");
                                }

                                logger.info(nodeInfo + " - Recover End");

                                // 該当ノードの一時停止を解除
                                super.removeNodeWaitStatus((String)subNodeList.get(i));
                                super.removeNodeWaitStatus(nodeInfo);
                            } else {
                                // ノードの復旧を記録
                                super.setArriveNode(nodeInfo);
                            }
                        } else {
                            logger.info(nodeDt[0] + ":" +  nodeDt[1] + " Node Check Arrival");
                            logger.info(nodeDt[0] + ":" +  nodeDt[1] + " Server Status [" + this.nodeStatusStr + "]");
                            StatusUtil.setNodeStatusDt(nodeDt[0] + ":" +  nodeDt[1], "[" + this.nodeStatusStr + "]");
                        }
                        logger.info(nodeDt[0] + ":" +  nodeDt[1] + " Node Check End");


                        logger.info("------------------------------------------------------------");
                        // ノードチェック(Sub)
                        if (subNodeList != null && i < subNodeList.size()) {
                            String subNodeInfo = (String)subNodeList.get(i);
                            String[] subNodeDt = subNodeInfo.split(":");

                            logger.info(subNodeDt[0] + ":" +  subNodeDt[1] + " Sub Node Check Start");

                            pingRet = super.execNodePing(subNodeDt[0], new Integer(subNodeDt[1]).intValue(), logger);
                            if (pingRet[1] != null) this.nodeStatusStr = pingRet[1];

                            if(pingRet[0].equals("false")) {
                                // ノードダウン
                                logger.info(subNodeDt[0] + ":" +  subNodeDt[1] + " SubNode Check Dead");
                                super.setDeadNode(subNodeInfo);
                                StatusUtil.setNodeStatusDt(subNodeDt[0] + ":" +  subNodeDt[1], "SubNode Check Dead");
                            } else if (!super.isNodeArrival(subNodeInfo)) {

                                // 停止していたノードが復帰した場合
                                // 停止中に登録予定であったデータを登録する
                                logger.info("Node Name [" + subNodeInfo +"] Reboot");

                                logger.info("Node Name [" + subNodeInfo +"] Use Wait Start");

                                // ノードの使用中断を要求
                                super.setNodeWaitStatus(subNodeInfo);
                                while(true) {
                                    // 使用停止まで待機
                                    if(super.getNodeUseStatus(subNodeInfo) == 0) break;
                                    Thread.sleep(10);
                                }

                                // 該当ノードのデータをリカバーする場合は該当ノードと対になるノードの使用停止を要求し、
                                // 遂になるノードの使用数が0になってからリカバーを開始する。
                                super.setNodeWaitStatus(nodeInfo);
                                logger.info("Node Name [" + nodeInfo +"] Use Wait Start");
                                while(true) {
                                    // 使用停止まで待機
                                    if(super.getNodeUseStatus(nodeInfo) == 0) break;
                                    Thread.sleep(5);
                                }

                                logger.info(subNodeInfo + " - Recover Start");
                                StatusUtil.setNodeStatusDt(subNodeInfo, "Recover Start");
                                // 復旧開始
                                if(super.nodeDataRecover(subNodeInfo, nodeInfo, DataDispatcher.ruleInt, DataDispatcher.oldRules, i, logger)) {

                                    // リカバー成功
                                    // 該当ノードの復帰を登録
                                    logger.info(subNodeInfo + " - Recover Success");
                                    StatusUtil.setNodeStatusDt(subNodeInfo, "Recover Success");
                                    super.setArriveNode(subNodeInfo);
                                } else {
                                    logger.info(subNodeInfo + " - Recover Miss");
                                    StatusUtil.setNodeStatusDt(subNodeInfo, "Recover Miss");
                                }

                                logger.info(subNodeInfo + " - Recover End");

                                // 一時停止を解除
                                super.removeNodeWaitStatus(subNodeInfo);
                                super.removeNodeWaitStatus(nodeInfo);
                            } else {
                                logger.info(subNodeDt[0] + ":" +  subNodeDt[1] + " Sub Node Check Arrival");
                                logger.info(subNodeDt[0] + ":" +  subNodeDt[1] + " Server Status [" + this.nodeStatusStr + "]");
                                StatusUtil.setNodeStatusDt(subNodeDt[0] + ":" +  subNodeDt[1], "[" + this.nodeStatusStr + "]");
                            }
                            logger.info(subNodeDt[0] + ":" +  subNodeDt[1] + " Sub Node Check End");
                            logger.info("************************************************************");
                        }
                    }
                }
            }
        } catch(Exception e) {
            logger.error("KeyNodeWatchHelper - executeHelper - Error", e);
            throw new BatchException(e);
        }

        logger.debug("KeyNodeWatchHelper - executeHelper - end");
        return ret;
    }


    /**
     * Helper後処理.<br>
     *
     */
    public void endHelper() {
    }


}