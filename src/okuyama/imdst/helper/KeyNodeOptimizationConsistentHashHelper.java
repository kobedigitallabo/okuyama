package okuyama.imdst.helper;

import java.io.*;
import java.util.*;
import java.net.*;

import okuyama.base.lang.BatchException;
import okuyama.base.job.AbstractHelper;
import okuyama.base.job.IJob;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.imdst.util.ImdstDefine;
import okuyama.imdst.util.DataDispatcher;
import okuyama.imdst.util.StatusUtil;
import okuyama.imdst.util.io.KeyNodeConnector;

/**
 * KeyNodeのデータを最適化するHelperクラス.<br>
 * ConsistentHash用.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyNodeOptimizationConsistentHashHelper extends AbstractMasterManagerHelper {

    // ノードの監視サイクル時間(ミリ秒)
    private int checkCycle = 10000 * 6;

    private KeyNodeConnector mainKeyNodeConnector = null;

    private KeyNodeConnector subKeyNodeConnector = null;

    private KeyNodeConnector thirdKeyNodeConnector = null;

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


        HashMap moveTargetData = null;

        boolean sendError = false;


        // Main
        String addMainDataNodeInfo = null;
        HashMap mainMoveTargetMap = null;
        String[] toMainDataNodeDt = null;
        Set mainSet = null;
        Iterator mainIterator = null;

        KeyNodeConnector toMainKeyNodeConnector = null;
        String toMainSendRet = null;

        String mainDataNodeStr = null;
        String[] mainDataNodeDetail = null;
        String mainRangStr = null;
        String mainTargetDataStr = null;

        ArrayList mainRemoveTargetDatas = null;

        // Sub
        String addSubDataNodeInfo = null;
        HashMap subMoveTargetMap = null;
        String[] toSubDataNodeDt = null;
        Set subSet = null;
        Iterator subIterator = null;

        KeyNodeConnector toSubKeyNodeConnector = null;
        String toSubSendRet = null;

        String subDataNodeStr = null;
        String[] subDataNodeDetail = null;
        String subRangStr = null;
        String subTargetDataStr = null;

        ArrayList subRemoveTargetDatas = null;


        // Third
        String addThirdDataNodeInfo = null;
        HashMap thirdMoveTargetMap = null;
        String[] toThirdDataNodeDt = null;
        Set thirdSet = null;
        Iterator thirdIterator = null;

        KeyNodeConnector toThirdKeyNodeConnector = null;
        String toThirdSendRet = null;

        String thirdDataNodeStr = null;
        String[] thirdDataNodeDetail = null;
        String thirdRangStr = null;
        String thirdTargetDataStr = null;

        ArrayList thirdRemoveTargetDatas = null;


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
                    Thread.sleep(checkCycle);

                    StringBuilder sendRequestBuf = new StringBuilder();

                    // 送信リクエスト文字列作成
                    // 処理番号28
                    sendRequestBuf.append("28");
                    sendRequestBuf.append(ImdstDefine.keyHelperClientParamSep);
                    sendRequestBuf.append("true");

                    if (moveTargetData != null) {
                        logger.info("The processing which moves the data which Datanode has to new Datanode is started.");
                        logger.info("Step - 1");
                        while (true) {
                            super.setNowNodeDataOptimization(true);
                            try {

                                // 全て初期化
                                sendError = false;

                                // Main
                                toMainKeyNodeConnector = null;
                                toMainSendRet = null;
                                mainSet = null;
                                mainIterator = null;
                                mainDataNodeStr = null;
                                mainDataNodeDetail = null;
                                mainRangStr = null;
                                mainTargetDataStr = null;
                                mainRemoveTargetDatas = new ArrayList();

                                // Sub
                                toSubKeyNodeConnector = null;
                                toSubSendRet = null;
                                subSet = null;
                                subIterator = null;
                                subDataNodeStr = null;
                                subDataNodeDetail = null;
                                subRangStr = null;
                                subTargetDataStr = null;
                                subRemoveTargetDatas = new ArrayList();

                                // Third
                                toThirdKeyNodeConnector = null;
                                toThirdSendRet = null;
                                thirdSet = null;
                                thirdIterator = null;
                                thirdDataNodeStr = null;
                                thirdDataNodeDetail = null;
                                thirdRangStr = null;
                                thirdTargetDataStr = null;
                                thirdRemoveTargetDatas = new ArrayList();

                                // データ移動先のメインデータノードに接続
                                addMainDataNodeInfo = (String)moveTargetData.get("tomain");
                                toMainDataNodeDt = addMainDataNodeInfo.split(":");
                                mainMoveTargetMap = (HashMap)moveTargetData.get("main");

                                addSubDataNodeInfo = (String)moveTargetData.get("tosub");
                                subMoveTargetMap = (HashMap)moveTargetData.get("sub");

                                addThirdDataNodeInfo = (String)moveTargetData.get("tothird");
                                thirdMoveTargetMap = (HashMap)moveTargetData.get("third");
                                logger.info("Step - 2");
                                // 使用開始してよいかをチェック
                                if (addSubDataNodeInfo != null && addThirdDataNodeInfo != null) {
                                    logger.info("Step - 3");
                                    StatusUtil.waitNodeUseStatus(addMainDataNodeInfo, addSubDataNodeInfo, addThirdDataNodeInfo);
                                    // 使用をマーク
                                    StatusUtil.addNodeUse(addMainDataNodeInfo);
                                    StatusUtil.addNodeUse(addSubDataNodeInfo);
                                    StatusUtil.addNodeUse(addThirdDataNodeInfo);
                                } else if (addSubDataNodeInfo != null) {
                                    logger.info("Step - 4");
                                    StatusUtil.waitNodeUseStatus(addMainDataNodeInfo, addSubDataNodeInfo, null);
                                    // 使用をマーク
                                    StatusUtil.addNodeUse(addMainDataNodeInfo);
                                    StatusUtil.addNodeUse(addSubDataNodeInfo);
                                } else {
                                    logger.info("Step - 5");
                                    StatusUtil.waitNodeUseStatus(addMainDataNodeInfo, null, null);
                                    // 使用をマーク
                                    StatusUtil.addNodeUse(addMainDataNodeInfo);
                                }


                                // データ移動先MainDataNode
                                // 移動データレンジMap
                                //System.out.println(addMainDataNodeInfo);
                                //System.out.println(mainMoveTargetMap);

                                if (super.isNodeArrival(addMainDataNodeInfo)) {
                                    logger.info("Step - 6");
                                    try {
                                        logger.info("Step - 7");
                                        toMainKeyNodeConnector = new KeyNodeConnector(toMainDataNodeDt[0], Integer.parseInt(toMainDataNodeDt[1]), toMainDataNodeDt[0]+":"+Integer.parseInt(toMainDataNodeDt[1]));
                                        toMainKeyNodeConnector.connect();
                                        toMainKeyNodeConnector.setSoTimeout(ImdstDefine.recoverConnectionTimeout);

                                        mainSet = mainMoveTargetMap.keySet();
                                        mainIterator = mainSet.iterator();

                                        // 移行先メインデータノードにデータ移行開始を送信
                                        toMainKeyNodeConnector.println(sendRequestBuf.toString());
                                        toMainKeyNodeConnector.flush();
                                        logger.info("Step - 8");
                                    } catch (Exception e) {
                                        // 使用停止を登録
                                        logger.info("Step Error - 9");
                                        super.setDeadNode(addMainDataNodeInfo, 37, e);
                                        toMainKeyNodeConnector.close();
                                        toMainKeyNodeConnector = null;
                                    }
                                }

                                // スレーブノード処理
                                // データ移動先SubDataNode
                                // 移動データレンジMap
                                //System.out.println(addSubDataNodeInfo);
                                //System.out.println(subMoveTargetMap);
                                logger.info("Step - 10");
                                if (addSubDataNodeInfo != null) {
                                    logger.info("Step - 11");
                                    if (super.isNodeArrival(addSubDataNodeInfo)) {
                                        logger.info("Step - 12");
                                        try {
                                            toSubDataNodeDt = addSubDataNodeInfo.split(":");
                                            toSubKeyNodeConnector = new KeyNodeConnector(toSubDataNodeDt[0], Integer.parseInt(toSubDataNodeDt[1]), toSubDataNodeDt[0]+":"+Integer.parseInt(toSubDataNodeDt[1]));
                                            toSubKeyNodeConnector.connect();
                                            toSubKeyNodeConnector.setSoTimeout(ImdstDefine.recoverConnectionTimeout);

                                            subSet = subMoveTargetMap.keySet();
                                            subIterator = subSet.iterator();

                                            // 移行先スレーブデータノードにデータ移行開始を送信
                                            toSubKeyNodeConnector.println(sendRequestBuf.toString());
                                            toSubKeyNodeConnector.flush();
                                            logger.info("Step - 13");
                                        } catch (Exception e) {
                                            logger.info("Step Error - 14");
                                            // 使用停止を登録
                                            super.setDeadNode(addSubDataNodeInfo, 38, e);
                                            toSubKeyNodeConnector.close();
                                            toSubKeyNodeConnector = null;
                                        }
                                    }
                                }

                                // サードノード処理
                                // データ移動先ThirdDataNode
                                // 移動データレンジMap
                                //System.out.println(addThirdDataNodeInfo);
                                //System.out.println(thirdMoveTargetMap);
                                logger.info("Step - 15");
                                if (addThirdDataNodeInfo != null) {
                                    logger.info("Step - 16");
                                    if (super.isNodeArrival(addThirdDataNodeInfo)) {
                                        logger.info("Step - 17");
                                        try {
                                            toThirdDataNodeDt = addThirdDataNodeInfo.split(":");
                                            toThirdKeyNodeConnector = new KeyNodeConnector(toThirdDataNodeDt[0], Integer.parseInt(toThirdDataNodeDt[1]), toThirdDataNodeDt[0]+":"+Integer.parseInt(toThirdDataNodeDt[1]));
                                            toThirdKeyNodeConnector.connect();
                                            toThirdKeyNodeConnector.setSoTimeout(ImdstDefine.recoverConnectionTimeout);

                                            thirdSet = thirdMoveTargetMap.keySet();
                                            thirdIterator = thirdSet.iterator();

                                            // 移行先スレーブデータノードにデータ移行開始を送信
                                            toThirdKeyNodeConnector.println(sendRequestBuf.toString());
                                            toThirdKeyNodeConnector.flush();
                                            logger.info("Step - 18");
                                        } catch (Exception e) {
                                            logger.info("Step Error - 19");
                                            // 使用停止を登録
                                            super.setDeadNode(addThirdDataNodeInfo, 39, e);
                                            toThirdKeyNodeConnector.close();
                                            toThirdKeyNodeConnector = null;
                                        }
                                    }
                                }


                                // 全ての移動対象(移動元のデータ)のノードを処理
                                // 対象データノード1ノードづつ処理
                                logger.info("Step - 20");
                                if (mainIterator == null) {
                                    logger.info("Step - 21");
                                    logger.error("KeyNodeOptimizationConsistentHashHelper - [mainIterator == null] MainDataNode Down!! Please Check [" + addMainDataNodeInfo + "]");
                                    if (toMainKeyNodeConnector != null) toMainKeyNodeConnector.close();
                                    if (toSubKeyNodeConnector != null) toSubKeyNodeConnector.close();
                                    if (toThirdKeyNodeConnector != null) toThirdKeyNodeConnector.close();

                                    Thread.sleep(5000);
                                    continue;
                                }

                                logger.info("Step - 22");
                                while(mainIterator.hasNext()) {
                                    logger.info("Step - 23");
                                    // 移動データレンジ文字列
                                    // System.out.println(mainRangStr);

                                    // Mainノード処理
                                    // キー値を取り出し
                                    mainDataNodeStr = (String)mainIterator.next();
                                    mainDataNodeDetail = mainDataNodeStr.split(":");
                                    // Rangの文字列を取り出し
                                    mainRangStr = (String)mainMoveTargetMap.get(mainDataNodeStr);


                                    // Subノード処理
                                    if (subIterator != null) {
                                        logger.info("Step - 24");
                                        // 移動データレンジ文字列
                                        // System.out.println(subRangStr);

                                        // キー値を取り出し
                                        subDataNodeStr = (String)subIterator.next();
                                        subDataNodeDetail = subDataNodeStr.split(":");
                                        // Rangの文字列を取り出し
                                        subRangStr = (String)subMoveTargetMap.get(subDataNodeStr);
                                    }

                                    logger.info("Step - 25");
                                    // Thirdノード処理
                                    if (thirdIterator != null) {
                                        logger.info("Step - 26");
                                        // 移動データレンジ文字列
                                        // System.out.println(thirdRangStr);

                                        // キー値を取り出し
                                        thirdDataNodeStr = (String)thirdIterator.next();
                                        thirdDataNodeDetail = thirdDataNodeStr.split(":");
                                        // Rangの文字列を取り出し
                                        thirdRangStr = (String)thirdMoveTargetMap.get(thirdDataNodeStr);
                                    }

                                    logger.info("Step - 27");
                                    // 使用開始してよいかをチェック
                                    if (subIterator != null && thirdIterator != null) {
                                        logger.info("Step - 28");
                                        StatusUtil.waitNodeUseStatus(mainDataNodeStr, subDataNodeStr, thirdDataNodeStr);
                                        // 使用をマーク
                                        StatusUtil.addNodeUse(mainDataNodeStr);
                                        StatusUtil.addNodeUse(subDataNodeStr);
                                        StatusUtil.addNodeUse(thirdDataNodeStr);
                                    } else if (addSubDataNodeInfo != null) {

                                        logger.info("Step - 29");
                                        StatusUtil.waitNodeUseStatus(mainDataNodeStr, subDataNodeStr, null);
                                        // 使用をマーク
                                        StatusUtil.addNodeUse(mainDataNodeStr);
                                        StatusUtil.addNodeUse(subDataNodeStr);
                                    } else {

                                        logger.info("Step - 30");
                                        StatusUtil.waitNodeUseStatus(mainDataNodeStr, null, null);
                                        // 使用をマーク
                                        StatusUtil.addNodeUse(mainDataNodeStr);
                                    }


                                    logger.info("Step - 31");
                                    // 対象ノードからデータ取り出し
                                    this.getTargetData(1, mainDataNodeDetail[0], Integer.parseInt(mainDataNodeDetail[1]), mainRangStr);
                                    mainRemoveTargetDatas.add(new String(mainDataNodeDetail[0] + "#" + mainDataNodeDetail[1] + "#" + mainRangStr));
                                    logger.info("Step - 32");

                                    // Subノード処理
                                    if (subIterator != null) {

                                        logger.info("Step - 33");
                                        // 対象ノードからデータ取り出し
                                        this.getTargetData(2, subDataNodeDetail[0], Integer.parseInt(subDataNodeDetail[1]), subRangStr);
                                        subRemoveTargetDatas.add(new String(subDataNodeDetail[0] + "#" + subDataNodeDetail[1] + "#" + subRangStr));
                                    }

                                    // Thirdノード処理
                                    if (thirdIterator != null) {

                                        logger.info("Step - 34");
                                        // 対象ノードからデータ取り出し
                                        this.getTargetData(3, thirdDataNodeDetail[0], Integer.parseInt(thirdDataNodeDetail[1]), thirdRangStr);
                                        thirdRemoveTargetDatas.add(new String(thirdDataNodeDetail[0] + "#" + thirdDataNodeDetail[1] + "#" + thirdRangStr));
                                    }


                                    // 対象のデータを順次対象のノードに移動
                                    // Main
                                    while((mainTargetDataStr = this.nextData(1, mainDataNodeStr)) != null) {

                                        logger.info("Step - 35");
                                        if (toMainKeyNodeConnector != null) {

                                            logger.info("Step - mainDataNodeStr[" + mainDataNodeStr + "] 36");
                                            toMainKeyNodeConnector.println(mainTargetDataStr);
                                            toMainKeyNodeConnector.flush();
                                            toMainSendRet = toMainKeyNodeConnector.readLine();

                                            logger.info("Step - mainDataNodeStr[" + mainDataNodeStr + "] 37");
                                            // エラーなら移行中止
                                            if (toMainSendRet == null || !toMainSendRet.equals("next")) { 

                                                logger.info("Step - mainDataNodeStr[" + mainDataNodeStr + "] 38");
                                                super.setDeadNode(addMainDataNodeInfo, 42, new Exception(addMainDataNodeInfo + "=SendError"));
                                                sendError = true;
                                                break;
                                            }
                                        }
                                    }


                                    logger.info("Step - 39");
                                    // Sub
                                    if (toSubKeyNodeConnector != null) {

                                        logger.info("Step - 40");
                                        while((subTargetDataStr = this.nextData(2, subDataNodeStr)) != null) {

                                            logger.info("Step - subDataNodeStr[" + subDataNodeStr + "] 41");
                                            toSubKeyNodeConnector.println(subTargetDataStr);
                                            toSubKeyNodeConnector.flush();
                                            toSubSendRet = toSubKeyNodeConnector.readLine();
                                            logger.info("Step - subDataNodeStr[" + subDataNodeStr + "] 42");
                                            // エラーなら移行中止
                                            if (toSubSendRet == null || !toSubSendRet.equals("next")) {

                                                logger.info("Step - subDataNodeStr[" + subDataNodeStr + "] 43");
                                                super.setDeadNode(addSubDataNodeInfo, 43, new Exception(addSubDataNodeInfo + "=SendError"));
                                                sendError = true;
                                                break;
                                            }
                                        }
                                    }

                                    logger.info("Step - 44");
                                    // Third
                                    if (toThirdKeyNodeConnector != null) {

                                    logger.info("Step - 45");
                                        while ((thirdTargetDataStr = this.nextData(3, thirdDataNodeStr)) != null) {

                                            logger.info("Step - thirdDataNodeStr[" + thirdDataNodeStr + "] 46");
                                            toThirdKeyNodeConnector.println(thirdTargetDataStr);
                                            toThirdKeyNodeConnector.flush();
                                            toThirdSendRet = toThirdKeyNodeConnector.readLine();

                                            logger.info("Step - thirdDataNodeStr[" + thirdDataNodeStr + "] 47");
                                            // エラーなら移行中止
                                            if (toThirdSendRet == null || !toThirdSendRet.equals("next")) {

                                                logger.info("Step - thirdDataNodeStr[" + thirdDataNodeStr + "] 48");
                                                super.setDeadNode(addThirdDataNodeInfo, 44, new Exception(addThirdDataNodeInfo + "=SendError"));
                                                sendError = true;
                                                break;
                                            }
                                        }
                                    }

                                    logger.info("Step - 49");
                                    // 転送元を切断
                                    this.closeConnect(1);
                                    super.execNodeUseEnd(mainDataNodeStr);

                                    if (subIterator != null) {
                                        logger.info("Step - 50");
                                        super.execNodeUseEnd(subDataNodeStr);
                                        this.closeConnect(2);
                                    }

                                    if (thirdIterator != null)  {

                                        logger.info("Step - 51");
                                        super.execNodeUseEnd(thirdDataNodeStr);
                                        this.closeConnect(3);
                                    }

                                    logger.info("Step - 52");
                                    if (sendError == true) break;
                                }

                                logger.info("Step - 53");
                                // 全てのデータの移行が完了
                                // 転送先に終了を通知
                                // Main
                                // 使用終了をマーク
                                super.execNodeUseEnd(addMainDataNodeInfo);
                                toMainKeyNodeConnector.println("-1");
                                toMainKeyNodeConnector.flush();
                                toMainKeyNodeConnector.println(ImdstDefine.imdstConnectExitRequest);
                                toMainKeyNodeConnector.flush();
                                toMainKeyNodeConnector.close();
                                toMainKeyNodeConnector.close();


                                logger.info("Step - 54");
                                // Sub
                                if (subIterator != null) {

                                    logger.info("Step - 55");
                                    // 使用終了をマーク
                                    super.execNodeUseEnd(addSubDataNodeInfo);

                                    toSubKeyNodeConnector.println("-1");
                                    toSubKeyNodeConnector.flush();
                                    toSubKeyNodeConnector.println(ImdstDefine.imdstConnectExitRequest);
                                    toSubKeyNodeConnector.flush();
                                    toSubKeyNodeConnector.close();
                                }

                                logger.info("Step - 56");
                                // Third
                                if (thirdIterator != null) {

                                    logger.info("Step - 57");
                                    // 使用終了をマーク
                                    super.execNodeUseEnd(addThirdDataNodeInfo);

                                    toThirdKeyNodeConnector.println("-1");
                                    toThirdKeyNodeConnector.flush();
                                    toThirdKeyNodeConnector.println(ImdstDefine.imdstConnectExitRequest);
                                    toThirdKeyNodeConnector.flush();
                                    toThirdKeyNodeConnector.close();
                                }

                                logger.info("Step - 58");
                                // 移動もとのデータを消す処理をここに追加
                                if (sendError == false) {

                                    logger.info("Step - 59");
                                    // 転送が正しく完了した場合のみ処理開始

                                    // Main
                                    for (int mainIdx = 0; mainIdx < mainRemoveTargetDatas.size(); mainIdx++) {
                                        logger.info("Step - 60");
                                        String mainRemoveHostDtStr = (String)mainRemoveTargetDatas.get(mainIdx);
                                        String[] mainRemoveHostDt = mainRemoveHostDtStr.split("#");
                                        if(!this.removeTargetData(mainRemoveHostDt[0], Integer.parseInt(mainRemoveHostDt[1]), mainRemoveHostDt[2])) {
                                            logger.info("Step - 61");
                                            logger.error("KeyNodeOptimizationConsistentHashHelper - removeTargetData - Error target=[" + mainRemoveHostDt[0] + ":" + mainRemoveHostDt[1] + " Range[" + mainRemoveHostDt[2] + "]");
                                        }
                                    }


                                    logger.info("Step - 62");
                                    // Sub
                                    if (subIterator != null) {

                                        logger.info("Step - 63");
                                        for (int subIdx = 0; subIdx < subRemoveTargetDatas.size(); subIdx++) {

                                            logger.info("Step - 64");
                                            String subRemoveHostDtStr = (String)subRemoveTargetDatas.get(subIdx);
                                            String[] subRemoveHostDt = subRemoveHostDtStr.split("#");
                                            if(!this.removeTargetData(subRemoveHostDt[0], Integer.parseInt(subRemoveHostDt[1]), subRemoveHostDt[2])) {
                                                logger.info("Step - 65");
                                                logger.error("KeyNodeOptimizationConsistentHashHelper - removeTargetData - Error target=[" + subRemoveHostDt[0] + ":" + subRemoveHostDt[1] + " Range[" + subRemoveHostDt[2] + "]");
                                            }
                                        }
                                    }


                                    logger.info("Step - 66");
                                    // Third
                                    if (thirdIterator != null) {

                                        logger.info("Step - 67");
                                        for (int thirdIdx = 0; thirdIdx < thirdRemoveTargetDatas.size(); thirdIdx++) {

                                            logger.info("Step - 68");
                                            String thirdRemoveHostDtStr = (String)thirdRemoveTargetDatas.get(thirdIdx);
                                            String[] thirdRemoveHostDt = thirdRemoveHostDtStr.split("#");
                                            if(!this.removeTargetData(thirdRemoveHostDt[0], Integer.parseInt(thirdRemoveHostDt[1]), thirdRemoveHostDt[2])) {
                                                logger.info("Step - 69");
                                                logger.error("KeyNodeOptimizationConsistentHashHelper - removeTargetData - Error target=[" + thirdRemoveHostDt[0] + ":" + thirdRemoveHostDt[1] + " Range[" + thirdRemoveHostDt[2] + "]");
                                            }
                                        }
                                    }

                                    logger.info("Step - 70");
                                    // メモリ上から依頼を消す
                                    super.removeConsistentHashMoveData();
                                    logger.info("Step - 71");
                                    super.setNowNodeDataOptimization(false);
                                    logger.info("Step - 72");
                                }

                                logger.info("Step - 73");
                                super.finishDynamicDataRemoveProcess();
                                logger.info("The processing which moves the data which Datanode has to new Datanode was completed.");
                                break;
                            } catch (Exception e) {
                                super.setNowNodeDataOptimization(false);
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

        logger.debug("KeyNodeOptimizationConsistentHashHelper - executeHelper - end");
        return ret;
    }


    /**
     * Helper後処理.<br>
     *
     */
    public void endHelper() {
    }



    /**
     *
     *
     * @param target
     * @param nodeName
     * @param nodePort
     * @param rangStr
     * @throw BatchException
     */
    private void getTargetData(int target, String nodeName, int nodePort, String rangStr) throws BatchException {
        StringBuilder buf = null;
        KeyNodeConnector keyNodeConnector = null;
        logger.info("getTargetData - target[" + target + "] nodeName[" + nodeName + "] nodePort[" + nodePort + "] rangStr[" + rangStr + "] - Start");
        try {
            if (!super.isNodeArrival(nodeName + ":" + nodePort)) return ;
            keyNodeConnector = new KeyNodeConnector(nodeName, nodePort, nodeName+":"+nodePort);
            keyNodeConnector.connect();
            keyNodeConnector.setSoTimeout(ImdstDefine.recoverConnectionTimeout);

            // 移動元からデータ読み込み
            buf = new StringBuilder();
            // 処理番号27
            buf.append("27");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append("true");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append(rangStr);

            // 送信
            keyNodeConnector.println(buf.toString());
            keyNodeConnector.flush();

            if (target == 1) {

                this.mainKeyNodeConnector = keyNodeConnector;
            } else if (target == 2) {

                this.subKeyNodeConnector = keyNodeConnector;
            } else if (target == 3) {

                this.thirdKeyNodeConnector = keyNodeConnector;
            }
            logger.info("getTargetData - target[" + target + "] nodeName[" + nodeName + "] nodePort[" + nodePort + "] rangStr[" + rangStr + "] - End"); 
        } catch (SocketException se) {

            logger.info("getTargetData - Error - SocketException"); 
            super.setDeadNode(nodeName + ":" + nodePort, 39, se);
        } catch (IOException ie) {

            logger.info("getTargetData - Error - IOException");
            super.setDeadNode(nodeName + ":" + nodePort, 40, ie);
        } catch(Exception e) {

            logger.info("getTargetData - Error - Exception");
            throw new BatchException(e);
        } 
    }


    /**
     *
     * @param target
     * @return String
     * @throw BatchException
     */
    private String nextData(int target, String nodeInfo) throws BatchException {
        String ret = null;
        String line = null;

        KeyNodeConnector keyNodeConnector = null;

        try {
            if (target == 1) {

                keyNodeConnector = this.mainKeyNodeConnector;
            } else if (target == 2) {

                keyNodeConnector = this.subKeyNodeConnector;
            } else if (target == 3) {

                keyNodeConnector = this.thirdKeyNodeConnector;
            }

            if (keyNodeConnector == null) return null;

            while((line = keyNodeConnector.readLine()) != null) {

                if (line.length() > 0) {
                    if (line.length() == 2 && line.equals("-1")) {

                        break;
                    } else {

                        ret = line;
                        break;
                    }
                }
            }
        } catch (SocketException se) {
            super.setDeadNode(nodeInfo, 43, se);
        } catch (IOException ie) {
            super.setDeadNode(nodeInfo, 44, ie);
        } catch(Exception e) {
            throw new BatchException(e);
        }
        return ret;
    }


    /**
     *
     *
     * @param target
     * @param nodeName
     * @param nodePort
     * @param rangStr
     * @throw BatchException
     */
    private boolean removeTargetData(String nodeName, int nodePort, String rangStr) throws BatchException {
        StringBuilder buf = null;
        KeyNodeConnector keyNodeConnector = null;
        String removeRet = null;

        boolean ret = false;
        try {
            keyNodeConnector = new KeyNodeConnector(nodeName, nodePort, nodeName+":"+nodePort);
            keyNodeConnector.connect();
            keyNodeConnector.setSoTimeout(ImdstDefine.recoverConnectionTimeout);

            // 移動元からデータ削除
            buf = new StringBuilder();
            // 処理番号29
            buf.append("29");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append("true");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append(rangStr);

            // 送信
            keyNodeConnector.println(buf.toString());
            keyNodeConnector.flush();

            removeRet = keyNodeConnector.readLine();

            if (removeRet != null && removeRet.equals("-1")) ret = true;

        } catch(Exception e) {
            logger.error("KeyNodeOptimizationConsistentHashHelper - removeTargetData - Error " + e);
        } finally {
            try {
                // コネクション切断
                if (keyNodeConnector != null) {
                    keyNodeConnector.println(ImdstDefine.imdstConnectExitRequest);
                    keyNodeConnector.flush();
                    keyNodeConnector.close();
                    keyNodeConnector = null;
                }
            } catch(Exception ee) {
            }
        }
        return ret;
    }


    /**
     *
     * @param target
     */
    private void closeConnect(int target) {
        KeyNodeConnector keyNodeConnector = null;

        try {

            if (target == 1) {

                keyNodeConnector = this.mainKeyNodeConnector;
            } else if (target == 2) {

                keyNodeConnector = this.subKeyNodeConnector;
            } else if (target == 3) {

                keyNodeConnector = this.thirdKeyNodeConnector;
            }


            // コネクション切断
            if (keyNodeConnector != null) {
                keyNodeConnector.println(ImdstDefine.imdstConnectExitRequest);
                keyNodeConnector.flush();
                keyNodeConnector.close();
                keyNodeConnector = null;
            }


            if (target == 1) {

                this.mainKeyNodeConnector = keyNodeConnector;
            } else if (target == 2) {

                this.subKeyNodeConnector = keyNodeConnector;
            } else if (target == 3) {

                this.thirdKeyNodeConnector = keyNodeConnector;
            }
        } catch(Exception e2) {
            // 無視
            logger.error(e2);
        }
    }
}