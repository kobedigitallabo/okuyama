package org.imdst.util;

import java.util.*;
import java.io.*;
import java.util.concurrent.ConcurrentHashMap;


import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.batch.lang.BatchException;

/**
 * システム全般の稼動ステータス管理モジュール.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class StatusUtil {

    // 0:処理をしていない 1以上:処理中
    private static int masterManagerStatus = 0;

    // ノードのWait状態を管理
    private static HashMap nodeStatusMap = new HashMap();
    private static Object nodeSync = new Object();

    // ノードの死活状態を管理
    private static ConcurrentHashMap checkErrorMap = new ConcurrentHashMap(50, 40, 50);

    // ノードの使用状態を管理(数値でプラス、マイナスが格納)
    private static HashMap nodeExecMap = new HashMap();

    // ノードの最新復活時間を管理
    private static ConcurrentHashMap nodeRebootTimeMap = new ConcurrentHashMap(50, 40, 50);

    // ノードの最新の状態詳細を管理
    private static ConcurrentHashMap nodeStatusDtMap = new ConcurrentHashMap(50, 5, 5);
    

    private static String nowMemoryStatus = null;

    private static String nowCpuStatus = null;

    // 全体ステータス
    // 0:正常 1:異常 2:終了 3:一時停止
    private static int status = 0;
    // 全体メッセージ
    private static String msg = null;

    // 自身の情報を設定
    private static String myNodeInfo = null;

    // MainMasterNodeである場合はtrueとなる
    private static Boolean mainMasterNode = null;

    // SlaveMainMasterNodeの情報
    private static String slaveMainMasterNodeInfo = null;

    // 自身がチェックしなければいけないマスターノードのIP群
    private static String checkTargetMasterNodes = null;


    // Transactionの使用有無
    private static Boolean transactionMode = null;

    // TransactionNodeの情報
    private static String[] transactionInfo = null;


    /**
     * ノード使用状態の枠を初期化
     */
    public static void initNodeExecMap(String[] nodeInfos) {
        synchronized(nodeExecMap) {
            for (int i = 0; i < nodeInfos.length; i++) {
                if (!nodeExecMap.containsKey(nodeInfos[i])) {
                    nodeExecMap.put(nodeInfos[i], new Integer(0));
                }
            }
        }
    }


    /**
     * 全体ステータスを設定
     */
    public static void setStatus(int status) {
        StatusUtil.status = status;
    }

    /**
     * 全体ステータスを設定
     */
    public static void setStatusAndMessage(int status, String msg) {
        StatusUtil.status = status;
        StatusUtil.msg = msg;
    }


    /**
     * 全体ステータスを取得
     */
    public static int getStatus() {
        return StatusUtil.status;
    }

    /**
     * 全体ステータスメッセージを取得
     */
    public static String getStatusMessage() {
        return StatusUtil.msg;
    }

    /**
     * 自身の情報をセット
     */
    public static void setMyNodeInfo(String my) {
        myNodeInfo = my;
    }

    /**
     * 自身の情報を取得
     */
    public static String getMyNodeInfo() {
        return myNodeInfo;
    }



    /**
     * 自身がチェックしなければいけないMasterNodeを登録.<br>
     *
     * @param masterNodes
     */
    public static void setCheckTargetMasterNodes(String masterNodes) {
        checkTargetMasterNodes = masterNodes;
    }

    /**
     * 自身がチェックしなければいけないMasterNodeを取得.<br>
     *
     * @param masterNodes
     */
    public static String getCheckTargetMasterNodes() {
        return checkTargetMasterNodes;
    }


    /**
     * ノードの生存を確認
     *
     *
     * @param nodeInfo 確認対象のノード情報
     */
    public static boolean isNodeArrival(String nodeInfo) {
        if (checkErrorMap.containsKey(nodeInfo)) return false;
        return true;
    }

    /**
     * ノードの復帰を登録
     *
     *
     * @param nodeInfo 対象のノード情報
     */
    public static void setArriveNode(String nodeInfo) {
        checkErrorMap.remove(nodeInfo);
        setNodeRebootTime(nodeInfo, new Long(System.currentTimeMillis()));
    }

    /**
     * ノードの停止を登録
     *
     *
     * @param nodeInfo 対象のノード情報
     */
    public static void setDeadNode(String nodeInfo) {
        System.out.println("222222222222222222222222222222=[" + nodeInfo + "]");
        checkErrorMap.put(nodeInfo, new Boolean(false));
    }


    /**
     * ノードの最新起動時間を設定
     *
     * @param nodeInfo 対象のノード情報
     */
    public static void setNodeRebootTime(String nodeInfo, Long time) {
        nodeRebootTimeMap.put(nodeInfo, time);
    }

    /**
     * ノードの最新起動時間を取得
     * 起動時間が記録されていない(一度も再起動が発生していない)場合はnullが応答
     
     * @param nodeInfo 対象のノード情報
     */
    public static Long getNodeRebootTime(String nodeInfo) {
        return (Long)nodeRebootTimeMap.get(nodeInfo);
    }


    /**
     * ノードの使用一時停止状態を問い合わせ
     */
    public static boolean isWaitStatus(String nodeInfo) {
        if(nodeStatusMap.containsKey(nodeInfo)) return true;
        return false;
    }

    /**
     * ノードの使用一時停止を設定
     */
    public static void setWaitStatus(String nodeInfo) {
        synchronized(nodeSync) {
            nodeStatusMap.put(nodeInfo, new Boolean(true));
        }
    }

    /**
     * ノードの使用一時停止を解除
     */
    public static void removeWaitStatus(String nodeInfo) {
        synchronized(nodeSync) {
            nodeStatusMap.remove(nodeInfo);
        }
    }

    // 本メソッドを呼び出す場合は必ずinitNodeExecMapで初期化を行う必要がある
    public static void addNodeUse(String nodeInfo) {
        synchronized(nodeExecMap) {
            Integer cnt = (Integer)nodeExecMap.get(nodeInfo);
            cnt = cnt + 1;
            nodeExecMap.put(nodeInfo, cnt);
        }
    }

    // 本メソッドを呼び出す場合は必ずinitNodeExecMapで初期化を行う必要がある
    public static void endNodeUse(String nodeInfo) {
        synchronized(nodeExecMap) {
            Integer cnt = (Integer)nodeExecMap.get(nodeInfo);
            cnt = cnt - 1;
            nodeExecMap.put(nodeInfo, cnt);
        }
    }

    // 本メソッドを呼び出す場合は必ずinitNodeExecMapで初期化を行う必要がある
    public static int getNodeUseStatus(String nodeInfo) {
        synchronized(nodeExecMap) {
            return ((Integer)nodeExecMap.get(nodeInfo)).intValue();
        }
    }

    // データノードの最新詳細状態を格納
    public static void setNodeStatusDt(String nodeInfo, String dtText) {
        nodeStatusDtMap.put(nodeInfo, dtText);
    }

    // データノードの最新詳細状態を取得
    public static String getNodeStatusDt(String nodeInfo) {
        return (String)nodeStatusDtMap.get(nodeInfo);
    }


    public static void setNowMemoryStatus(String statusStr) {
        nowMemoryStatus = statusStr;
    }

    public static void setNowCpuStatus(String statusStr) {
        nowCpuStatus = statusStr;
    }

    public static String getNowMemoryStatus() {
        return nowMemoryStatus;
    }

    public static String getNowCpuStatus() {
        return  nowCpuStatus;
    }

    // MainMasterNodeかを設定
    public static void setMainMasterNode(boolean flg) {
        mainMasterNode = new Boolean(flg);
    }

    // MainMasterNodeかを判定する
    public static boolean isMainMasterNode() {
        while (mainMasterNode == null) {
            try {
                Thread.sleep(10);
            } catch(Exception e) {}
        }
        return mainMasterNode.booleanValue();
    }

    // SlaveMainMasterNodeの情報をセットする
    public static void setSlaveMasterNodes(String infos) {
        slaveMainMasterNodeInfo = infos;
    }

    // SlaveMainMasterNodeの情報を返す
    public static String getSlaveMasterNodes() {
        while (mainMasterNode == null) {
            try {
                Thread.sleep(10);
            } catch(Exception e) {}
        }
        return slaveMainMasterNodeInfo;
    }


    // Transaction設定が有効か設定
    public static void setTransactionMode(boolean flg) {
        transactionMode = new Boolean(flg);
    }

    // Transaction設定が有効か判定する
    public static boolean isTransactionMode() {
        while (transactionMode == null) {
            try {
                Thread.sleep(10);
            } catch(Exception e) {}
        }
        return transactionMode.booleanValue();
    }

    // TransactionNodeの情報をセットする
    public static void setTransactionNode(String[] info) {
        transactionInfo = info;
    }

    // TransactionNodの情報を返す
    public static String[] getTransactionNode() {
        while (transactionMode == null) {
            try {
                Thread.sleep(10);
            } catch(Exception e) {}
        }
        return transactionInfo;
    }

    public static boolean isStandby() {
        while(mainMasterNode == null || transactionMode == null) {
            try {
                Thread.sleep(50);
            } catch(Exception e) {}
        }
        return true;
    }


}