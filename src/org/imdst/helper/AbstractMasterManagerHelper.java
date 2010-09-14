package org.imdst.helper;

import java.io.*;
import java.util.*;
import java.net.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.batch.lang.BatchException;
import org.batch.job.AbstractHelper;
import org.batch.job.IJob;
import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.imdst.util.KeyMapManager;
import org.imdst.util.ImdstDefine;
import org.imdst.util.DataDispatcher;
import org.imdst.util.StatusUtil;
import org.imdst.util.io.KeyNodeConnector;

/**
 * MasterNodeのメイン実行部分<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
abstract public class AbstractMasterManagerHelper extends AbstractHelper {


    private static AtomicInteger connPoolCount = new AtomicInteger(0);

    private int nowSt = 0;

    private static ConcurrentHashMap allConnectionMap = new ConcurrentHashMap(40, 30, 64);

    protected static ConcurrentHashMap keyNodeConnectPool = new ConcurrentHashMap(1024, 1000, 512);

    private static HashMap moveData4ConsistentHash = null;

    private static boolean executeKeyNodeOptimizationFlg = false;

    private static boolean nowNodeDataOptimization = false;

    /**
     * ノードの生存を確認
     *
     *
     * @param nodeInfo 確認対象のノード情報
     */
    protected boolean isNodeArrival(String nodeInfo) {
        return StatusUtil.isNodeArrival(nodeInfo);
    }

    /**
     * ノードの停止を登録
     *
     *
     * @param nodeInfo 対象のノード情報
     */
    protected void setDeadNode(String nodeInfo, int setPoint, Throwable te) {

        //if (te != null) {
        //    te.printStackTrace();
        //}

        // コネクションキャッシュが存在する場合は削除
        if (keyNodeConnectPool.containsKey(nodeInfo)) {
            ((ArrayBlockingQueue)keyNodeConnectPool.get(nodeInfo)).clear();
        }

        // MainMasterNodeの場合のみ設定される
        if (StatusUtil.isMainMasterNode()) {
            if (StatusUtil.isNodeArrival(nodeInfo)) {
                StatusUtil.setDeadNode(nodeInfo);

                // 対象のSlaveMasterNode全てに依頼
                String slaves = StatusUtil.getSlaveMasterNodes();

                if (slaves != null && !slaves.trim().equals("")) {
                    String[] slaveList = slaves.split(",");
                    int execCounter = 0;

                    // MasterNodeへの伝搬は失敗しても2回試す
                    while (execCounter < 2) {
                        // 1ノードづつ実行
                        for (int i = 0; i < slaveList.length; i++) {

                            if (slaveList[i] == null) continue;

                            Socket socket = null;
                            PrintWriter pw = null;
                            BufferedReader br = null;

                            try {

                                // Slaveノード名とポートに分解
                                String[] slaveNodeDt = slaveList[i].split(":");

                                InetSocketAddress inetAddr = new InetSocketAddress(slaveNodeDt[0], Integer.parseInt(slaveNodeDt[1]));
                                socket = new Socket();
                                socket.connect(inetAddr, ImdstDefine.nodeConnectionOpenShortTimeout);
                                socket.setSoTimeout(ImdstDefine.nodeConnectionTimeout);

                                pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), 
                                                                                                                ImdstDefine.keyHelperClientParamEncoding)));
                                br = new BufferedReader(new InputStreamReader(socket.getInputStream(), 
                                                                                                ImdstDefine.keyHelperClientParamEncoding));

                                // 文字列バッファ初期化
                                StringBuffer serverRequestBuf = new StringBuffer();

                                // 処理番号連結
                                serverRequestBuf.append("95");
                                // セパレータ連結
                                serverRequestBuf.append(ImdstDefine.keyHelperClientParamSep);
                                // 障害ノード名連結
                                serverRequestBuf.append(nodeInfo);

                                // サーバ送信
                                pw.println(serverRequestBuf.toString());
                                pw.flush();

                                // サーバから結果受け取り
                                String serverRetStr = br.readLine();

                                String[] serverRet = serverRetStr.split(ImdstDefine.keyHelperClientParamSep);

                                // 処理の妥当性確認
                                if (serverRet[0].equals("95")) {
                                    if (!serverRet[1].equals("true")) {
                                        // TODO:復帰登録失敗
                                        // 異常事態だが、稼動していないことも考えられるので、
                                        // 無視する
                                        //System.out.println("Slave Master Node setDeadNode Error [" + slaveList[i] + "]");
                                    } else {
                                        slaveList[i] = null;
                                    }
                                }
                            } catch(Exception e) {

                                // TODO:復帰登録失敗
                                // 異常事態だが、稼動していないことも考えられるので、
                                // 無視する
                                //System.out.println("Slave Master Node setArriveNode Error [" + slaveList[i] + "]");
                                //e.printStackTrace();

                            } finally {
                                try {
                                    if (pw != null) {
                                        // 接続切断を通知
                                        pw.println(ImdstDefine.imdstConnectExitRequest);
                                        pw.flush();

                                        pw.close();
                                        pw = null;
                                    }

                                    if (br != null) {
                                        br.close();
                                        br = null;
                                    }

                                    if (socket != null) {
                                        socket.close();
                                        socket = null;
                                    }
                                } catch(Exception e2) {
                                    // 無視
                                }
                            }
                        }
                        execCounter++;
                    }
                }
            }
        }
    }


    /**
     * ノードの復帰を登録
     *
     * @param nodeInfo 対象のノード情報
     */
    protected void setArriveNode(String nodeInfo) {
        StatusUtil.setArriveNode(nodeInfo);

        // MainのMasterNodeの場合のみ実行
        // SlaveのMasterNodeにもノードの復帰登録を依頼
        if (StatusUtil.isMainMasterNode()) {

            // 対象のSlaveMasterNode全てに依頼
            String slaves = StatusUtil.getSlaveMasterNodes();

            if (slaves != null && !slaves.trim().equals("")) {
                String[] slaveList = slaves.split(",");

                // 1ノードづつ実行
                for (int i = 0; i < slaveList.length; i++) {

                    Socket socket = null;
                    PrintWriter pw = null;
                    BufferedReader br = null;

                    try {

                        // Slaveノード名とポートに分解
                        String[] slaveNodeDt = slaveList[i].split(":");
                        InetSocketAddress inetAddr = new InetSocketAddress(slaveNodeDt[0], Integer.parseInt(slaveNodeDt[1]));
                        socket = new Socket();
                        socket.connect(inetAddr, ImdstDefine.nodeConnectionOpenTimeout);
                        socket.setSoTimeout(ImdstDefine.nodeConnectionTimeout);

                        pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), 
                                                                                                        ImdstDefine.keyHelperClientParamEncoding)));
                        br = new BufferedReader(new InputStreamReader(socket.getInputStream(), 
                                                                                        ImdstDefine.keyHelperClientParamEncoding));

                        // 文字列バッファ初期化
                        StringBuffer serverRequestBuf = new StringBuffer();

                        // 処理番号連結
                        serverRequestBuf.append("92");
                        // セパレータ連結
                        serverRequestBuf.append(ImdstDefine.keyHelperClientParamSep);

                        // 停止ノード名連結
                        serverRequestBuf.append(nodeInfo);

                        // サーバ送信
                        pw.println(serverRequestBuf.toString());
                        pw.flush();

                        // サーバから結果受け取り
                        String serverRetStr = br.readLine();

                        String[] serverRet = serverRetStr.split(ImdstDefine.keyHelperClientParamSep);

                        // 処理の妥当性確認
                        if (serverRet[0].equals("92")) {
                            if (!serverRet[1].equals("true")) {
                                // TODO:復帰登録失敗
                                // 異常事態だが、稼動していないことも考えられるので、
                                // 無視する
                                //System.out.println("Slave Master Node setArriveNode Error [" + slaveList[i] + "]");
                            } 
                        }
                    } catch(Exception e) {

                        // TODO:復帰登録失敗
                        // 異常事態だが、稼動していないことも考えられるので、
                        // 無視する
                        //System.out.println("Slave Master Node setArriveNode Error [" + slaveList[i] + "]");
                        //e.printStackTrace();
                    } finally {
                        try {
                            if (pw != null) {
                                // 接続切断を通知
                                pw.println(ImdstDefine.imdstConnectExitRequest);
                                pw.flush();

                                pw.close();
                                pw = null;
                            }

                            if (br != null) {
                                br.close();
                                br = null;
                            }

                            if (socket != null) {
                                socket.close();
                                socket = null;
                            }
                        } catch(Exception e2) {
                            // 無視
                        }
                    }
                }
            }
        }
    }


    /**
     * ノードのリカバリーの開始、終了をスレーブのMasterNodeへ送信
     *
     *
     * @param recoverMode
     */
    protected void setRecoverNode(boolean recoverMode, String nodeInfo) {

        KeyNodeConnector.setRecoverMode(recoverMode, nodeInfo);
        // MainMasterNodeの場合のみ設定される
        if (StatusUtil.isMainMasterNode()) {
            String sendCheckStr = null;

            if (StatusUtil.isNodeArrival(nodeInfo)) {

                // 対象のSlaveMasterNode全てに依頼
                String slaves = StatusUtil.getSlaveMasterNodes();

                if (slaves != null && !slaves.trim().equals("")) {
                    String[] slaveList = slaves.split(",");
                    int execCounter = 0;

                    // MasterNodeへの伝搬は失敗しても2回試す
                    while (execCounter < 2) {
                        // 1ノードづつ実行
                        for (int i = 0; i < slaveList.length; i++) {

                            if (slaveList[i] == null) continue;

                            Socket socket = null;
                            PrintWriter pw = null;
                            BufferedReader br = null;

                            try {

                                // Slaveノード名とポートに分解
                                String[] slaveNodeDt = slaveList[i].split(":");

                                InetSocketAddress inetAddr = new InetSocketAddress(slaveNodeDt[0], Integer.parseInt(slaveNodeDt[1]));
                                socket = new Socket();
                                socket.connect(inetAddr, ImdstDefine.nodeConnectionOpenShortTimeout);
                                socket.setSoTimeout(ImdstDefine.nodeConnectionTimeout);

                                pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), 
                                                                                                                ImdstDefine.keyHelperClientParamEncoding)));
                                br = new BufferedReader(new InputStreamReader(socket.getInputStream(), 
                                                                                                ImdstDefine.keyHelperClientParamEncoding));

                                // 文字列バッファ初期化
                                StringBuffer serverRequestBuf = new StringBuffer();

                                
                                // 処理番号連結
                                if (recoverMode) {
                                    // リカバー開始
                                    serverRequestBuf.append("96");
                                    // セパレータ連結
                                    serverRequestBuf.append(ImdstDefine.keyHelperClientParamSep);
                                    serverRequestBuf.append(nodeInfo);
                                    sendCheckStr = "96";
                                } else {
                                    // リカバー終了
                                    serverRequestBuf.append("97");
                                    serverRequestBuf.append(ImdstDefine.keyHelperClientParamSep);
                                    sendCheckStr = "97";
                                }

                                // サーバ送信
                                pw.println(serverRequestBuf.toString());
                                pw.flush();

                                // サーバから結果受け取り
                                String serverRetStr = br.readLine();

                                String[] serverRet = serverRetStr.split(ImdstDefine.keyHelperClientParamSep);

                                // 処理の妥当性確認

                                if (serverRet[0].equals(sendCheckStr)) {
                                    if (!serverRet[1].equals("true")) {
                                        // TODO:復帰登録失敗
                                        // 異常事態だが、稼動していないことも考えられるので、
                                        // 無視する
                                        //System.out.println("Slave Master Node setDeadNode Error [" + slaveList[i] + "]");
                                    } else {
                                        slaveList[i] = null;
                                    }
                                }
                            } catch(Exception e) {

                                // TODO:復帰登録失敗
                                // 異常事態だが、稼動していないことも考えられるので、
                                // 無視する
                                //System.out.println("Slave Master Node setArriveNode Error [" + slaveList[i] + "]");
                                //e.printStackTrace();

                            } finally {
                                try {
                                    if (pw != null) {
                                        // 接続切断を通知
                                        pw.println(ImdstDefine.imdstConnectExitRequest);
                                        pw.flush();

                                        pw.close();
                                        pw = null;
                                    }

                                    if (br != null) {
                                        br.close();
                                        br = null;
                                    }

                                    if (socket != null) {
                                        socket.close();
                                        socket = null;
                                    }
                                } catch(Exception e2) {
                                    // 無視
                                }
                            }
                        }
                        execCounter++;
                    }
                }
            }
        }
    }


    /**
     * ノードのリカバリーの終了通知とDataNode起動をスレーブのMasterNodeへ送信.<br>
     *
     *
     * @param recoverMode
     */
    protected void setRecoverSuccess(String nodeInfo) {

        KeyNodeConnector.setRecoverMode(false, nodeInfo);
        StatusUtil.setArriveNode(nodeInfo);

        // MainMasterNodeの場合のみ設定される
        if (StatusUtil.isMainMasterNode()) {
            String sendCheckStr = null;

            if (StatusUtil.isNodeArrival(nodeInfo)) {

                // 対象のSlaveMasterNode全てに依頼
                String slaves = StatusUtil.getSlaveMasterNodes();

                if (slaves != null && !slaves.trim().equals("")) {
                    String[] slaveList = slaves.split(",");
                    int execCounter = 0;

                    // MasterNodeへの伝搬は失敗しても2回試す
                    while (execCounter < 2) {
                        // 1ノードづつ実行
                        for (int i = 0; i < slaveList.length; i++) {

                            if (slaveList[i] == null) continue;

                            Socket socket = null;
                            PrintWriter pw = null;
                            BufferedReader br = null;

                            try {

                                // Slaveノード名とポートに分解
                                String[] slaveNodeDt = slaveList[i].split(":");

                                InetSocketAddress inetAddr = new InetSocketAddress(slaveNodeDt[0], Integer.parseInt(slaveNodeDt[1]));
                                socket = new Socket();
                                socket.connect(inetAddr, ImdstDefine.nodeConnectionOpenShortTimeout);
                                socket.setSoTimeout(ImdstDefine.nodeConnectionTimeout);

                                pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), 
                                                                                                                ImdstDefine.keyHelperClientParamEncoding)));
                                br = new BufferedReader(new InputStreamReader(socket.getInputStream(), 
                                                                                                ImdstDefine.keyHelperClientParamEncoding));

                                // 文字列バッファ初期化
                                StringBuffer serverRequestBuf = new StringBuffer();

                                
                                // 処理番号連結
                                // リカバー終了
                                serverRequestBuf.append("97");
                                serverRequestBuf.append(ImdstDefine.keyHelperClientParamSep);

                                // サーバ送信
                                pw.println(serverRequestBuf.toString());
                                pw.flush();

                                // サーバから結果受け取り
                                String serverRetStr = br.readLine();

                                String[] serverRet = serverRetStr.split(ImdstDefine.keyHelperClientParamSep);

                                // 処理の妥当性確認
                                if (serverRet[0].equals("97")) {
                                    if (!serverRet[1].equals("true")) {
                                        // TODO:復帰登録失敗
                                        // 異常事態だが、稼動していないことも考えられるので、
                                        // 無視する
                                        //System.out.println("Slave Master Node setRecoverSuccess Error [" + slaveList[i] + "]");
                                    } else {
                                        slaveList[i] = null;
                                    }
                                }

                                // 文字列バッファ初期化
                                serverRequestBuf = new StringBuffer();

                                // 処理番号連結
                                serverRequestBuf.append("92");
                                // セパレータ連結
                                serverRequestBuf.append(ImdstDefine.keyHelperClientParamSep);

                                // 停止ノード名連結
                                serverRequestBuf.append(nodeInfo);

                                // サーバ送信
                                pw.println(serverRequestBuf.toString());
                                pw.flush();

                                // サーバから結果受け取り
                                serverRetStr = br.readLine();

                                serverRet = serverRetStr.split(ImdstDefine.keyHelperClientParamSep);

                                // 処理の妥当性確認
                                if (serverRet[0].equals("92")) {
                                    if (!serverRet[1].equals("true")) {
                                        // TODO:復帰登録失敗
                                        // 異常事態だが、稼動していないことも考えられるので、
                                        // 無視する
                                        //System.out.println("Slave Master Node setRecoverSuccess Error [" + slaveList[i] + "]");
                                    } 
                                }
                            } catch(Exception e) {

                                // TODO:復帰登録失敗
                                // 異常事態だが、稼動していないことも考えられるので、
                                // 無視する
                                //System.out.println("Slave Master Node setRecoverSuccess Error [" + slaveList[i] + "]");
                                //e.printStackTrace();

                            } finally {
                                try {
                                    if (pw != null) {
                                        // 接続切断を通知
                                        pw.println(ImdstDefine.imdstConnectExitRequest);
                                        pw.flush();

                                        pw.close();
                                        pw = null;
                                    }

                                    if (br != null) {
                                        br.close();
                                        br = null;
                                    }

                                    if (socket != null) {
                                        socket.close();
                                        socket = null;
                                    }
                                } catch(Exception e2) {
                                    // 無視
                                }
                            }
                        }
                        execCounter++;
                    }
                }
            }
        }
    }


    /**
     * ノードとの接続プールが有効か確認
     *
     *
     * @param nodeInfo 対象のノード情報
     * @return boolean true:有効 false:無効
     */
    protected boolean checkConnectionEffective(String nodeInfo, Long time) {
        if (time == null) return true;
        Long rebootTime = StatusUtil.getNodeRebootTime(nodeInfo);

        if (rebootTime == null) return true;
        if (rebootTime.longValue() <= time.longValue()) return true;
        return false;
    }



    /**
     *ノードの一時停止を要求.<br>
     *
     * @param nodeInfo 停止対象ノード
     */
    protected void setNodeWaitStatus(String nodeInfo) {
        StatusUtil.setWaitStatus(nodeInfo);

        // MainのMasterNodeの場合のみ実行
        // SlaveのMasterNodeにもノードの一時停止を依頼
        if (StatusUtil.isMainMasterNode()) {

            // 対象のSlaveMasterNode全てに依頼
            String slaves = StatusUtil.getSlaveMasterNodes();

            if (slaves != null && !slaves.trim().equals("")) {
                String[] slaveList = slaves.split(",");

                // 1ノードづつ実行
                for (int i = 0; i < slaveList.length; i++) {

                    Socket socket = null;
                    PrintWriter pw = null;
                    BufferedReader br = null;

                    try {

                        // Slaveノード名とポートに分解
                        String[] slaveNodeDt = slaveList[i].split(":");

                        InetSocketAddress inetAddr = new InetSocketAddress(slaveNodeDt[0], Integer.parseInt(slaveNodeDt[1]));
                        socket = new Socket();
                        socket.connect(inetAddr, ImdstDefine.nodeConnectionOpenTimeout);
                        socket.setSoTimeout(ImdstDefine.nodeConnectionTimeout);

                        pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), 
                                                                                                        ImdstDefine.keyHelperClientParamEncoding)));
                        br = new BufferedReader(new InputStreamReader(socket.getInputStream(), 
                                                                                        ImdstDefine.keyHelperClientParamEncoding));

                        // 文字列バッファ初期化
                        StringBuffer serverRequestBuf = new StringBuffer();

                        // 処理番号連結
                        serverRequestBuf.append("90");
                        // セパレータ連結
                        serverRequestBuf.append(ImdstDefine.keyHelperClientParamSep);

                        // 停止ノード名連結
                        serverRequestBuf.append(nodeInfo);

                        // サーバ送信
                        pw.println(serverRequestBuf.toString());
                        pw.flush();

                        // サーバから結果受け取り
                        String serverRetStr = br.readLine();

                        String[] serverRet = serverRetStr.split(ImdstDefine.keyHelperClientParamSep);

                        // 処理の妥当性確認
                        if (serverRet[0].equals("90")) {
                            if (!serverRet[1].equals("true")) {
                                // TODO:停止失敗
                                // 異常事態だが、稼動していないことも考えられるので、
                                // 無視する
                                //System.out.println("Slave Master Node setNodeWaitStatus Error [" + slaveList[i] + "]");
                            } 
                        }
                    } catch(Exception e) {

                        // TODO:停止失敗
                        // 異常事態だが、稼動していないことも考えられるので、
                        // 無視する
                        //System.out.println("Slave Master Node setNodeWaitStatus Error [" + slaveList[i] + "]");
                        //e.printStackTrace();
                    } finally {
                        try {
                            if (pw != null) {
                                // 接続切断を通知
                                pw.println(ImdstDefine.imdstConnectExitRequest);
                                pw.flush();

                                pw.close();
                                pw = null;
                            }

                            if (br != null) {
                                br.close();
                                br = null;
                            }

                            if (socket != null) {
                                socket.close();
                                socket = null;
                            }
                        } catch(Exception e2) {
                            // 無視
                        }
                    }
                }
            }
        }
    }


    /**
     *ノードの一時停止を解除.<br>
     *
     * @param nodeInfo 解除対象ノード
     */
    protected void removeNodeWaitStatus(String nodeInfo) {
        StatusUtil.removeWaitStatus(nodeInfo);

        // MainのMasterNodeの場合のみ実行
        // SlaveのMasterNodeにもノードの一時停止解除を依頼
        if (StatusUtil.isMainMasterNode()) {

            // 対象のSlaveMasterNode全てに依頼
            String slaves = StatusUtil.getSlaveMasterNodes();

            if (slaves != null && !slaves.trim().equals("")) {
                String[] slaveList = slaves.split(",");

                // 1ノードづつ実行
                for (int i = 0; i < slaveList.length; i++) {

                    Socket socket = null;
                    PrintWriter pw = null;
                    BufferedReader br = null;

                    try {

                        // Slaveノード名とポートに分解
                        String[] slaveNodeDt = slaveList[i].split(":");

                        InetSocketAddress inetAddr = new InetSocketAddress(slaveNodeDt[0], Integer.parseInt(slaveNodeDt[1]));
                        socket = new Socket();
                        socket.connect(inetAddr, ImdstDefine.nodeConnectionOpenTimeout);
                        socket.setSoTimeout(ImdstDefine.nodeConnectionTimeout);

                        pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), 
                                                                                                        ImdstDefine.keyHelperClientParamEncoding)));
                        br = new BufferedReader(new InputStreamReader(socket.getInputStream(), 
                                                                                        ImdstDefine.keyHelperClientParamEncoding));

                        // 文字列バッファ初期化
                        StringBuffer serverRequestBuf = new StringBuffer();

                        // 処理番号連結
                        serverRequestBuf.append("91");
                        // セパレータ連結
                        serverRequestBuf.append(ImdstDefine.keyHelperClientParamSep);

                        // 停止ノード名連結
                        serverRequestBuf.append(nodeInfo);

                        // サーバ送信
                        pw.println(serverRequestBuf.toString());
                        pw.flush();

                        // サーバから結果受け取り
                        String serverRetStr = br.readLine();

                        String[] serverRet = serverRetStr.split(ImdstDefine.keyHelperClientParamSep);

                        // 処理の妥当性確認
                        if (serverRet[0].equals("91")) {
                            if (!serverRet[1].equals("true")) {
                                // TODO:停止解除失敗
                                // 異常事態だが、稼動していないことも考えられるので、
                                // 無視する
                                //System.out.println("Slave Master Node removeNodeWaitStatus Error [" + slaveList[i] + "]");
                            } 
                        }
                    } catch(Exception e) {

                        // TODO:停止解除失敗
                        // 異常事態だが、稼動していないことも考えられるので、
                        // 無視する
                        //System.out.println("Slave Master Node removeNodeWaitStatus Error [" + slaveList[i] + "]");
                        //e.printStackTrace();
                    } finally {
                        try {
                            if (pw != null) {
                                // 接続切断を通知
                                pw.println(ImdstDefine.imdstConnectExitRequest);
                                pw.flush();

                                pw.close();
                                pw = null;
                            }

                            if (br != null) {
                                br.close();
                                br = null;
                            }

                            if (socket != null) {
                                socket.close();
                                socket = null;
                            }
                        } catch(Exception e2) {
                            // 無視
                        }
                    }
                }
            }
        }
    }


    /**
     * ノードに対するアクセス終了をマーク
     *
     */
    protected void execNodeUseEnd(String nodeInfo) {
        StatusUtil.endNodeUse(nodeInfo);
    }

    /**
     * 指定ノードの使用状況を取得
     * 返却値は現在の使用数をあらわす
     *
     */
    protected int getNodeUseStatus(String nodeInfo) {
        return StatusUtil.getNodeUseStatus(nodeInfo);
    }


    protected String[] execNodePing(String nodeName, int port, ILogger logger) {
        return execNodePing(nodeName, port, logger, ImdstDefine.defaultDeadPingCount); 
    }

    /**
     * ノードに対して生存確認用のPingを行う.<br>
     *
     * @param nodeName ノード名
     * @param port ポート番号
     * @param logger ロガー
     * @param deadCount Deadとみなす回数
     * @return String[] 結果 配列の1番目:"true" or "false", 配列の2番目:1番目が"true"の場合ステータス文字列
     */
    protected String[] execNodePing(String nodeName, int port, ILogger logger, int deadCount) {
        String[] retStrs = new String[2];
        retStrs[0] = "true";
        String connectionFullName = nodeName + ":" + port;

        KeyNodeConnector keyNodeConnector = null;

        String[] retParams = null;
        boolean cacheConnectUse = false;

        for (int tryCount = 0; tryCount < deadCount; tryCount++) {
            retStrs = new String[2];
            retStrs[0] = "true";

            keyNodeConnector = null;
            retParams = null;

            try {

                // 一回目のPingはCacheのコネクションを積極的に使う
                if (tryCount == 0) {
                    // キャッシュが存在する場合はそこから取得
                    if (keyNodeConnectPool.containsKey(connectionFullName)) {
                        if((keyNodeConnector = (KeyNodeConnector)((ArrayBlockingQueue)keyNodeConnectPool.get(connectionFullName)).poll()) != null) {
                            if (!checkConnectionEffective(connectionFullName, keyNodeConnector.getConnetTime())) {
                                keyNodeConnector = null;
                            }
                        }
                    } 
                }

                // コネクションがなければ自身で接続
                if (keyNodeConnector == null) {

                    // 接続
                    keyNodeConnector = new KeyNodeConnector(nodeName, port, connectionFullName);
                    keyNodeConnector.connect(ImdstDefine.nodeConnectionOpenPingTimeout);
                }

                // タイムアウト設定
                keyNodeConnector.setSoTimeout(ImdstDefine.nodeConnectionPingTimeout);

                // Key値でデータノード名を保存
                StringBuffer buf = new StringBuffer();
                // パラメータ作成 処理タイプ[セパレータ]キー値のハッシュ値文字列[セパレータ]データノード名
                buf.append("10");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append("PING_CHECK");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append("Check");

                // 送信
                keyNodeConnector.println(buf.toString());
                keyNodeConnector.flush();

                // 返却値取得
                String retParam = keyNodeConnector.readLine();

                retParams = retParam.split(ImdstDefine.keyHelperClientParamSep);

                if (!retParams[1].equals("true")) {

                    retStrs[0] = "false";
                } else {

                    retStrs[0] = "true";
                    retStrs[1] = retParams[2];
                }

                if (retStrs[0].equals("true")) {
                    try {

                        // 正しく終了した場合のみコネクションをキャッシュに戻す
                        if (keyNodeConnector != null) {
                            keyNodeConnector.setSoTimeout(ImdstDefine.nodeConnectionTimeout);
                            addKeyNodeCacheConnectionPool(keyNodeConnector);
                            keyNodeConnector = null;
                        }
                    } catch(Exception e1) {
                        // 無視
                        logger.error(e1);
                    }
                    break;
                }
            } catch(Exception e) {

                retStrs[0] = "false";
                logger.info("Node Ping Chekc Error Node Name = [" + nodeName + "] Port [" + port + "]");
                logger.info(e);
            } finally {
                try {

                    if (keyNodeConnector != null) keyNodeConnector.close();
                } catch(Exception e2) {
                    // 無視
                    logger.error(e2);
                }
            }
        }
        return retStrs;
    }


    /**
     * データノードとのコネクションをセットする.<br>
     * ConnectionPoolHelperが使用する.<br>
     *
     *
     */
    protected void setActiveConnection(String connectionName, KeyNodeConnector keyNodeConnector) {

        ArrayBlockingQueue connList = null;
        connList = (ArrayBlockingQueue)allConnectionMap.get(connectionName);

        if (connList == null) connList = new ArrayBlockingQueue(512);

        if(connList.offer(keyNodeConnector)) {
            allConnectionMap.put(connectionName, connList);
            connPoolCount.incrementAndGet();
        }

    }


    /**
     * 使用済みの接続をPoolに戻す.<br>
     * DataNodeとの接続キャッシュ用.<br>
     *
     */
    protected void addKeyNodeCacheConnectionPool(KeyNodeConnector keyNodeConnector) {
        String connectionFullName = null;
        if (keyNodeConnector != null) {

            connectionFullName = keyNodeConnector.getNodeFullName();

            if (keyNodeConnectPool.containsKey(connectionFullName)) {

                ((ArrayBlockingQueue)keyNodeConnectPool.get(connectionFullName)).offer(keyNodeConnector);
            } else {

                synchronized(keyNodeConnectPool) {

                    if (!keyNodeConnectPool.containsKey(connectionFullName)) {

                        ArrayBlockingQueue connPoolQueue = new ArrayBlockingQueue(20000);
                        connPoolQueue.offer(keyNodeConnector);
                        keyNodeConnectPool.put(connectionFullName, connPoolQueue);
                    } else {

                        addKeyNodeCacheConnectionPool(keyNodeConnector);
                    }
                }
            }
        }
    }


    protected void executeKeyNodeOptimization(boolean flg) {
        executeKeyNodeOptimizationFlg = flg;
    }

    protected boolean isExecuteKeyNodeOptimization() {
        return executeKeyNodeOptimizationFlg;
    }

    protected HashMap getConsistentHashMoveData() {
        return moveData4ConsistentHash;
    }


    protected void setConsistentHashMoveData(HashMap map) {
        moveData4ConsistentHash = map;
    }


    protected void removeConsistentHashMoveData() {
        moveData4ConsistentHash = null;
    }


    protected void setNowNodeDataOptimization(boolean flg) {
        nowNodeDataOptimization = flg;
    }

    protected boolean getNowNodeDataOptimization() {
        return nowNodeDataOptimization;
    }


    /**
     * ConnectionPoolが作りだしたコネクションを使用する.<br>
     *
     */
    protected KeyNodeConnector getActiveConnection(String connectionName) {
        KeyNodeConnector keyNodeConnector = null;
        ArrayBlockingQueue connList = (ArrayBlockingQueue)allConnectionMap.get(connectionName);

        if (connList != null) {
            //long start = System.nanoTime();
            keyNodeConnector = (KeyNodeConnector)connList.poll();
            if (keyNodeConnector != null) {
                connPoolCount.decrementAndGet();
                if(!this.checkConnectionEffective(connectionName, keyNodeConnector.getConnetTime())) return null;
            }
        }
        return keyNodeConnector;
        
    }

    protected int getNowConnectionPoolCount() {
        return connPoolCount.intValue();
    }
}