package org.imdst.helper;

import java.io.*;
import java.util.*;
import java.net.*;

import org.batch.lang.BatchException;
import org.batch.job.AbstractHelper;
import org.batch.job.IJob;
import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.imdst.util.KeyMapManager;
import org.imdst.util.ImdstDefine;
import org.imdst.util.DataDispatcher;
import org.imdst.util.StatusUtil;

/**
 * MasterNodeのメイン実行部分<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
abstract public class AbstractMasterManagerHelper extends AbstractHelper {


    private static int connPoolCount = 0;

    private int nowSt = 0;

    private static HashMap allConnectionMap = new HashMap();

    private static Object connSync = new Object();

    private static HashMap moveData4ConsistentHash = null;

    private static boolean executeKeyNodeOptimizationFlg = false;


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
        if (te != null) {
            te.printStackTrace();
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

        BufferedReader br = null;
        PrintWriter pw = null;
        Socket socket = null;
        String[] retParams = null;

        for (int tryCount = 0; tryCount < deadCount; tryCount++) {
            retStrs = new String[2];
            retStrs[0] = "true";

            br = null;
            pw = null;
            socket = null;
            retParams = null;

            try {
                // 接続
                InetSocketAddress inetAddr = new InetSocketAddress(nodeName, port);
                socket = new Socket();
                socket.connect(inetAddr, ImdstDefine.nodeConnectionOpenPingTimeout);
                socket.setSoTimeout(ImdstDefine.nodeConnectionPingTimeout);

                OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding);
                pw = new PrintWriter(new BufferedWriter(osw));

                InputStreamReader isr = new InputStreamReader(socket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding);
                br = new BufferedReader(isr);

                // Key値でデータノード名を保存
                StringBuffer buf = new StringBuffer();
                // パラメータ作成 処理タイプ[セパレータ]キー値のハッシュ値文字列[セパレータ]データノード名
                buf.append("10");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append("PING_CHECK");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append("Check");

                // 送信
                pw.println(buf.toString());
                pw.flush();

                // 返却値取得
                String retParam = br.readLine();

                retParams = retParam.split(ImdstDefine.keyHelperClientParamSep);

                if (!retParams[1].equals("true")) {

                    retStrs[0] = "false";
                } else {

                    retStrs[0] = "true";
                    retStrs[1] = retParams[2];
                    
                }
                pw.println(ImdstDefine.imdstConnectExitRequest);
                pw.flush();

                if (retStrs[0].equals("true")) break;
            } catch(Exception e) {

                retStrs[0] = "false";
                logger.info("Node Ping Chekc Error Node Name = [" + nodeName + "] Port [" + port + "]");
                logger.info(e);
            } finally {
                try {

                    if (br != null) br.close();
                    if (pw != null) {
                        pw.close();
                    }
                    if (socket != null) socket.close();
                } catch(Exception e2) {
                    // 無視
                    logger.error(e2);
                }
            }
        }
        return retStrs;
    }


    /**
     * ダウン状態から復帰したノードに対して、ペアーのノードのデータをコピーする.<br>
     * コピー元のデータをコピー先へ.<br>
     * 本メソッドを呼び出す前に必ず両ノードの使用を一時中断していること
     *
     * @param コピー先ノード(予定)
     * @param コピー元ノード(予定)
     *
     * @return boolean 成否
     */
    protected boolean nodeDataRecover(String nodeInfo, String masterNodeInfo, ILogger logger) throws BatchException {
        return this.nodeDataRecover(nodeInfo, masterNodeInfo, false, logger);
    }

    /**
     * ダウン状態から復帰したノードに対して、ペアーのノードのデータをコピーする.<br>
     * コピー元のデータをコピー先へ.<br>
     * 本メソッドを呼び出す前に必ず両ノードの使用を一時中断していること
     *
     * @param コピー先ノード(予定)
     * @param コピー元ノード(予定)
     *
     * @return boolean 成否
     */
    protected boolean nodeDataRecover(String nodeInfo, String masterNodeInfo, boolean noDataCheck, ILogger logger) throws BatchException {

        boolean ret = true;

        String retParam = null;
        String[] retParams = null;
        String lineCount = null;

        String[] nodeDt = nodeInfo.split(":");
        String[] masterNodeDt = masterNodeInfo.split(":");

        String nodeName = nodeDt[0];
        int nodePort = new Integer(nodeDt[1]).intValue();

        String masterNodeName = masterNodeDt[0];
        int masterNodePort = new Integer(masterNodeDt[1]).intValue();


        PrintWriter pw = null;
        BufferedReader br = null;
        Socket socket = null;
        ObjectOutputStream oos = null;

        PrintWriter mpw = null;
        BufferedReader mbr = null;
        Socket msocket = null;
        ObjectInputStream mois = null;

        try {

            logger.info("Data Recover Schedule [" + masterNodeInfo + " => " + nodeInfo + "]");
            // コピー先KeyNodeとの接続を確立
            socket = new Socket(nodeName, nodePort);
            socket.setSoTimeout(ImdstDefine.recoverConnectionTimeout);

            OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding);
            pw = new PrintWriter(new BufferedWriter(osw));

            InputStreamReader isr = new InputStreamReader(socket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding);
            br = new BufferedReader(isr);

            // コピー元KeyNodeとの接続を確立
            msocket = new Socket(masterNodeName, masterNodePort);
            msocket.setSoTimeout(ImdstDefine.recoverConnectionTimeout);

            OutputStreamWriter mosw = new OutputStreamWriter(msocket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding);
            mpw = new PrintWriter(new BufferedWriter(mosw));

            InputStreamReader misr = new InputStreamReader(msocket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding);
            mbr = new BufferedReader(misr);


            // TODO:ここでそれぞれのノードの最終更新時間を見て新しいほうのデータで上書き
            //      するが微妙かも

            // コピー元予定から最終更新時刻取得
            StringBuffer buf = new StringBuffer();
            // 処理番号11
            buf.append("11");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append("true");
            // 送信
            mpw.println(buf.toString());
            mpw.flush();
            // データ取得
            retParam = mbr.readLine();

            String[] updateDate = retParam.split(ImdstDefine.keyHelperClientParamSep);

            long masterDate = new Long(updateDate[2]).longValue();


            // コピー先予定から最終更新時刻取得
            buf = new StringBuffer();
            // 処理番号11
            buf.append("11");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append("true");
            // 送信
            pw.println(buf.toString());
            pw.flush();
            // データ取得
            retParam = br.readLine();

            updateDate = retParam.split(ImdstDefine.keyHelperClientParamSep);

            long nodeDate = new Long(updateDate[2]).longValue();


            // どちらが新しいか比べる
            if (noDataCheck == true || masterDate >= nodeDate) {

                // 予定どうり
                logger.info("Data Recover Actually [" + masterNodeInfo + " => " + nodeInfo + "]");

                // もともと生存していたノードを差分モードOnにする
                buf = new StringBuffer();
                // 処理番号22
                buf.append("22").append(ImdstDefine.keyHelperClientParamSep).append("true");
                // 送信
                mpw.println(buf.toString());
                mpw.flush();

                // コピー元の一時停止を解除
                this.removeNodeWaitStatus(masterNodeInfo);


                // コピー元からデータ読み込み
                buf = new StringBuffer();
                // 処理番号20
                buf.append("20");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append("true");

                // 送信
                mpw.println(buf.toString());
                mpw.flush();

                // データ行数取得
                // 1行にメモリに乗るのに十分余裕のあるサイズが送られてくる
                lineCount = mbr.readLine();


                // 取得したデータをコピー先に書き出し
                // 処理番号21
                buf = new StringBuffer();
                buf.append("21");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append(lineCount);
                // 送信
                pw.println(buf.toString());
                pw.flush();

                for (int i = 0; i < Integer.parseInt(lineCount); i++) {
                    // 値を書き出し
                    retParam = mbr.readLine();
                    pw.println(retParam);
                    pw.flush();
                }


                // コピー元を一時停止にする
                this.setNodeWaitStatus(masterNodeInfo);
                while(true) {
                    // 使用停止まで待機
                    if(this.getNodeUseStatus(masterNodeInfo) == 0) break;
                    Thread.sleep(50);
                }

                // 停止完了後差分データを取得
                buf = new StringBuffer();
                // 処理番号24
                buf.append("24");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append("true");

                // 送信
                mpw.println(buf.toString());
                mpw.flush();

                // 差分データを送る
                buf = new StringBuffer();
                buf.append("25");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append(mbr.readLine());
                pw.println(buf.toString());
                pw.flush();

                // もともと生存していたノードを差分モードOffにする
                buf = new StringBuffer();
                // 処理番号23
                buf.append("23").append(ImdstDefine.keyHelperClientParamSep).append("true");
                // 送信
                mpw.println(buf.toString());
                mpw.flush();

            } else {

                // 当初の予定から逆転
                logger.info("Data Recover Actually [" + nodeInfo + " => " + masterNodeInfo + "]");

                // 最終更新日付が新しいノードを差分モードOnにする
                buf = new StringBuffer();
                // 処理番号22
                buf.append("22").append(ImdstDefine.keyHelperClientParamSep).append("true");
                // 送信
                pw.println(buf.toString());

                pw.flush();

                // コピー元の一時停止を解除
                this.removeNodeWaitStatus(nodeInfo);


                // コピー元からデータ読み込み
                buf = new StringBuffer();
                // 処理番号20
                buf.append("20");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append("true");

                // 送信
                pw.println(buf.toString());

                pw.flush();

                // データ行数取得
                // 1行にメモリに乗るのに十分余裕のあるサイズが送られてくる
                lineCount = br.readLine();


                // 取得したデータをコピー先に書き出し
                // 処理番号21
                buf = new StringBuffer();
                buf.append("21");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append(lineCount);

                // 送信
                mpw.println(buf.toString());
                mpw.flush();

                for (int i = 0; i < Integer.parseInt(lineCount); i++) {

                    // データ取得
                    retParam = br.readLine();

                    // 値を書き出し
                    mpw.println(retParam);

                    mpw.flush();
                }


                // コピー元を一時停止にする
                this.setNodeWaitStatus(nodeInfo);

                while(true) {

                    // 使用停止まで待機
                    if(this.getNodeUseStatus(nodeInfo) == 0) break;
                    Thread.sleep(50);
                }


                // 停止完了後差分データを取得
                buf = new StringBuffer();
                // 処理番号24
                buf.append("24");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append("true");

                // 送信
                pw.println(buf.toString());

                pw.flush();

                // 差分データを送る
                buf = new StringBuffer();
                buf.append("25");
                buf.append(ImdstDefine.keyHelperClientParamSep);
                buf.append(br.readLine());
                mpw.println(buf.toString());

                mpw.flush();

                // もともと生存していたノードを差分モードOffにする
                buf = new StringBuffer();
                // 処理番号23
                buf.append("23").append(ImdstDefine.keyHelperClientParamSep).append("true");
                // 送信
                pw.println(buf.toString());

                pw.flush();

            }
        } catch (Exception e) {

            logger.error(e);
            ret = false;
        } finally {
            try {

                // コネクション切断
                if (pw != null) {
                    pw.println(ImdstDefine.imdstConnectExitRequest);
                    pw.flush();
                    pw.close();
                }
                if (oos != null) oos.close();
                if (socket != null) socket.close();


                // コネクション切断
                if (mpw != null) {
                    mpw.println(ImdstDefine.imdstConnectExitRequest);
                    mpw.flush();
                    mpw.close();
                }
                if (mois != null) mois.close();
                if (msocket != null) msocket.close();

            } catch(Exception e2) {
                // 無視
                logger.error(e2);
            }
        }
        return ret;
    }


    /**
     *
     *
     */
    protected void setActiveConnection(String connectionName, HashMap connectionMap) {
        synchronized(connSync) {
            ArrayList connList = null;
            connList = (ArrayList)allConnectionMap.get(connectionName);

            if (connList == null) connList = new ArrayList();

            connList.add(connectionMap);
            allConnectionMap.put(connectionName, connList);
            connPoolCount++;
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


    /**
     *
     *
     */
    protected HashMap getActiveConnection(String connectionName) {
        HashMap ret = null;
        synchronized(connSync) {
            ArrayList connList = (ArrayList)allConnectionMap.get(connectionName);
            if (connList != null) {
                if(connList.size() > 0) {
                    ret = (HashMap)connList.remove(0);
                    connPoolCount--;
                }
            }
        }

        if (ret != null) {
            if(!this.checkConnectionEffective(connectionName, (Long)ret.get("time"))) ret = null;
        }
        return ret;
        
    }

    protected int getNowConnectionPoolCount() {
        return connPoolCount;
    }
}