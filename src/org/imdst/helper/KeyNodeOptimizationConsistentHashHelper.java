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

    private PrintWriter mainPw = null;
    private BufferedReader mainBr = null;
    private Socket mainSocket = null;

    private PrintWriter subPw = null;
    private BufferedReader subBr = null;
    private Socket subSocket = null;

    private PrintWriter thirdPw = null;
    private BufferedReader thirdBr = null;
    private Socket thirdSocket = null;

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

        boolean sendError = false;


        // Main
        String addMainDataNodeInfo = null;
        HashMap mainMoveTargetMap = null;
        String[] toMainDataNodeDt = null;
        Set mainSet = null;
        Iterator mainIterator = null;

        Socket toMainSocket = null;
        PrintWriter toMainPw = null;
        BufferedReader toMainBr = null;
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

        Socket toSubSocket = null;
        PrintWriter toSubPw = null;
        BufferedReader toSubBr = null;
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

        Socket toThirdSocket = null;
        PrintWriter toThirdPw = null;
        BufferedReader toThirdBr = null;
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

                    StringBuffer sendRequestBuf = new StringBuffer();

                    // 送信リクエスト文字列作成
                    // 処理番号28
                    sendRequestBuf.append("28");
                    sendRequestBuf.append(ImdstDefine.keyHelperClientParamSep);
                    sendRequestBuf.append("true");

                    if (moveTargetData != null) {
                        while (true) {
                            try {

                                // 全て初期化
                                sendError = false;

                                // Main
                                toMainSocket = null;
                                toMainPw = null;
                                toMainBr = null;
                                toMainSendRet = null;
                                mainSet = null;
                                mainIterator = null;
                                mainDataNodeStr = null;
                                mainDataNodeDetail = null;
                                mainRangStr = null;
                                mainTargetDataStr = null;
                                mainRemoveTargetDatas = new ArrayList();

                                // Sub
                                toSubSocket = null;
                                toSubPw = null;
                                toSubBr = null;
                                toSubSendRet = null;
                                subSet = null;
                                subIterator = null;
                                subDataNodeStr = null;
                                subDataNodeDetail = null;
                                subRangStr = null;
                                subTargetDataStr = null;
                                subRemoveTargetDatas = new ArrayList();

                                // Third
                                toThirdSocket = null;
                                toThirdPw = null;
                                toThirdBr = null;
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
System.out.println("-------------------- tomain -----------------");
System.out.println(addMainDataNodeInfo);
System.out.println(mainMoveTargetMap);

                                try {
                                    toMainSocket = new Socket(toMainDataNodeDt[0], Integer.parseInt(toMainDataNodeDt[1]));
                                    toMainSocket.setSoTimeout(ImdstDefine.recoverConnectionTimeout);
                                    toMainPw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(toMainSocket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding)));
                                    toMainBr = new BufferedReader(new InputStreamReader(toMainSocket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding));

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
System.out.println("-------------------- tosub -----------------");
System.out.println(addSubDataNodeInfo);
System.out.println(subMoveTargetMap);
                                if (addSubDataNodeInfo != null) {
                                    try {
                                        toSubDataNodeDt = addSubDataNodeInfo.split(":");
                                        toSubSocket = new Socket(toSubDataNodeDt[0], Integer.parseInt(toSubDataNodeDt[1]));
                                        toSubSocket.setSoTimeout(ImdstDefine.recoverConnectionTimeout);
                                        toSubPw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(toSubSocket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding)));
                                        toSubBr = new BufferedReader(new InputStreamReader(toSubSocket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding));

                                        subSet = subMoveTargetMap.keySet();
                                        subIterator = subSet.iterator();

                                        // 移行先スレーブデータノードにデータ移行開始を送信
                                        toSubPw.println(sendRequestBuf.toString());
                                        toSubPw.flush();
                                    } catch (Exception e) {
                                        toSubPw = null;
                                    }
                                }

                                // サードノード処理
                                addThirdDataNodeInfo = (String)moveTargetData.get("tothird");
                                thirdMoveTargetMap = (HashMap)moveTargetData.get("third");
System.out.println("-------------------- tothird -----------------");
System.out.println(addThirdDataNodeInfo);
System.out.println(thirdMoveTargetMap);
                                if (addThirdDataNodeInfo != null) {
                                    try {
                                        toThirdDataNodeDt = addThirdDataNodeInfo.split(":");
                                        toThirdSocket = new Socket(toThirdDataNodeDt[0], Integer.parseInt(toThirdDataNodeDt[1]));
                                        toThirdSocket.setSoTimeout(ImdstDefine.recoverConnectionTimeout);
                                        toThirdPw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(toThirdSocket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding)));
                                        toThirdBr = new BufferedReader(new InputStreamReader(toThirdSocket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding));

                                        thirdSet = thirdMoveTargetMap.keySet();
                                        thirdIterator = thirdSet.iterator();

                                        // 移行先スレーブデータノードにデータ移行開始を送信
                                        toThirdPw.println(sendRequestBuf.toString());
                                        toThirdPw.flush();
                                    } catch (Exception e) {
                                        toThirdPw = null;
                                    }
                                }


                                // 全ての移動対象のノードを処理
                                // 対象データノード1ノードづつ処理
                                while(mainIterator.hasNext()) {
System.out.println("11111111111111111111111=" + mainRangStr);
                                    // Mainノード処理
                                    // キー値を取り出し
                                    mainDataNodeStr = (String)mainIterator.next();
                                    mainDataNodeDetail = mainDataNodeStr.split(":");
                                    // Rangの文字列を取り出し
                                    mainRangStr = (String)mainMoveTargetMap.get(mainDataNodeStr);
                                    // 対象ノードからデータ取り出し
                                    this.getTargetData(1, mainDataNodeDetail[0], Integer.parseInt(mainDataNodeDetail[1]), mainRangStr);
                                    mainRemoveTargetDatas.add(new String(mainDataNodeDetail[0] + "#" + mainDataNodeDetail[1] + "#" + mainRangStr));


                                    // Subノード処理
                                    if (subIterator != null) {
System.out.println("22222222222222222222222222=" + subRangStr);
                                        // キー値を取り出し
                                        subDataNodeStr = (String)subIterator.next();
                                        subDataNodeDetail = subDataNodeStr.split(":");
                                        // Rangの文字列を取り出し
                                        subRangStr = (String)subMoveTargetMap.get(subDataNodeStr);
                                        // 対象ノードからデータ取り出し
                                        this.getTargetData(2, subDataNodeDetail[0], Integer.parseInt(subDataNodeDetail[1]), subRangStr);
                                        subRemoveTargetDatas.add(new String(subDataNodeDetail[0] + "#" + subDataNodeDetail[1] + "#" + subRangStr));
                                    }

                                    // Thirdノード処理
                                    if (thirdIterator != null) {
System.out.println("333333333333333333333333333=" + thirdRangStr);
                                        // キー値を取り出し
                                        thirdDataNodeStr = (String)thirdIterator.next();
                                        thirdDataNodeDetail = thirdDataNodeStr.split(":");
                                        // Rangの文字列を取り出し
                                        thirdRangStr = (String)thirdMoveTargetMap.get(thirdDataNodeStr);
                                        // 対象ノードからデータ取り出し
                                        this.getTargetData(3, thirdDataNodeDetail[0], Integer.parseInt(thirdDataNodeDetail[1]), thirdRangStr);
                                        thirdRemoveTargetDatas.add(new String(thirdDataNodeDetail[0] + "#" + thirdDataNodeDetail[1] + "#" + thirdRangStr));
                                    }


                                    // 対象のデータを順次対象のノードに移動
                                    while((mainTargetDataStr = this.nextData(1)) != null) {
System.out.println("4444444444444444444444444");
                                        toMainPw.println(mainTargetDataStr);
                                        toMainPw.flush();
                                        toMainSendRet = toMainBr.readLine();
                                        // エラーなら移行中止
                                        if (toMainSendRet == null || !toMainSendRet.equals("next")) { 
System.out.println("55555555555555555555555");
                                            sendError = true;
                                            break;
                                        }
System.out.println("66666666666666666666666666");
                                        if (subIterator != null) {
System.out.println("7777777777777777777777777");
                                            if ((subTargetDataStr = this.nextData(2)) != null) {
System.out.println("8888888888888888888888888");
                                                toSubPw.println(subTargetDataStr);
                                                toSubPw.flush();
                                                toSubSendRet = toSubBr.readLine();
                                                // エラーなら移行中止
                                                if (toSubSendRet == null || !toSubSendRet.equals("next")) {
System.out.println("999999999999999999999999999");
                                                    sendError = true;
                                                    break;
                                                }
                                            }
                                        }
System.out.println("10-10-10-10-10-10-10-10-10-10");
                                        if (thirdIterator != null) {
System.out.println("11-11-11-11-11-11-11-11-11-11");
                                            if ((thirdTargetDataStr = this.nextData(3)) != null) {
System.out.println("12-12-12-12-12-12-12-12-12-12");
                                                toThirdPw.println(thirdTargetDataStr);
                                                toThirdPw.flush();
                                                toThirdSendRet = toThirdBr.readLine();
                                                // エラーなら移行中止
                                                if (toThirdSendRet == null || !toThirdSendRet.equals("next")) {
System.out.println("13-13-13-13-13-13-13-13-13-13");
                                                    sendError = true;
                                                    break;
                                                }
                                            }
                                        }
                                    }

System.out.println("14-14-14-14-14-14-14-14-14-14");
                                    // 転送元を切断
                                    this.closeConnect(1);
                                    if (subIterator != null) this.closeConnect(2);
                                    if (thirdIterator != null) this.closeConnect(3);
                                    if (sendError == true) break;
                                }

System.out.println("15-15-15-15-15-15-15-15-15-15");

                                // 全てのデータの移行が完了
                                // 転送先に終了を通知
                                // Main
                                toMainPw.println("-1");
                                toMainPw.flush();
                                toMainPw.println(ImdstDefine.imdstConnectExitRequest);
                                toMainPw.flush();
                                toMainPw.close();
                                toMainSocket.close();
System.out.println("16-16-16-16-16-16-16-16-16-16");
                                // Sub
                                if (subIterator != null) {
                                
System.out.println("17-17-17-17-17-17-17-17-17-17");
                                    toSubPw.println("-1");
                                    toSubPw.flush();
                                    toSubPw.println(ImdstDefine.imdstConnectExitRequest);
                                    toSubPw.flush();
                                    toSubPw.close();
                                    toSubSocket.close();
                                }

                                // Third
                                if (thirdIterator != null) {
System.out.println("18-18-18-18-18-18-18-18-18-18");
                                    toThirdPw.println("-1");
                                    toThirdPw.flush();
                                    toThirdPw.println(ImdstDefine.imdstConnectExitRequest);
                                    toThirdPw.flush();
                                    toThirdPw.close();
                                    toThirdSocket.close();
                                }


                                // 移動もとのデータを消す処理をここに追加
                                if (sendError == false) {

System.out.println("19-19-19-19-19-19-19-19-19-19");

                                    // 転送が正しく完了した場合のみ処理開始

                                    // Main
                                    for (int mainIdx = 0; mainIdx < mainRemoveTargetDatas.size(); mainIdx++) {
                                        String mainRemoveHostDtStr = (String)mainRemoveTargetDatas.get(mainIdx);
                                        String[] mainRemoveHostDt = mainRemoveHostDtStr.split("#");
                                        if(!this.removeTargetData(mainRemoveHostDt[0], Integer.parseInt(mainRemoveHostDt[1]), mainRemoveHostDt[2])) {
                                            logger.error("KeyNodeOptimizationConsistentHashHelper - removeTargetData - Error target=[" + mainRemoveHostDt[0] + ":" + mainRemoveHostDt[1] + " Range[" + mainRemoveHostDt[2] + "]");
                                        }
                                    }

                                    // Sub
                                    if (subIterator != null) {
System.out.println("20-20-20-20-20-20-20-20-20-20");
                                        for (int subIdx = 0; subIdx < subRemoveTargetDatas.size(); subIdx++) {
                                            String subRemoveHostDtStr = (String)subRemoveTargetDatas.get(subIdx);
                                            String[] subRemoveHostDt = subRemoveHostDtStr.split("#");
                                            if(!this.removeTargetData(subRemoveHostDt[0], Integer.parseInt(subRemoveHostDt[1]), subRemoveHostDt[2])) {
                                                logger.error("KeyNodeOptimizationConsistentHashHelper - removeTargetData - Error target=[" + subRemoveHostDt[0] + ":" + subRemoveHostDt[1] + " Range[" + subRemoveHostDt[2] + "]");
                                            }
                                        }
                                    }

                                    // Third
                                    if (thirdIterator != null) {
System.out.println("21-21-21-21-21-21-21-21-21-21");

                                        for (int thirdIdx = 0; thirdIdx < thirdRemoveTargetDatas.size(); thirdIdx++) {
                                            String thirdRemoveHostDtStr = (String)thirdRemoveTargetDatas.get(thirdIdx);
                                            String[] thirdRemoveHostDt = thirdRemoveHostDtStr.split("#");
                                            if(!this.removeTargetData(thirdRemoveHostDt[0], Integer.parseInt(thirdRemoveHostDt[1]), thirdRemoveHostDt[2])) {
                                                logger.error("KeyNodeOptimizationConsistentHashHelper - removeTargetData - Error target=[" + thirdRemoveHostDt[0] + ":" + thirdRemoveHostDt[1] + " Range[" + thirdRemoveHostDt[2] + "]");
                                            }
                                        }
                                    }
System.out.println("22-22-22-22-22-22-22-22-22-22");

                                    // メモリ上から依頼を消す
                                    super.removeConsistentHashMoveData();
                                }

                                break;
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
        StringBuffer buf = null;
        Socket socket = null;
        PrintWriter pw = null;
        BufferedReader br = null;

        try {
            socket = new Socket(nodeName, nodePort);
            socket.setSoTimeout(ImdstDefine.recoverConnectionTimeout);

            pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding)));
            br = new BufferedReader(new InputStreamReader(socket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding));

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

            if (target == 1) {

                this.mainPw = pw;
                this.mainBr = br;
                this.mainSocket = socket;
            } else if (target == 2) {

                this.subPw = pw;
                this.subBr = br;
                this.subSocket = socket;
            } else if (target == 3) {

                this.thirdPw = pw;
                this.thirdBr = br;
                this.thirdSocket = socket;
            }

        } catch(Exception e) {
            throw new BatchException(e);
        } 
    }


    /**
     *
     * @param target
     * @return String
     * @throw BatchException
     */
    private String nextData(int target) throws BatchException {
        String ret = null;
        String line = null;

        BufferedReader br = null;

        try {
            if (target == 1) {

                br = this.mainBr;
            } else if (target == 2) {

                br = this.subBr;
            } else if (target == 3) {

                br = this.thirdBr;
            }

            while((line = br.readLine()) != null) {

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
        StringBuffer buf = null;
        Socket socket = null;
        PrintWriter pw = null;
        BufferedReader br = null;
        String removeRet = null;

        boolean ret = false;
        try {
            socket = new Socket(nodeName, nodePort);
            socket.setSoTimeout(ImdstDefine.recoverConnectionTimeout);

            pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream() , ImdstDefine.keyHelperClientParamEncoding)));
            br = new BufferedReader(new InputStreamReader(socket.getInputStream(), ImdstDefine.keyHelperClientParamEncoding));

            // 移動元からデータ削除
            buf = new StringBuffer();
            // 処理番号29
            buf.append("29");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append("true");
            buf.append(ImdstDefine.keyHelperClientParamSep);
            buf.append(rangStr);

            // 送信
            pw.println(buf.toString());
            pw.flush();

            removeRet = br.readLine();

            if (removeRet != null && removeRet.equals("-1")) ret = true;

        } catch(Exception e) {
            logger.error("KeyNodeOptimizationConsistentHashHelper - removeTargetData - Error " + e);
        } finally {
            try {
                // コネクション切断
                if (pw != null) {
                    pw.println(ImdstDefine.imdstConnectExitRequest);
                    pw.flush();
                    pw.close();
                    pw = null;
                }

                // コネクション切断
                if (br != null) {
                    br.close();
                    br = null;
                }


                if (socket != null) {
                    socket.close();
                    socket = null;
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
        Socket socket = null;
        PrintWriter pw = null;
        BufferedReader br = null;

        try {

            if (target == 1) {

                pw = this.mainPw;
                br = this.mainBr;
                socket = this.mainSocket;
            } else if (target == 2) {

                pw = this.subPw;
                br = this.subBr;
                socket = this.subSocket;
            } else if (target == 3) {

                pw = this.thirdPw;
                br = this.thirdBr;
                socket = this.thirdSocket;
            }


            // コネクション切断
            if (pw != null) {
                pw.println(ImdstDefine.imdstConnectExitRequest);
                pw.flush();
                pw.close();
                pw = null;
            }

            // コネクション切断
            if (br != null) {
                br.close();
                br = null;
            }


            if (socket != null) {
                socket.close();
                socket = null;
            }


            if (target == 1) {

                this.mainPw = pw;
                this.mainBr = br;
                this.mainSocket = socket;
            } else if (target == 2) {

                this.subPw = pw;
                this.subBr = br;
                this.subSocket = socket;
            } else if (target == 3) {

                this.thirdPw = pw;
                this.thirdBr = br;
                this.thirdSocket = socket;
            }
        } catch(Exception e2) {
            // 無視
            logger.error(e2);
        }
    }
}