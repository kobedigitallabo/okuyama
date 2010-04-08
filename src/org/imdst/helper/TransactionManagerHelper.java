package org.imdst.helper;

import java.io.*;
import java.util.*;
import java.net.*;
import javax.script.*;

import org.batch.lang.BatchException;
import org.batch.job.AbstractHelper;
import org.batch.job.IJob;
import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.imdst.util.KeyMapManager;
import org.imdst.util.ImdstDefine;
import org.imdst.util.StatusUtil;


import com.sun.mail.util.BASE64DecoderStream;
import com.sun.mail.util.BASE64EncoderStream;

/**
 * 分散Transactionrを実現する.<br>
 * 処理メソッドは以下<br>
 *<br>
 * 1.データのLockを行う<br>
 * 2.データのLock解除を行う<br>
 * 3.データのLock状況を確認する<br>
 *<br>
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class TransactionManagerHelper extends AbstractHelper {

    // KeyMapManagerインスタンス
    private KeyMapManager keyMapManager = null;

    private Socket soc = null;

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(TransactionManagerHelper.class);

    // 初期化メソッド定義
    public void initHelper(String initValue) {
    }

    // Jobメイン処理定義
    public String executeHelper(String optionParam) throws BatchException {
        logger.debug("TransactionManagerHelper - executeHelper - start");

        String ret = null;
        boolean closeFlg = false;

        try{
            String[] retParams = null;
            StringBuffer retParamBuf = null;

            Object[] parameters = super.getParameters();

            String clientParametersStr = null;
            String[] clientParameterList = null;

            Integer requestHashCode = null;
            String transactionCode = null;
            int lockWaitingTime = 0;
            int lockingTime = 0;

            // Jobからの引数
            this.keyMapManager = (KeyMapManager)parameters[0];
            this.soc = (Socket)parameters[1];

            // クライアントへのアウトプット
            OutputStreamWriter osw = new OutputStreamWriter(this.soc.getOutputStream() , 
                                                            ImdstDefine.keyHelperClientParamEncoding);
            PrintWriter pw = new PrintWriter(new BufferedWriter(osw));

            // クライアントからのインプット
            InputStreamReader isr = new InputStreamReader(this.soc.getInputStream(),
                                                          ImdstDefine.keyHelperClientParamEncoding);
            BufferedReader br = new BufferedReader(isr);

            while(!closeFlg) {
                try {

                    clientParametersStr = br.readLine();

                    // クライアントからの要求が接続切要求ではないか確認
                    if (clientParametersStr == null || 
                            clientParametersStr.equals("") || 
                                clientParametersStr.equals(ImdstDefine.imdstConnectExitRequest)) {
                        // 切断要求
                        logger.debug("Client Connect Exit Request");
                        closeFlg = true;
                        break;
                    }

                    clientParameterList = clientParametersStr.split(ImdstDefine.keyHelperClientParamSep);

                    // 処理番号を取り出し
                    retParamBuf = new StringBuffer();

                    if(clientParameterList[0].equals("30")) {

                        // Key値とTransactionCodeを使用してLockを取得する
                        requestHashCode = new Integer(clientParameterList[1]);
                        transactionCode = clientParameterList[2];
                        lockingTime = Integer.parseInt(clientParameterList[3]);
                        lockWaitingTime = Integer.parseInt(clientParameterList[4]);

                        // メソッド呼び出し
                        retParams = this.lockDatanode(requestHashCode, transactionCode, lockingTime, lockWaitingTime);
                        retParamBuf.append(retParams[0]);
                        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        retParamBuf.append(retParams[1]);
                        if (retParams.length > 2) {
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(retParams[2]);
                        }

                    } else if(clientParameterList[0].equals("31")) {
                        // Key値とTransactionCodeを使用してLockの開放を行う
                        requestHashCode = new Integer(clientParameterList[1]);
                        transactionCode = clientParameterList[2];

                        // メソッド呼び出し
                        retParams = this.releaseLockDatanode(requestHashCode, transactionCode);
                        retParamBuf.append(retParams[0]);
                        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        retParamBuf.append(retParams[1]);
                        if (retParams.length > 2) {
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(retParams[2]);
                        }
                    } else if(clientParameterList[0].equals("32")) {

                        // KeyでLockが行われているか確認する
                        requestHashCode = new Integer(clientParameterList[1]);

                        // メソッド呼び出し
                        retParams = this.hasLockDatanode(requestHashCode);
                        retParamBuf.append(retParams[0]);
                        retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                        retParamBuf.append(retParams[1]);
                        if (retParams.length > 2) {
                            retParamBuf.append(ImdstDefine.keyHelperClientParamSep);
                            retParamBuf.append(retParams[2]);
                        }
                    }


                    if (retParamBuf != null) {
                        pw.println(retParamBuf.toString());
                        pw.flush();
                    }
                } catch (SocketException se) {
                    closeFlg = true;
                }
            }
            pw.close();
            br.close();
            ret = super.SUCCESS;
        } catch(Exception e) {

            logger.error("TransactionManagerHelper - executeHelper - Error", e);
            ret = super.ERROR;
            throw new BatchException(e);
        } finally {

            try {
                if (this.soc != null) {
                    this.soc.close();
                    this.soc = null;
                }
            } catch(Exception e2) {
                logger.error("TransactionManagerHelper - executeHelper - Error2", e2);
                ret = super.ERROR;
                throw new BatchException(e2);
            }
        }

        logger.debug("TransactionManagerHelper - executeHelper - end");
        return ret;
    }

    /**
     * 終了メソッド定義
     */
    public void endHelper() {
        logger.debug("TransactionManagerHelper - endHelper - start");
        try {
            if (this.soc != null) {
                this.soc.close();
                this.soc = null;
            }
        } catch(Exception e2) {
            logger.error("TransactionManagerHelper - executeHelper - Error2", e2);
        }
        logger.debug("TransactionManagerHelper - endHelper - end");
    }


    /**
     * KeyとTransactionCodeでLockを実施する.<br>
     * ロック取得までの繰り返し時間を設定でき、この時間を超えた場合は<br>
     * ロック取得失敗.<br>
     * 
     * @param key Lock対象キー
     * @param transactionCode Lock時に使用するTransactionCode
     * @param lockingTime Lock継続時間(秒)(ここでの設定時間が経過するとLockは自動的に解除される。0を設定するとremoveを呼び出すまで解除されない)
     * @param lockWaitingTime Lock取得待ち時間(秒)
     * @return String[] 要素1:処理コード,要素2:Lock成否("true" or "false"), 要素3:成功の場合はtransactionCode
     */
    private String[] lockDatanode(Integer key, String transactionCode, int lockingTime, int lockWaitingTime) {
        //logger.debug("TransactionManagerHelper - lockDatanode - start");
        String[] retStrs = null;
        long counter = 0;
        String retTranCd = null;
        int miniCounter = 0;

        try {
            if(!this.keyMapManager.checkError()) {
                while (true) {
                    miniCounter = 0;
                    while(10 > miniCounter) {

                        retTranCd = this.keyMapManager.locking(key, transactionCode, lockingTime);
                        if (retTranCd != null) break;
	                    if (counter == lockWaitingTime) break;
                        miniCounter++;
                        Thread.sleep(100);
                    }

					if (counter == lockWaitingTime) break;
                    if (retTranCd != null) break;
                    counter++;
                }


                if (retTranCd != null) {
                    retStrs = new String[3];
                    retStrs[0] = "30";
                    retStrs[1] = "true";
                    retStrs[2] = transactionCode;
                } else {
                    retStrs = new String[2];
                    retStrs[0] = "30";
                    retStrs[1] = "false";
                }
            } else {
                    retStrs = new String[2];
                    retStrs[0] = "30";
                    retStrs[1] = "false";
            }
        } catch (Exception e) {
            logger.error("TransactionManagerHelper - lockDatanode - Error", e);
            retStrs = new String[2];
            retStrs[0] = "30";
            retStrs[1] = "false";
        }
        //logger.debug("TransactionManagerHelper - lockDatanode - end");
        return retStrs;
    }


    /**
     * KeyとTransactionCodeでLock開放を実施する.<br>
     * Lock時に使用したTransactionCodeと一致しなければ開放されない.<br>
     *
     * @param key Lock対象キー
     * @param transactionCode Lock時に使用するTransactionCode
     * @return String[] 要素1:処理コード,要素2:Lock開放成否("true" or "false"), 要素3:成功の場合はtransactionCode
     */
    private String[] releaseLockDatanode(Integer key, String transactionCode) {
        //logger.debug("TransactionManagerHelper - releaseLockDatanode - start");
        String[] retStrs = null;
        try {
            if(!this.keyMapManager.checkError()) {
                if (this.keyMapManager.isLock(key)) {

                    if (this.keyMapManager.removeLock(key, transactionCode) != null) {

                        retStrs = new String[3];
                        retStrs[0] = "31";
                        retStrs[1] = "true";
                        retStrs[2] = transactionCode;
                    } else {

                        retStrs = new String[2];
                        retStrs[0] = "31";
                        retStrs[1] = "false";
                    }
                } else {

                    retStrs = new String[3];
                    retStrs[0] = "31";
                    retStrs[1] = "true";
                    retStrs[2] = transactionCode;
                }
            } else {

                    retStrs = new String[2];
                    retStrs[0] = "31";
                    retStrs[1] = "false";
            }
        } catch (Exception e) {
            logger.error("TransactionManagerHelper - releaseLockDatanode - Error", e);
            retStrs = new String[2];
            retStrs[0] = "31";
            retStrs[1] = "false";
        }
        //logger.debug("TransactionManagerHelper - releaseLockDatanode - end");
        return retStrs;
    }

    /**
     * KeyのLock状況を返す.<br>
     *
     * @param key Lock対象キー
     * @return String[] 要素1:処理コード,要素2:Lock状況("true" or "false"),要素3:Lock中の場合はTransactionCode
     */
    private String[] hasLockDatanode(Integer key) {
        //logger.debug("TransactionManagerHelper - isLockDatanode - start");
        String[] retStrs = null;
        try {
            if(!this.keyMapManager.checkError()) {
                String transactionCode = this.keyMapManager.getLockedTransactionCode(key);
                if (transactionCode != null) {

                    retStrs = new String[3];
                    retStrs[0] = "32";
                    retStrs[1] = "true";
                    retStrs[2] = transactionCode;
                } else {
                    retStrs = new String[2];
                    retStrs[0] = "32";
                    retStrs[1] = "false";
                }
            } else {
                    retStrs = new String[2];
                    retStrs[0] = "32";
                    retStrs[1] = "false";
            }
        } catch (Exception e) {
            logger.error("TransactionManagerHelper - isLockDatanode - Error", e);
            retStrs = new String[2];
            retStrs[0] = "32";
            retStrs[1] = "false";
        }
        //logger.debug("TransactionManagerHelper - isLockDatanode - end");
        return retStrs;
    }

}