package org.imdst.job;

import java.util.*;
import java.io.*;
import java.net.*;

import org.batch.lang.BatchException;
import org.batch.job.AbstractJob;
import org.batch.job.AbstractHelper;
import org.batch.job.IJob;
import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.imdst.util.KeyMapManager;
import org.imdst.util.StatusUtil;
import org.imdst.util.ImdstDefine;
import org.imdst.util.DataDispatcher;
import org.imdst.util.StatusUtil;

/**
 * MasterNode、自身でポートを上げて待ち受ける<br>
 * クライアントからの要求をHelperに依頼する.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class MasterManagerJob extends AbstractJob implements IJob {

    // デフォルト起動ポート
    private int portNo = 8888;

    // Accept後のコネクター作成までの処理並列数
    private int maxConnectParallelExecution = 10;
    private long maxConnectParallelQueue = 3;
    private String[] maxConnectParallelQueueNames = null;

    // Acceptコネクタがリードできるかを監視する処理並列数
    private int maxAcceptParallelExecution = 15;
    private long maxAcceptParallelQueue = 5;
    private String[] maxAcceptParallelQueueNames = null;

    // 実際のデータ処理並列数
    private int maxWorkerParallelExecution = 15;
    private int maxWorkerParallelQueue = 5;
    private String[] maxWorkerParallelQueueNames = null;


    // ロードバランス設定
    private boolean loadBalance = false;
    private boolean[] blanceModes = new boolean[4];
    private int nowBalanceIdx = 0;

    private boolean transactionMode = false;

    // 起動モード(okuyama=okuyamaオリジナル, memcache=memcache)
    private String mode = "okuyama";

    // サーバーソケット
    ServerSocket serverSocket = null;


    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(MasterManagerJob.class);


    /**
     * 初期化処理.<br>
     *
     * @param initValue
     */
    public void initJob(String initValue) {
        logger.debug("MasterManagerJob - initJob - start");

        this.portNo = Integer.parseInt(initValue);

        String sizeStr = (String)super.getPropertiesValue(ImdstDefine.Prop_MasterNodeMaxConnectParallelExecution);
        if (sizeStr != null && Integer.parseInt(sizeStr) > this.maxConnectParallelExecution) {
            this.maxConnectParallelExecution = Integer.parseInt(sizeStr);
        }

        String queueSizeStr = (String)super.getPropertiesValue(ImdstDefine.Prop_MasterNodeMaxConnectParallelQueue);
        if (queueSizeStr != null && this.maxConnectParallelExecution > (Integer.parseInt(queueSizeStr) * 2)) {
            this.maxConnectParallelQueue = Integer.parseInt(queueSizeStr);
        } else if (queueSizeStr != null) {
            this.maxConnectParallelQueue = this.maxConnectParallelExecution / 2;
        }
        this.maxConnectParallelQueueNames = new String[new Long(this.maxConnectParallelQueue).intValue()];



        sizeStr = (String)super.getPropertiesValue(ImdstDefine.Prop_MasterNodeMaxAcceptParallelExecution);
        if (sizeStr != null && Integer.parseInt(sizeStr) > this.maxAcceptParallelExecution) {
            this.maxAcceptParallelExecution = Integer.parseInt(sizeStr);
        }

        queueSizeStr = (String)super.getPropertiesValue(ImdstDefine.Prop_MasterNodeMaxAcceptParallelQueue);
        if (queueSizeStr != null && this.maxAcceptParallelExecution > (Integer.parseInt(queueSizeStr) * 2)) {
            this.maxAcceptParallelQueue = Integer.parseInt(queueSizeStr);
        } else if (queueSizeStr != null) {
            this.maxAcceptParallelQueue = this.maxAcceptParallelExecution / 2;
        }
        this.maxAcceptParallelQueueNames = new String[new Long(this.maxAcceptParallelQueue).intValue()];



        sizeStr = (String)super.getPropertiesValue(ImdstDefine.Prop_MasterNodeMaxWorkerParallelExecution);
        if (sizeStr != null) this.maxWorkerParallelExecution = Integer.parseInt(sizeStr);

        queueSizeStr = (String)super.getPropertiesValue(ImdstDefine.Prop_MasterNodeMaxWorkerParallelQueue);
        if (queueSizeStr != null && this.maxWorkerParallelExecution > (Integer.parseInt(queueSizeStr) * 2)) {
            this.maxWorkerParallelQueue = Integer.parseInt(queueSizeStr);
        } else if (queueSizeStr != null) {
            this.maxWorkerParallelQueue = this.maxWorkerParallelExecution / 2;
        }
        this.maxWorkerParallelQueueNames = new String[new Long(this.maxWorkerParallelQueue).intValue()];


        // データ取得時に使用するノード使用割合を決定
        blanceModes[0] = false;
        blanceModes[1] = false;
        blanceModes[2] = true;
        blanceModes[3] = true;

        logger.debug("MasterManagerJob - initJob - end");
    }


    /**
     * メイン処理.<br>
     * ServerSocketをOpenしてクライアントを待ち受ける.<br>
     * アルゴリズムに合わせて処理を呼びわけ.<br>
     *
     * @param optionParam
     * @return String
     * @throw Exception
     */
    public String executeJob(String optionParam) throws BatchException {
        logger.debug("MasterManagerJob - executeJob - start");
        String ret = SUCCESS;

        try{

            // KeyMapNode情報の初期化完了を確認
            if(DataDispatcher.isStandby() && StatusUtil.isStandby()) {
                if (StatusUtil.getDistributionAlgorithm().equals(ImdstDefine.dispatchModeMod)) {

                    // Modアルゴリズム
                    ret = this.executeModMasterServer(optionParam);
                } else if (StatusUtil.getDistributionAlgorithm().equals(ImdstDefine.dispatchModeConsistentHash)) {
                    ret = executeConsistentHashMasterServer(optionParam);
                }
            }
        } catch(Exception e) {
            logger.error("MasterManagerJob - executeJob - Error", e);
            throw new BatchException(e);
        }

        //logger.debug("MasterManagerJob - executeJob - end");
        return ret;
    }


    /**
     * Modアルゴリズム処理.<br>
     * ServerSocketをOpenしてクライアントを待ち受ける.<br>
     * 処理スレッドとチェーン用のQueueを作成.<br>
     *
     * @param optionParam
     * @return String
     * @throw Exception
     */
    private String executeModMasterServer (String optionParam) throws Exception {
        String ret = SUCCESS;

        Object[] helperParams = null;
        int paramSize = 8;

        String[] transactionManagerInfos = null;

        Socket socket = null;
        long queueIndex = 0L;

        long accessCount = 0L;

        try{

            // モードを決定
            if (optionParam != null && !optionParam.trim().equals("")) this.mode = optionParam;

            // ロードバランス設定
            String loadBalanceStr = (String)super.getPropertiesValue(ImdstDefine.Prop_LoadBalanceMode);
            if (loadBalanceStr != null) {
                loadBalance = new Boolean(loadBalanceStr).booleanValue();
            }


            // サーバソケットの生成
            this.serverSocket = new ServerSocket(this.portNo);
            // 共有領域にServerソケットのポインタを格納
            super.setJobShareParam(super.getJobName() + "_ServeSocket", this.serverSocket);


            // オリジナルのキュー領域を作成
            for (int i = 0; i < this.maxConnectParallelQueue; i++) {
                super.createUniqueHelperParamQueue("MasterManagerConnectHelper" + i, 7000);
                this.maxConnectParallelQueueNames[i] = "MasterManagerConnectHelper" + i;
            }

            for (int i = 0; i < this.maxAcceptParallelQueue; i++) {
                super.createUniqueHelperParamQueue("MasterManagerAcceptHelper" + i, 4000);
                this.maxAcceptParallelQueueNames[i] = "MasterManagerAcceptHelper" + i;
            }

            for (int i = 0; i < this.maxWorkerParallelQueue; i++) {
                super.createUniqueHelperParamQueue("MasterManagerHelper" + i, 4000);
                this.maxWorkerParallelQueueNames[i] = "MasterManagerHelper" + i;
            }


            // Worker用Helperを起動
            // 引数は取得用Queueプレフィックス
            // 引数は追加用Queue数
            for (int i = 0; i < this.maxConnectParallelExecution; i++) {
                queueIndex = (i+1) % this.maxConnectParallelQueue;
                helperParams = new Object[2];
                helperParams[0] = "MasterManagerConnectHelper" + queueIndex;
                helperParams[1] = this.maxAcceptParallelQueueNames;
                super.executeHelper("MasterManagerConnectHelper", helperParams);
            }

            for (int i = 0; i < this.maxAcceptParallelExecution; i++) {
                queueIndex = (i+1) % this.maxAcceptParallelQueue;
                helperParams = new Object[2];
                helperParams[0] = "MasterManagerAcceptHelper" + queueIndex;
                helperParams[1] = this.maxWorkerParallelQueueNames;
                super.executeHelper("MasterManagerAcceptHelper", helperParams);
            }

            for (int i = 0; i < this.maxWorkerParallelExecution; i++) {
                queueIndex = (i+1) % this.maxWorkerParallelQueue;

                helperParams = new Object[paramSize];
                helperParams[0] = null;
                helperParams[1] = null;
                helperParams[2] = this.mode;
                if (loadBalance) helperParams[3] = new Boolean(this.blanceModes[nowBalanceIdx]);
                helperParams[4] = StatusUtil.isTransactionMode();
                helperParams[5] = StatusUtil.getTransactionNode();
                helperParams[6] = "MasterManagerHelper" + queueIndex;
                helperParams[7] = this.maxAcceptParallelQueueNames;
                super.executeHelper("MasterManagerHelper", helperParams);

                this.nowBalanceIdx++;
                if (this.blanceModes.length == this.nowBalanceIdx) this.nowBalanceIdx = 0;
            }


            // メイン処理開始
            while (true) {
                if (StatusUtil.getStatus() == 1 || StatusUtil.getStatus() == 2) break;
                try {

                    // クライアントからの接続待ち
                    socket = serverSocket.accept();
                    accessCount++;

                    Object[] queueParam = new Object[1];
                    queueParam[0] = socket;

                    // アクセス済みのソケットをキューに貯める
                    super.addSpecificationParameterQueue("MasterManagerConnectHelper" + (accessCount%this.maxConnectParallelQueue), queueParam);


                    // TODO:以下は別スレッドに切り出すべき
                    // 各スレッドが減少していないかを確かめる
                    if (super.getActiveHelperCount("MasterManagerConnectHelper") < (maxConnectParallelExecution / 2)) {
                        queueIndex = (accessCount) % this.maxConnectParallelExecution;

                        helperParams = new Object[2];
                        helperParams[0] = "MasterManagerConnectHelper" + queueIndex;
                        helperParams[1] = this.maxAcceptParallelQueueNames;

                        super.executeHelper("MasterManagerConnectHelper", helperParams);
                    }

                    if (super.getActiveHelperCount("MasterManagerAcceptHelper") < (maxAcceptParallelExecution / 2)) {
                        queueIndex = (accessCount) % this.maxAcceptParallelQueue;

                        helperParams = new Object[2];
                        helperParams[0] = "MasterManagerAcceptHelper" + queueIndex;
                        helperParams[1] = this.maxWorkerParallelQueueNames;

                        super.executeHelper("MasterManagerAcceptHelper", helperParams);
                    }

                    if (super.getActiveHelperCount("MasterManagerHelper") < (maxWorkerParallelExecution / 2)) {
                        queueIndex = (accessCount) % this.maxWorkerParallelExecution;

                        helperParams = new Object[paramSize];
                        helperParams[0] = null;
                        helperParams[1] = null;
                        helperParams[2] = this.mode;
                        if (loadBalance) helperParams[3] = new Boolean(this.blanceModes[nowBalanceIdx]);
                        helperParams[4] = StatusUtil.isTransactionMode();
                        helperParams[5] = StatusUtil.getTransactionNode();
                        helperParams[6] = "MasterManagerHelper" + queueIndex;
                        helperParams[7] = this.maxAcceptParallelQueueNames;

                        super.executeHelper("MasterManagerHelper", helperParams);
                        this.nowBalanceIdx++;
                        if (this.blanceModes.length == this.nowBalanceIdx) this.nowBalanceIdx = 0;
                    }
                } catch (Exception e) {
                    if (StatusUtil.getStatus() == 2) {
                        logger.info("MasterManagerJob - executeJob - ServerEnd");
                        break;
                    }
                    logger.error(e);
                }
            }
        } catch(Exception e) {
            logger.error("MasterManagerJob - executeJob - Error", e);
            throw new BatchException(e);
        }

        //logger.debug("MasterManagerJob - executeJob - end");
        return ret;
    }


    /**
     * ConsistentHashアルゴリズム処理.<br>
     * ServerSocketをOpenしてクライアントを待ち受ける.<br>
     * executeModMasterServerを呼び出しているだけ.<br>
     *
     * @param optionParam
     * @return String
     * @throw Exception
     */
    private String executeConsistentHashMasterServer (String optionParam) throws Exception {
        return executeModMasterServer(optionParam);
    }
}