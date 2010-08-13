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

    // Acceptコネクタがリードできるかを監視する処理並列数
    private int maxAcceptParallelExecution = 15;

    // 実際のデータ処理並列数
    private int maxWorkerParallelExecution = 15;

    private long multiQueue = 3;

    // ロードバランス設定
    private boolean loadBalance = false;
    private boolean blanceMode = false;

    private boolean transactionMode = false;

    // 起動モード(okuyama=okuyamaオリジナル, memcache=memcache)
    private String mode = "okuyama";

    // サーバーソケット
    ServerSocket serverSocket = null;


    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(MasterManagerJob.class);

    // 初期化メソッド定義
    public void initJob(String initValue) {
        logger.debug("MasterManagerJob - initJob - start");

        this.portNo = Integer.parseInt(initValue);

        String sizeStr = (String)super.getPropertiesValue(ImdstDefine.Prop_MasterNodeMaxConnectParallelExecution);
        if (sizeStr != null && Integer.parseInt(sizeStr) > maxConnectParallelExecution) {
            maxConnectParallelExecution = Integer.parseInt(sizeStr);
        }

        sizeStr = (String)super.getPropertiesValue(ImdstDefine.Prop_MasterNodeMaxAcceptParallelExecution);
        if (sizeStr != null && Integer.parseInt(sizeStr) > maxAcceptParallelExecution) {
            maxAcceptParallelExecution = Integer.parseInt(sizeStr);
        }

        sizeStr = (String)super.getPropertiesValue(ImdstDefine.Prop_MasterNodeMaxWorkerParallelExecution);
        if (sizeStr != null) maxWorkerParallelExecution = Integer.parseInt(sizeStr);

        logger.debug("MasterManagerJob - initJob - end");
    }

    // Jobメイン処理定義
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
     * 1クライアント1スレッド型.<br>
     */
    private String executeModMasterServer (String optionParam) throws Exception {
        String ret = SUCCESS;

        Object[] helperParams = null;
        int paramSize = 7;

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
            for (int i = 0; i < this.multiQueue; i++) {
                super.createUniqueHelperParamQueue("MasterManagerConnectHelper" + i, 7000);
                super.createUniqueHelperParamQueue("MasterManagerAcceptHelper" + i, 7000);
                super.createUniqueHelperParamQueue("MasterManagerHelper" + i, 7000);
            }

            for (int i = 0; i < maxConnectParallelExecution; i++) {
                queueIndex = (i+1) % this.multiQueue;
                helperParams = new Object[1];
                helperParams[0] = new Long(queueIndex).toString();
                super.executeHelper("MasterManagerConnectHelper", helperParams);
            }

            for (int i = 0; i < maxAcceptParallelExecution; i++) {
                queueIndex = (i+1) % this.multiQueue;
                helperParams = new Object[1];
                helperParams[0] = new Long(queueIndex).toString();

                super.executeHelper("MasterManagerAcceptHelper", helperParams);
            }

            for (int i = 0; i < maxWorkerParallelExecution; i++) {
                queueIndex = (i+1) % this.multiQueue;

                helperParams = new Object[paramSize];
                helperParams[0] = null;
                helperParams[1] = null;
                helperParams[2] = this.mode;
                if (loadBalance) helperParams[3] = new Boolean(blanceMode);
                helperParams[4] = StatusUtil.isTransactionMode();
                helperParams[5] = StatusUtil.getTransactionNode();
                helperParams[6] = new Long(queueIndex).toString();
                super.executeHelper("MasterManagerHelper", helperParams);
                if (blanceMode) {
                    blanceMode = false;
                } else {
                    blanceMode = true;
                }
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
                    super.addSpecificationParameterQueue("MasterManagerConnectHelper" + (accessCount%this.multiQueue), queueParam);


                    // 各スレッドが減少していないかを確かめる
                    if (super.getActiveHelperCount("MasterManagerConnectHelper") < (maxConnectParallelExecution / 2)) {
                        queueIndex = (accessCount) % this.multiQueue;
                        helperParams = new Object[1];
                        helperParams[0] = new Long(queueIndex).toString();

                        super.executeHelper("MasterManagerConnectHelper", helperParams);
                    }

                    if (super.getActiveHelperCount("MasterManagerAcceptHelper") < (maxAcceptParallelExecution / 2)) {
                        queueIndex = (accessCount) % this.multiQueue;
                        helperParams = new Object[1];
                        helperParams[0] = new Long(queueIndex).toString();

                        super.executeHelper("MasterManagerAcceptHelper", helperParams);
                    }

                    if (super.getActiveHelperCount("MasterManagerHelper") < (maxWorkerParallelExecution / 2)) {
                        queueIndex = (accessCount) % this.multiQueue;

                        helperParams = new Object[paramSize];
                        helperParams[0] = null;
                        helperParams[1] = null;
                        helperParams[2] = this.mode;
                        if (loadBalance) helperParams[3] = new Boolean(blanceMode);
                        helperParams[4] = StatusUtil.isTransactionMode();
                        helperParams[5] = StatusUtil.getTransactionNode();
                        helperParams[6] = new Long(queueIndex).toString();

                        super.executeHelper("MasterManagerHelper", helperParams);
                        if (blanceMode) {
                            blanceMode = false;
                        } else {
                            blanceMode = true;
                        }
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
}