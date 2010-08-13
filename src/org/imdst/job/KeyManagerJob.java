package org.imdst.job;

import java.io.*;
import java.net.*;

import org.batch.lang.BatchException;
import org.batch.job.AbstractJob;
import org.batch.job.AbstractHelper;
import org.batch.job.IJob;
import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
import org.imdst.util.KeyMapManager;
import org.imdst.util.ImdstDefine;
import org.imdst.util.StatusUtil;

/**
 * keyとValueの関係を管理するJob、自身でポートを上げて待ち受ける<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyManagerJob extends AbstractJob implements IJob {

    private String myPrefix = null;

    // ポート番号
    private int portNo = 5554;


    // Accept後のコネクター作成までの処理並列数
    private int maxConnectParallelExecution = 10;

    // Acceptコネクタがリードできるかを監視する処理並列数
    private int maxAcceptParallelExecution = 15;

    // 実際のデータ処理並列数
    private int maxWorkerParallelExecution = 15;

    private long multiQueue = 5;

    // サーバーソケット
    ServerSocket serverSocket = null;

    // KeyMapManagerインスタンス
    private KeyMapManager keyMapManager = null;

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(KeyManagerJob.class);

    /**
     * 初期化メソッド定義
     */
    public void initJob(String initValue) {
        logger.debug("KeyManagerJob - initJob - start");

        this.myPrefix = super.getJobName();

        this.portNo = Integer.parseInt(initValue);

        String sizeStr = (String)super.getPropertiesValue(ImdstDefine.Prop_KeyNodeMaxConnectParallelExecution);
        if (sizeStr != null && Integer.parseInt(sizeStr) > maxConnectParallelExecution) {
            maxConnectParallelExecution = Integer.parseInt(sizeStr);
        }

        sizeStr = (String)super.getPropertiesValue(ImdstDefine.Prop_KeyNodeMaxAcceptParallelExecution);
        if (sizeStr != null && Integer.parseInt(sizeStr) > maxAcceptParallelExecution) {
            maxAcceptParallelExecution = Integer.parseInt(sizeStr);
        }

        sizeStr = (String)super.getPropertiesValue(ImdstDefine.Prop_KeyNodeMaxWorkerParallelExecution);
        if (sizeStr != null && Integer.parseInt(sizeStr) > maxWorkerParallelExecution) {
            maxWorkerParallelExecution = Integer.parseInt(sizeStr);
        }

        logger.debug("KeyManagerJob - initJob - end");
    }

    /** 
     * Jobメイン処理定義
     */
    public String executeJob(String optionParam) throws BatchException {
        logger.debug("KeyManagerJob - executeJob - start");

        String ret = SUCCESS;

        Object[] helperParams = null;
        String[] keyMapFiles = null;

        String keySizeStr = null;
        int keySize = 500000;

        boolean workFileMemoryMode = false;
        String workFileMemoryModeStr = null;

        boolean dataMemoryMode = true;
        String dataMemoryModeStr = null;

        Socket socket = null;

        String keyManagerConnectHelperQueuePrefix = "KeyManagerConnectHelper" + this.myPrefix;
        long queueIndex = 0L;


        long accessCount = 0L;

        try{

            // Option値を分解
            keyMapFiles = optionParam.split(",");

            // KeyMapManagerの設定値を取得
            workFileMemoryModeStr = super.getPropertiesValue(super.getJobName() + ".memoryMode");
            dataMemoryModeStr = super.getPropertiesValue(super.getJobName() + ".dataMemory");
            keySizeStr = super.getPropertiesValue(super.getJobName() + ".keySize");

            // workファイルを保持するか判断
            if (workFileMemoryModeStr != null && workFileMemoryModeStr.equals("true")) workFileMemoryMode = true;
            // データをメモリに持つか判断
            if (dataMemoryModeStr != null && dataMemoryModeStr.equals("false")) dataMemoryMode = false;
            if (keySizeStr != null) keySize = Integer.parseInt(keySizeStr);

            this.keyMapManager = new KeyMapManager(keyMapFiles[0], keyMapFiles[1], workFileMemoryMode, keySize, dataMemoryMode);
            this.keyMapManager.start();


            // オリジナルのキュー領域を作成
            // オリジナルのキュー領域を作成
            for (int i = 0; i < this.multiQueue; i++) {
                super.createUniqueHelperParamQueue("KeyManagerConnectHelper" + this.myPrefix + i, 4000);
                super.createUniqueHelperParamQueue("KeyManagerAcceptHelper" + this.myPrefix + i, 6000);
                super.createUniqueHelperParamQueue("KeyManagerHelper" + this.myPrefix + i, 6000);
            }


            // 監視スレッド起動
            for (int i = 0; i < maxConnectParallelExecution; i++) {
                queueIndex = (i+1) % this.multiQueue;

                Object[] prefixParam = new Object[1];
                prefixParam[0] = this.myPrefix + queueIndex;
                super.executeHelper("KeyManagerConnectHelper", prefixParam);
            }

            for (int i = 0; i < maxAcceptParallelExecution; i++) {
                queueIndex = (i+1) % this.multiQueue;

                Object[] prefixParam = new Object[1];
                prefixParam[0] = this.myPrefix + queueIndex;
                super.executeHelper("KeyManagerAcceptHelper", prefixParam);
            }

            for (int i = 0; i < maxWorkerParallelExecution; i++) {
                queueIndex = (i+1) % this.multiQueue;

                helperParams = new Object[2];
                helperParams[0] = this.keyMapManager;
                helperParams[1] = this.myPrefix + queueIndex;
                super.executeHelper("KeyManagerHelper", helperParams);
            }


            // サーバソケットの生成
            this.serverSocket = new ServerSocket(this.portNo);
            // 共有領域にServerソケットのポインタを格納
            super.setJobShareParam(super.getJobName() + "_ServeSocket", this.serverSocket);

            while (true) {
                if (StatusUtil.getStatus() == 1 || StatusUtil.getStatus() == 2) break;
                try {

                    // クライアントからの接続待ち
                    accessCount++;
                    socket = serverSocket.accept();

                    Object[] queueParam = new Object[1];
                    queueParam[0] = socket;

                    // アクセス済みのソケットをキューに貯める
                    queueIndex = accessCount % this.multiQueue;
                    super.addSpecificationParameterQueue(keyManagerConnectHelperQueuePrefix + queueIndex, queueParam);


                    // 各スレッドが減少していないかを確かめる
                    if (super.getActiveHelperCount("KeyManagerConnectHelper") < (maxConnectParallelExecution / 2)) {
                        queueIndex = accessCount % this.multiQueue;
                        Object[] prefixParam = new Object[1];
                        prefixParam[0] = this.myPrefix + queueIndex;
                        super.executeHelper("KeyManagerConnectHelper", prefixParam);
                    }

                    if (super.getActiveHelperCount("KeyManagerAcceptHelper") < (maxAcceptParallelExecution / 2)) {
                        queueIndex = accessCount % this.multiQueue;
                        Object[] prefixParam = new Object[1];
                        prefixParam[0] = this.myPrefix + queueIndex;
                        super.executeHelper("KeyManagerAcceptHelper", prefixParam);
                    }

                    if (super.getActiveHelperCount("KeyManagerHelper") < (maxWorkerParallelExecution / 2)) {
                        queueIndex = accessCount % this.multiQueue;
                        helperParams = new Object[2];
                        helperParams[0] = this.keyMapManager;
                        helperParams[1] = this.myPrefix + queueIndex;
                        super.executeHelper("KeyManagerHelper", helperParams);
                    }
                } catch (Exception e) {
                    if (StatusUtil.getStatus() == 2) {
                        logger.info("KeyManagerJob - executeJob - ServerEnd");
                        break;
                    }
                    logger.error(e);
                }
            }
        } catch(Exception e) {
            logger.error("KeyManagerJob - executeJob - Error", e);
            throw new BatchException(e);
        }

        //logger.debug("KeyManagerJob - executeJob - end");
        return ret;
    }


}