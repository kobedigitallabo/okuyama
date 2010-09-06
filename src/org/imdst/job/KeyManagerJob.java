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

        String queueSizeStr = (String)super.getPropertiesValue(ImdstDefine.Prop_KeyNodeMaxConnectParallelQueue);
        if (queueSizeStr != null && this.maxConnectParallelExecution > (Integer.parseInt(queueSizeStr) * 2)) {
            this.maxConnectParallelQueue = Integer.parseInt(queueSizeStr);
        } else if (queueSizeStr != null) {
            this.maxConnectParallelQueue = this.maxConnectParallelExecution / 2;
        }
        this.maxConnectParallelQueueNames = new String[new Long(this.maxConnectParallelQueue).intValue()];



        sizeStr = (String)super.getPropertiesValue(ImdstDefine.Prop_KeyNodeMaxAcceptParallelExecution);
        if (sizeStr != null && Integer.parseInt(sizeStr) > maxAcceptParallelExecution) {
            maxAcceptParallelExecution = Integer.parseInt(sizeStr);
        }

        queueSizeStr = (String)super.getPropertiesValue(ImdstDefine.Prop_KeyNodeMaxAcceptParallelQueue);
        if (queueSizeStr != null && this.maxAcceptParallelExecution > (Integer.parseInt(queueSizeStr) * 2)) {
            this.maxAcceptParallelQueue = Integer.parseInt(queueSizeStr);
        } else if (queueSizeStr != null) {
            this.maxAcceptParallelQueue = this.maxAcceptParallelExecution / 2;
        }
        this.maxAcceptParallelQueueNames = new String[new Long(this.maxAcceptParallelQueue).intValue()];



        sizeStr = (String)super.getPropertiesValue(ImdstDefine.Prop_KeyNodeMaxWorkerParallelExecution);
        if (sizeStr != null && Integer.parseInt(sizeStr) > maxWorkerParallelExecution) {
            maxWorkerParallelExecution = Integer.parseInt(sizeStr);
        }

        queueSizeStr = (String)super.getPropertiesValue(ImdstDefine.Prop_KeyNodeMaxWorkerParallelQueue);
        if (queueSizeStr != null && this.maxWorkerParallelExecution > (Integer.parseInt(queueSizeStr) * 2)) {
            this.maxWorkerParallelQueue = Integer.parseInt(queueSizeStr);
        } else if (queueSizeStr != null) {
            this.maxWorkerParallelQueue = this.maxWorkerParallelExecution / 2;
        }
        this.maxWorkerParallelQueueNames = new String[new Long(this.maxWorkerParallelQueue).intValue()];



        logger.debug("KeyManagerJob - initJob - end");
    }

    /** 
     * Jobメイン処理定義
     */
    public String executeJob(String optionParam) throws BatchException {
        logger.debug("KeyManagerJob - executeJob - start");

        String ret = SUCCESS;

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
            for (int i = 0; i < this.maxConnectParallelQueue; i++) {
                super.createUniqueHelperParamQueue("KeyManagerConnectHelper" + this.myPrefix + i, 7000);
                this.maxConnectParallelQueueNames[i] = "KeyManagerConnectHelper" + this.myPrefix + i;
            }

            for (int i = 0; i < this.maxAcceptParallelQueue; i++) {
                super.createUniqueHelperParamQueue("KeyManagerAcceptHelper" + this.myPrefix + i, 5000);
                this.maxAcceptParallelQueueNames[i] = "KeyManagerAcceptHelper" + this.myPrefix + i;
            }

            for (int i = 0; i < this.maxWorkerParallelQueue; i++) {
                super.createUniqueHelperParamQueue("KeyManagerHelper" + this.myPrefix + i, 5000);
                this.maxWorkerParallelQueueNames[i] = "KeyManagerHelper" + this.myPrefix + i;
            }



            // 監視スレッド起動
            for (int i = 0; i < maxConnectParallelExecution; i++) {
                queueIndex = (i+1) % this.maxConnectParallelQueue;

                Object[] queueParam = new Object[2];
                queueParam[0] = "KeyManagerConnectHelper" + this.myPrefix + queueIndex;
                queueParam[1] = this.maxAcceptParallelQueueNames;
                super.executeHelper("KeyManagerConnectHelper", queueParam);
            }

            for (int i = 0; i < maxAcceptParallelExecution; i++) {
                queueIndex = (i+1) % this.maxAcceptParallelQueue;

                Object[] queueParam = new Object[2];
                queueParam[0] = "KeyManagerAcceptHelper" + this.myPrefix + queueIndex;
                queueParam[1] = this.maxWorkerParallelQueueNames;
                super.executeHelper("KeyManagerAcceptHelper", queueParam);
            }

            for (int i = 0; i < maxWorkerParallelExecution; i++) {
                queueIndex = (i+1) % this.maxWorkerParallelQueue;

                Object[] queueParam = new Object[3];
                queueParam[0] = this.keyMapManager;
                queueParam[1] = "KeyManagerHelper" + this.myPrefix + queueIndex;
                queueParam[2] = this.maxAcceptParallelQueueNames;
                super.executeHelper("KeyManagerHelper", queueParam);
            }


            // サーバソケットの生成
            this.serverSocket = new ServerSocket(this.portNo);
            // 共有領域にServerソケットのポインタを格納
            super.setJobShareParam(super.getJobName() + "_ServeSocket", this.serverSocket);

            // 処理開始
            logger.info("DataNodeServer-Accept-Start");

            while (true) {
                if (StatusUtil.getStatus() == 1 || StatusUtil.getStatus() == 2) break;
                try {

                    // クライアントからの接続待ち
                    accessCount++;
                    socket = serverSocket.accept();

                    Object[] helperParam = new Object[1];
                    helperParam[0] = socket;

                    // アクセス済みのソケットをキューに貯める
                    super.addSpecificationParameterQueue(keyManagerConnectHelperQueuePrefix + (accessCount % this.maxConnectParallelQueue), helperParam);


                    // 各スレッドが減少していないかを確かめる
                    if (super.getActiveHelperCount("KeyManagerConnectHelper") < (maxConnectParallelExecution / 2)) {
                        queueIndex = accessCount % this.maxConnectParallelQueue;

                        Object[] queueParam = new Object[2];
                        queueParam[0] = "KeyManagerConnectHelper" + this.myPrefix + queueIndex;
                        queueParam[1] = this.maxAcceptParallelQueueNames;
                        super.executeHelper("KeyManagerConnectHelper", queueParam);
                    }

                    if (super.getActiveHelperCount("KeyManagerAcceptHelper") < (maxAcceptParallelExecution / 2)) {
                        queueIndex = accessCount % this.maxAcceptParallelQueue;

                        Object[] queueParam = new Object[2];
                        queueParam[0] = "KeyManagerAcceptHelper" + this.myPrefix + queueIndex;
                        queueParam[1] = this.maxWorkerParallelQueueNames;
                        super.executeHelper("KeyManagerAcceptHelper", queueParam);
                    }

                    if (super.getActiveHelperCount("KeyManagerHelper") < (maxWorkerParallelExecution / 2)) {
                        queueIndex = accessCount % this.maxWorkerParallelQueue;

                        Object[] queueParam = new Object[3];
                        queueParam[0] = this.keyMapManager;
                        queueParam[1] = "KeyManagerHelper" + this.myPrefix + queueIndex;
                        queueParam[2] = this.maxAcceptParallelQueueNames;
                        super.executeHelper("KeyManagerHelper", queueParam);
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