package okuyama.imdst.job;

import java.io.*;
import java.net.*;

import okuyama.base.lang.BatchException;
import okuyama.base.job.AbstractJob;
import okuyama.base.job.AbstractHelper;
import okuyama.base.job.IJob;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.imdst.util.KeyMapManager;
import okuyama.imdst.util.ImdstDefine;
import okuyama.imdst.util.StatusUtil;


/**
 * keyとValueの関係を管理するJob、自身でポートを上げて待ち受ける.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyManagerJob extends AbstractJob implements IJob {

    private String myPrefix = null;

    // ポート番号
    private int portNo = 5554;


    private String keySizeStr = null;
    private int keySize = 500000;

    private boolean workFileMemoryMode = false;
    private String workFileMemoryModeStr = null;

    private boolean dataMemoryMode = true;
    private String dataMemoryModeStr = null;


    // Accept後のコネクター作成までの処理並列数
    private int maxConnectParallelExecution = 10;
    private long maxConnectParallelQueue = 3;
    private String[] maxConnectParallelQueueNames = null;


    // Acceptコネクタがリードできるかを監視する処理並列数
    private int maxAcceptParallelExecution = 4;
    private long maxAcceptParallelQueue = 2;
    private String[] maxAcceptParallelQueueNames = null;


    // 実際のデータ処理並列数
    private int maxWorkerParallelExecution = 4;
    private int maxWorkerParallelQueue = 2;
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
     * 初期化メソッド定義.<br>
     * 
     * @param initValue
     * @return 
     * @throws 
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
        if (queueSizeStr != null && this.maxConnectParallelExecution >= Integer.parseInt(queueSizeStr)) {
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
        if (queueSizeStr != null && this.maxAcceptParallelExecution >= Integer.parseInt(queueSizeStr)) {
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
        if (queueSizeStr != null && this.maxWorkerParallelExecution >= Integer.parseInt(queueSizeStr)) {
            this.maxWorkerParallelQueue = Integer.parseInt(queueSizeStr);
        } else if (queueSizeStr != null) {
            this.maxWorkerParallelQueue = this.maxWorkerParallelExecution / 2;
        }
        this.maxWorkerParallelQueueNames = new String[new Long(this.maxWorkerParallelQueue).intValue()];



        logger.debug("KeyManagerJob - initJob - end");
    }


    /**
     * Jobメイン処理定義.<br>
     *
     * @param
     * @return 
     * @throws 
     */
    public String executeJob(String optionParam) throws BatchException {
        logger.debug("KeyManagerJob - executeJob - start");

        String ret = SUCCESS;

        Socket socket = null;

        String keyManagerConnectHelperQueuePrefix = "KeyManagerConnectHelper" + this.myPrefix;

        long accessCount = 0L;


        try{

            // KeyMapManagerの設定値を初期化
            this.initDataPersistentConfig(optionParam);

            // オリジナルのキュー領域を作成
            this.initHelperTaskQueue();

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


    /**
     * データ永続化用のKeyMapManagerを初期化.<br>
     *
     * @param optionParam
     * @return 
     * @throws 
     */
    private void initDataPersistentConfig(String optionParam) {
        String[] keyMapFiles = null;
        String keyStoreForFileStr = null;
        boolean keyStoreForFileFlg = false;
        String keyStoreDirsStr = null;
        String[] keyStoreDirs = null;
        try {
            keyMapFiles = optionParam.split(",");

            // KeyMapManagerの設定値を取得
            this.workFileMemoryModeStr = super.getPropertiesValue(super.getJobName() + ".memoryMode");
            this.dataMemoryModeStr = super.getPropertiesValue(super.getJobName() + ".dataMemory");

            keyStoreForFileStr = super.getPropertiesValue(super.getJobName() + ".keyMemory");
            if (keyStoreForFileStr != null || !keyStoreForFileStr.equals("")) {
                if (keyStoreForFileStr.equals("false")) {
                    keyStoreForFileFlg = true;
                }
            }

            keyStoreDirsStr = super.getPropertiesValue(super.getJobName() + ".keyStoreDirs");
            if (keyStoreDirsStr != null) keyStoreDirs = keyStoreDirsStr.split(",");

            this.keySizeStr = super.getPropertiesValue(super.getJobName() + ".keySize");

            // workファイルを保持するか判断
            if (workFileMemoryModeStr != null && workFileMemoryModeStr.equals("true")) workFileMemoryMode = true;
            // データをメモリに持つか判断
            if (dataMemoryModeStr != null && dataMemoryModeStr.equals("false")) dataMemoryMode = false;

            // データ保持予測件数
            if (keySizeStr != null) keySize = Integer.parseInt(keySizeStr);

            // KeyMapManager初期化
            if (keyStoreForFileFlg) {

                // Key is FileStoreMode
                this.keyMapManager = new KeyMapManager(keyMapFiles[0], keyMapFiles[1], workFileMemoryMode, keySize, dataMemoryMode, keyStoreDirs);
            } else {

                // Key is MemoryStoreMode
                this.keyMapManager = new KeyMapManager(keyMapFiles[0], keyMapFiles[1], workFileMemoryMode, keySize, dataMemoryMode);
            }
            this.keyMapManager.start();
        } catch(Exception e) {
                e.printStackTrace();
        }
    }


    /**
     * Helper用のQueue領域を生成する.<br>
     *
     * @param
     * @return 
     * @throws 
     */
    private void initHelperTaskQueue() {
        long queueIndex = 0L;
        try {
            
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
                super.executeHelper("KeyManagerConnectHelper", queueParam, true);
            }

            for (int i = 0; i < maxAcceptParallelExecution; i++) {
                queueIndex = (i+1) % this.maxAcceptParallelQueue;

                Object[] queueParam = new Object[2];
                queueParam[0] = "KeyManagerAcceptHelper" + this.myPrefix + queueIndex;
                queueParam[1] = this.maxWorkerParallelQueueNames;
                super.executeHelper("KeyManagerAcceptHelper", queueParam, true);
            }

            for (int i = 0; i < maxWorkerParallelExecution; i++) {
                queueIndex = (i+1) % this.maxWorkerParallelQueue;

                Object[] queueParam = new Object[3];
                queueParam[0] = this.keyMapManager;
                queueParam[1] = "KeyManagerHelper" + this.myPrefix + queueIndex;
                queueParam[2] = this.maxAcceptParallelQueueNames;
                super.executeHelper("KeyManagerHelper", queueParam, true);
            }
        } catch(Exception e) {
                e.printStackTrace();
        }
    }
}