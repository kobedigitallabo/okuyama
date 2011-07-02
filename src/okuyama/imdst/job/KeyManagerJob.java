package okuyama.imdst.job;

import java.io.*;
import java.util.zip.Deflater;
import java.net.*;
import java.util.concurrent.atomic.AtomicInteger;

import okuyama.base.lang.BatchException;
import okuyama.base.job.AbstractJob;
import okuyama.base.job.AbstractHelper;
import okuyama.base.job.IJob;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;

import okuyama.imdst.util.JavaSystemApi;
import okuyama.imdst.util.KeyMapManager;
import okuyama.imdst.util.ImdstDefine;
import okuyama.imdst.util.StatusUtil;
import okuyama.imdst.util.JavaSystemApi;

/**
 * keyとValueの関係を管理するJob、自身でポートを上げて待ち受ける.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class KeyManagerJob extends AbstractJob implements IJob {

    private String myPrefix = null;

    // デフォルト起動設定
    private String bindIpAddress = null;
    private int portNo = 5553;
    private int backLog = 50;

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
        logger.info("okuyama DataNode Initialization Start ...");
        System.out.println("okuyama DataNode Initialization Start ...");

        // BindするIP及び、Port、Backlog設定
        if (initValue.indexOf(":") != -1) {

            String[] splitInitVal = initValue.split(":");
            this.bindIpAddress = splitInitVal[0];

            if (splitInitVal[1].indexOf("&") != -1) {

                splitInitVal = splitInitVal[1].split("&");
                this.portNo = Integer.parseInt(splitInitVal[0]);
                this.backLog = Integer.parseInt(splitInitVal[1]);
            } else {

                this.portNo = Integer.parseInt(splitInitVal[1]);
            }
        } else if (initValue.indexOf("&") != -1){

            String[] splitInitVal = initValue.split("&");
            this.portNo = Integer.parseInt(splitInitVal[0]);
            this.backLog = Integer.parseInt(splitInitVal[1]);
        } else {

            this.portNo = Integer.parseInt(initValue);
        }


        ////// 設定反映 //////

        // データ永続化Mapのタイプを設定(現在はokuyama.imdst.util.serializemap.SerializeMapか通常のConcurrentHashMapベースのMap
        String dataSaveMapType = (String)super.getPropertiesValue(ImdstDefine.Prop_DataSaveMapType);
        if (dataSaveMapType != null &&  dataSaveMapType.toLowerCase().equals(ImdstDefine.Prop_DataSaveMapTypeSerialize)) {
            // SerializeMap
            ImdstDefine.useSerializeMap = true;
            ImdstDefine.serializerClassName = (String)super.getPropertiesValue(ImdstDefine.Prop_SerializerClassName);
        }


        // データ圧縮設定
        String saveDataCompress = (String)super.getPropertiesValue(ImdstDefine.Prop_SaveDataCompress);
        if (saveDataCompress != null &&  saveDataCompress.toLowerCase().equals("false")) {
            // 圧縮しない
            ImdstDefine.saveValueCompress = false;
        } else {

            String saveDataCompressType = (String)super.getPropertiesValue(ImdstDefine.Prop_SaveDataCompressType);
            if (saveDataCompress != null && !saveDataCompressType.equals("")) { 
                if (saveDataCompressType.equals("1")) {
                    ImdstDefine.valueCompresserLevel = Deflater.BEST_SPEED;
                } else if (saveDataCompressType.equals("9")) {
                    ImdstDefine.valueCompresserLevel = Deflater.BEST_COMPRESSION;
                }
            }
        }


        // データ永続化トランザクションファイルの遅延設定
        String transactionFileCommit = (String)super.getPropertiesValue(ImdstDefine.Prop_DataSaveTransactionFileEveryCommit);
        if (transactionFileCommit != null &&  transactionFileCommit.toLowerCase().equals("false")) {
            // 遅延させる
            ImdstDefine.dataTransactionFileFlushTiming = false;
        }

        // 共有データファイルの書き換え遅延設定
        String shareDataFileDelay = (String)super.getPropertiesValue(ImdstDefine.Prop_ShareDataFileWriteDelayFlg);
        if (shareDataFileDelay != null &&  shareDataFileDelay.toLowerCase().equals("true")) {
            ImdstDefine.dataFileWriteDelayFlg = true;
            String shareDataFileMaxDelayStr = (String)super.getPropertiesValue(ImdstDefine.Prop_ShareDataFileMaxDelayCount);
            if (shareDataFileMaxDelayStr != null) {
                try {
                    int shareDataFileMaxDelayInt = new Integer(shareDataFileMaxDelayStr).intValue();
                    if (shareDataFileMaxDelayInt > 0 && shareDataFileMaxDelayInt < 1000000) {
                        ImdstDefine.dataFileWriteDelayMaxSize = shareDataFileMaxDelayInt;
                    }
                } catch (Exception ce) {
                }
            }
        }

        // メモリモードで稼働している場合にメモリに書き出すValue単位のMaxサイズ(byte)
        String maxMemoryStoreSize = (String)super.getPropertiesValue(ImdstDefine.Prop_SaveDataMemoryStoreLimitSize);
        try {
            if (maxMemoryStoreSize != null && Integer.parseInt(maxMemoryStoreSize) > 0) {
                ImdstDefine.bigValueFileStoreUse = true;
                ImdstDefine.memoryStoreLimitSize = Integer.parseInt(maxMemoryStoreSize);
            }
        } catch (Exception ce2) {
        }

        // 自身のJOB名取出し
        this.myPrefix = super.getJobName();

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
            InetSocketAddress bindAddress= null;
            if (this.bindIpAddress == null) {
                bindAddress= new InetSocketAddress(this.portNo);
            } else {
                bindAddress= new InetSocketAddress(this.bindIpAddress, this.portNo);
            }

            this.serverSocket = new ServerSocket();
            this.serverSocket.bind(bindAddress, this.backLog);

            // 共有領域にServerソケットのポインタを格納
            super.setJobShareParam(super.getJobName() + "_ServeSocket", this.serverSocket);


            // 処理開始
            logger.info("okuyama DataNode Initialization End ...");
            System.out.println("okuyama DataNode Initialization End ...");

            logger.info("okuyama DataNode start ...");
            logger.info("listening on " + bindAddress);
            System.out.println("okuyama DataNode start");
            System.out.println("listening on " + bindAddress);

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
                    logger.error("", e);
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

        String memoryLimitSize = null;
        int memoryLimitSizeInt = -1;
        String virtualStorageDirsStr = null;
        String[] virtualStorageDirs = null;

        try {
            keyMapFiles = optionParam.split(",");

            // KeyMapManagerの設定値を取得
            this.workFileMemoryModeStr = super.getPropertiesValue(super.getJobName() + ImdstDefine.Prop_MemoryMode);
            this.dataMemoryModeStr = super.getPropertiesValue(super.getJobName() + ImdstDefine.Prop_DataMemory);

            keyStoreForFileStr = super.getPropertiesValue(super.getJobName() + ImdstDefine.Prop_KeyMemory);
            if (keyStoreForFileStr != null || !keyStoreForFileStr.equals("")) {
                if (keyStoreForFileStr.equals("false")) {
                    keyStoreForFileFlg = true;
                }
            }

            keyStoreDirsStr = super.getPropertiesValue(super.getJobName() + ImdstDefine.Prop_KeyStoreDirs);
            if (keyStoreDirsStr != null) keyStoreDirs = keyStoreDirsStr.split(",");

            this.keySizeStr = super.getPropertiesValue(super.getJobName() + ImdstDefine.Prop_KeySize);

            // workファイルを保持するか判断
            if (workFileMemoryModeStr != null && workFileMemoryModeStr.equals("true")) workFileMemoryMode = true;
            // データをメモリに持つか判断
            if (dataMemoryModeStr != null && dataMemoryModeStr.equals("false")) dataMemoryMode = false;

            // データ保持予測件数
            if (keySizeStr != null) keySize = Integer.parseInt(keySizeStr);

            // 仮想ストレージ設定
            // トランザクションログを記録しかつ、Valueがメモリの場合のみ設定可能
            if (workFileMemoryMode == false && keyStoreForFileFlg == false) {
                memoryLimitSize = super.getPropertiesValue(super.getJobName() + ImdstDefine.Prop_MemoryLimitSize);
                if (memoryLimitSize != null && !memoryLimitSize.trim().equals("") && new Integer(memoryLimitSize).intValue() > 0) {

                    virtualStorageDirsStr = super.getPropertiesValue(super.getJobName() + ImdstDefine.Prop_VirtualStoreDirs);
                    if (virtualStorageDirsStr != null && !virtualStorageDirsStr.trim().equals("")) {
                        memoryLimitSizeInt = new Integer(memoryLimitSize).intValue();
                        virtualStorageDirs = virtualStorageDirsStr.split(",");
                    }
                }
            }

            // KeyMapManager初期化
            if (keyStoreForFileFlg) {

                // Key is FileStoreMode
                this.keyMapManager = new KeyMapManager(keyMapFiles[0], keyMapFiles[1], workFileMemoryMode, keySize, dataMemoryMode, keyStoreDirs);
            } else {

                // Key is MemoryStoreMode
                this.keyMapManager = new KeyMapManager(keyMapFiles[0], keyMapFiles[1], workFileMemoryMode, keySize, dataMemoryMode, memoryLimitSizeInt, virtualStorageDirs);
            }
            this.keyMapManager.start();

            // 設定情報以外の値が入っている場合は一旦停止
            if (this.keyMapManager.getSaveDataCount() > 50) {
                JavaSystemApi.manualGc();
                Thread.sleep(60000);
            }

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


            // KeyManagerHelper設定
            Object[] helperShareParams = new Object[(this.maxWorkerParallelQueue * 2)];

            queueIndex = 0;
            for (int i = 0; i < (this.maxWorkerParallelQueue * 2); i=i+2) {
                helperShareParams[i] = "Bind-KeyManagerHelper" + this.myPrefix + queueIndex;
                helperShareParams[i+1] = new AtomicInteger(0);
                queueIndex++;
            }

            queueIndex = 0;
            for (int i = 0; i < this.maxWorkerParallelExecution; i++) {
                queueIndex = (i+1) % this.maxWorkerParallelQueue;
                AtomicInteger queueBindHelperCount = (AtomicInteger)helperShareParams[(new Long(queueIndex).intValue()*2)+1];
                queueBindHelperCount.getAndIncrement();
                helperShareParams[(new Long(queueIndex).intValue()*2)+1] = queueBindHelperCount;
            }

            for (int i = 0; i < maxWorkerParallelExecution; i++) {
                queueIndex = (i+1) % this.maxWorkerParallelQueue;

                Object[] queueParam = new Object[4];
                queueParam[0] = this.keyMapManager;
                queueParam[1] = "KeyManagerHelper" + this.myPrefix + queueIndex;
                queueParam[2] = this.maxAcceptParallelQueueNames;
                queueParam[3] = "Bind-KeyManagerHelper" + this.myPrefix + queueIndex;

                if (i == 0) {

                    super.executeHelper("KeyManagerHelper", queueParam, true, helperShareParams);
                } else {
                    super.executeHelper("KeyManagerHelper", queueParam, true);
                }
            }
        } catch(Exception e) {
                e.printStackTrace();
        }
    }
}