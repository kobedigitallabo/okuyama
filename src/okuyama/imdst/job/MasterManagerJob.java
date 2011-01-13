package okuyama.imdst.job;

import java.util.*;
import java.io.*;
import java.net.*;
import java.util.concurrent.atomic.AtomicInteger;


import okuyama.base.lang.BatchException;
import okuyama.base.job.AbstractJob;
import okuyama.base.job.AbstractHelper;
import okuyama.base.job.IJob;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.imdst.util.KeyMapManager;
import okuyama.imdst.util.StatusUtil;
import okuyama.imdst.util.ImdstDefine;
import okuyama.imdst.util.DataDispatcher;
import okuyama.imdst.util.StatusUtil;

import com.sun.mail.util.BASE64EncoderStream;

/**
 * MasterNode、自身でポートを上げて待ち受ける<br>
 * クライアントからの要求をHelperに依頼する.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class MasterManagerJob extends AbstractJob implements IJob {

    // デフォルト起動設定
    private String bindIpAddress = null;
    private int portNo = 8888;
    private int backLog = 50;

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
    private Boolean noBalance = new Boolean(false);
    private int balancePattern = 10;
    private Boolean[] balanceModes = null;
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
        logger.info("okuyama MasterNode Initialization Start ...");
        System.out.println("okuyama MasterNode Initialization Start ...");

        // バインドIPおよび、ポート番号、BackLog初期化
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


        // メソッド実行許可初期化
        String executionMethodsStr = (String)super.getPropertiesValue(ImdstDefine.Prop_ExecutionMethods);
        if (executionMethodsStr != null && !executionMethodsStr.trim().equals("")) {
            String[] methodNoList = executionMethodsStr.split(",");
            StatusUtil.initExecuteMethodList(methodNoList);
        }


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
        balanceModes = new Boolean[balancePattern];
        balanceModes[0] = new Boolean(false);
        balanceModes[1] = new Boolean(false);
        balanceModes[2] = new Boolean(false);
        balanceModes[3] = new Boolean(false);
        balanceModes[4] = new Boolean(false);
        balanceModes[5] = new Boolean(false);
        balanceModes[6] = new Boolean(false);
        balanceModes[7] = new Boolean(true);
        balanceModes[8] = new Boolean(true);
        balanceModes[9] = new Boolean(true);

        int ratio = 10;
        String ratioStr = (String)super.getPropertiesValue(ImdstDefine.Prop_BalanceRatio);

        if (ratioStr != null && !ratioStr.trim().equals("")) {

            String[] ratioStrs = ratioStr.split(":");
            try {
                if ((Integer.parseInt(ratioStrs[0]) + Integer.parseInt(ratioStrs[1])) == 10) {
                    for (int i = 0; i < 10; i++) {
                        if (i >= Integer.parseInt(ratioStrs[0])) {
                            balanceModes[i] = new Boolean(true);
                        }
                    }
                }
            } catch (Exception e) {
            }
        }


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
        int paramSize = 9;

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

            // データ個別保持のモード設定
            String userIsolationMode = (String)super.getPropertiesValue(ImdstDefine.Prop_IsolationMode);

            if (userIsolationMode != null && userIsolationMode.equals("true")) {
                // IsolationMode
                String isolationPrefix = super.getPropertiesValue(ImdstDefine.Prop_IsolationPrefix);
                if (isolationPrefix != null && isolationPrefix.trim().length() == 5) {

                    StatusUtil.initIsolationMode(true, isolationPrefix);
                } else {

                    return ERROR;
                }
            }


            // サーバソケットの生成
            InetSocketAddress bindAddress = null;
            if (this.bindIpAddress == null) {
                bindAddress = new InetSocketAddress(this.portNo); 
            } else {
                bindAddress = new InetSocketAddress(this.bindIpAddress, this.portNo);
            }

            this.serverSocket = new ServerSocket();
            this.serverSocket.bind(bindAddress, this.backLog);

            // 共有領域にServerソケットのポインタを格納
            super.setJobShareParam(super.getJobName() + "_ServeSocket", this.serverSocket);


            // オリジナルのキュー領域を作成
            for (int i = 0; i < this.maxConnectParallelQueue; i++) {
                super.createUniqueHelperParamQueue("MasterManagerConnectHelper" + i, 10000);
                this.maxConnectParallelQueueNames[i] = "MasterManagerConnectHelper" + i;
            }

            for (int i = 0; i < this.maxAcceptParallelQueue; i++) {
                super.createUniqueHelperParamQueue("MasterManagerAcceptHelper" + i, 10000);
                this.maxAcceptParallelQueueNames[i] = "MasterManagerAcceptHelper" + i;
            }

            for (int i = 0; i < this.maxWorkerParallelQueue; i++) {
                super.createUniqueHelperParamQueue("MasterManagerHelper" + i, 10000);
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
                super.executeHelper("MasterManagerConnectHelper", helperParams, true);
            }

            for (int i = 0; i < this.maxAcceptParallelExecution; i++) {
                queueIndex = (i+1) % this.maxAcceptParallelQueue;
                helperParams = new Object[2];
                helperParams[0] = "MasterManagerAcceptHelper" + queueIndex;
                helperParams[1] = this.maxWorkerParallelQueueNames;
                super.executeHelper("MasterManagerAcceptHelper", helperParams, true);
            }



            // MasterManagerHelper用設定
            Object[] helperShareParams = new Object[(this.maxWorkerParallelQueue * 2)];

            queueIndex = 0;
            for (int i = 0; i < (this.maxWorkerParallelQueue * 2); i=i+2) {
                helperShareParams[i] = "Bind-MasterManagerHelper" + queueIndex;
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


            queueIndex = 0;
            for (int i = 0; i < this.maxWorkerParallelExecution; i++) {
                queueIndex = (i+1) % this.maxWorkerParallelQueue;

                helperParams = new Object[paramSize];
                helperParams[0] = null;
                helperParams[1] = null;
                helperParams[2] = this.mode;
                helperParams[3] = null;
                helperParams[4] = StatusUtil.isTransactionMode();
                helperParams[5] = StatusUtil.getTransactionNode();
                helperParams[6] = "MasterManagerHelper" + queueIndex;
                helperParams[7] = this.maxAcceptParallelQueueNames;
                helperParams[8] = "Bind-MasterManagerHelper" + queueIndex;

                if (i == 0) {
                    super.executeHelper("MasterManagerHelper", helperParams, true, helperShareParams);
                } else {
                    super.executeHelper("MasterManagerHelper", helperParams, true);
                }
            }


            // 処理開始
            logger.info("okuyama MasterNode Initialization End ...");
            System.out.println("okuyama MasterNode Initialization End ...");

            logger.info("okuyama MasterNode start ...");
            logger.info("listening on " + bindAddress);
            System.out.println("okuyama MasterNode start");
            System.out.println("listening on " + bindAddress);


            // メイン処理開始
            while (true) {
                if (StatusUtil.getStatus() == 1 || StatusUtil.getStatus() == 2) break;
                try {

                    Object[] queueParam = new Object[2];
                    if (this.loadBalance) {
                        queueParam[1] = this.balanceModes[new Long(accessCount % this.balancePattern).intValue()];
                    } else {
                        queueParam[1] = this.noBalance;
                    }

                    // クライアントからの接続待ち
                    socket = serverSocket.accept();

                    // ソケット格納
                    queueParam[0] = socket;

                    // アクセス済みのソケットをキューに貯める
                    super.addSpecificationParameterQueue("MasterManagerConnectHelper" + (accessCount%this.maxConnectParallelQueue), queueParam);
                    accessCount++;

                } catch (IOException ie) {

                    // IOエラーの場合は再度SocketをOpenする
                    try {
                        serverSocket.close();
                    } catch (IOException ie2) {
                    } finally {
                        try {

                            this.serverSocket = new ServerSocket();
                            this.serverSocket.bind(bindAddress, this.backLog);
                            // 共有領域にServerソケットのポインタを格納
                            super.setJobShareParam(super.getJobName() + "_ServeSocket", this.serverSocket);
                        } catch (IOException ie3) {
                            break;
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
        } finally {
            try {
                serverSocket.close();
            } catch (Exception e2) {
            }
            logger.info("okuyama MasterNode shutdown ...");
            System.out.println("okuyama MasterNode shutdown");
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