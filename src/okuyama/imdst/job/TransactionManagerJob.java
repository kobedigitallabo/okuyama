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
import okuyama.imdst.util.StatusUtil;

/**
 * OkuyamaでTransactionの概念を実現する.<br>
 * 分散トランザクションマネージャ.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class TransactionManagerJob extends AbstractJob implements IJob {

    // ポート番号
    private int portNo = 10060;

    // サーバーソケット
    ServerSocket serverSocket = null;

    // KeyMapManagerインスタンス(トランザクション管理用で使用する)
    private KeyMapManager keyMapManager = null;

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(TransactionManagerJob.class);

    /**
     * 初期化メソッド定義
     */
    public void initJob(String initValue) {
        logger.debug("TransactionManagerJob - initJob - start");

        this.portNo = Integer.parseInt(initValue);

        logger.debug("TransactionManagerJob - initJob - end");
    }

    /** 
     * Jobメイン処理定義
     */
    public String executeJob(String optionParam) throws BatchException {
        logger.debug("TransactionManagerJob - executeJob - start");

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

            this.keyMapManager = new KeyMapManager(keyMapFiles[0], keyMapFiles[1], workFileMemoryMode, keySize, dataMemoryMode, false, null);
            this.keyMapManager.start();

            // サーバソケットの生成
            this.serverSocket = new ServerSocket(this.portNo);
            // 共有領域にServerソケットのポインタを格納
            super.setJobShareParam(super.getJobName() + "_ServeSocket", this.serverSocket);

            while (true) {
                if (StatusUtil.getStatus() == 1 || StatusUtil.getStatus() == 2) break;
                try {

                    // クライアントからの接続待ち
                    socket = serverSocket.accept();
                    //logger.debug(socket.getInetAddress() + " ACCESS");
                    helperParams = new Object[2];
                    helperParams[0] = this.keyMapManager;
                    helperParams[1] = socket;
                    super.executeHelper("TransactionManagerHelper", helperParams);
                } catch (Exception e) {
                    if (StatusUtil.getStatus() == 2) {
                        logger.info("TransactionManagerJob - executeJob - ServerEnd");
                        break;
                    }
                    logger.error(e);
                }
            }
        } catch(Exception e) {
            logger.error("TransactionManagerJob - executeJob - Error", e);
            throw new BatchException(e);
        }

        //logger.debug("TransactionManagerJob - executeJob - end");
        return ret;
    }


}