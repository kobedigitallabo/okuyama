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
import org.imdst.util.StatusUtil;
import org.imdst.util.JavaSystemApi;

/**
 * Serverのリソース全般を管理する.<br>
 * メモリの使用状況の管理.<br>
 * Serverソケット関係の終了を監視.<br>
 * Parameterファイルに設定されているマーカーファイル郡を使用して管理を行う.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ServerManagedJob extends AbstractJob implements IJob {

    // 停止ファイルの監視サイクル時間(ミリ秒)
    private int checkCycle = 5000;

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(ServerManagedJob.class);

    // 初期化メソッド定義
    public void initJob(String initValue) {
        logger.debug("ServerManagedJob - initJob - start");
        logger.debug("ServerManagedJob - initJob - end");
    }

    // Jobメイン処理定義
    public String executeJob(String optionParam) throws BatchException {
        logger.debug("ServerManagedJob - executeJob - start");
        String ret = SUCCESS;
        String serverStopMarkerFileName = null;
        File serverStopMarkerFile = null;

        boolean serverRunning = true;

        Object[] shareKeys = null;
        ServerSocket serverSocket = null;

        try{
            while (serverRunning) {
                Thread.sleep(checkCycle);

                // 停止ファイル関係チェック
                if (StatusUtil.getStatus() == 1) {
                    serverRunning = false;
                    logger.info("ServerManagedJob - 状態異常です Msg = [" + StatusUtil.getStatusMessage() + "]");
                }

                if (StatusUtil.getStatus() == 2) {
                    serverRunning = false;
                    logger.info("ServerManagedJob - 終了状態です");
                }

                serverStopMarkerFileName = super.getPropertiesValue("ServerStopFile");

                serverStopMarkerFile = new File(new File(serverStopMarkerFileName).getAbsolutePath());
                if (serverStopMarkerFile.exists()) {
                    serverRunning = false;
                    logger.info("ServerManagedJob - Server停止ファイルが存在します");
                    StatusUtil.setStatus(2);
                }

                StringBuffer memBuf = new StringBuffer();
                memBuf.append("JVM MaxMemory Size =[" + JavaSystemApi.getRuntimeMaxMem("M") + "];");
                memBuf.append("JVM TotalMemory Size =[" + JavaSystemApi.getRuntimeTotalMem("M") + "]; ");
                memBuf.append("JVM FreeMemory Size =[" + JavaSystemApi.getRuntimeFreeMem("M") + "]; ");
                memBuf.append("JVM Use Memory Percent=[" + JavaSystemApi.getUseMemoryPercent() + "]");
                StatusUtil.setNowMemoryStatus(memBuf.toString());

                if (optionParam != null &&  optionParam.equals("true")) {
                    // メモリ関係チェック
                    logger.info("JVM MaxMemory Size =[" + JavaSystemApi.getRuntimeMaxMem("M") + "]");
                    logger.info("JVM TotalMemory Size =[" + JavaSystemApi.getRuntimeTotalMem("M") + "]");
                    logger.info("JVM FreeMemory Size =[" + JavaSystemApi.getRuntimeFreeMem("M") + "]");
                    logger.info("JVM Use Memory Percent=[" + JavaSystemApi.getUseMemoryPercent() + "]");
                }
                // GC発行
                JavaSystemApi.autoGc();
            }

            shareKeys = super.getJobShareParamKeys();

            if(shareKeys != null) {
                for (int i = 0; i < shareKeys.length; i++) {
                    if (shareKeys[i] instanceof String) {
                        if (((String)shareKeys[i]).indexOf("ServeSocket") != -1) {
                            try {
                                serverSocket = (ServerSocket)super.getJobShareParam((String)shareKeys[i]);
                                serverSocket.close();
                                serverSocket = null;
                            } catch (Exception e2) {
                                logger.error("ServerManagedJob - executeJob - ServerSocket Colse Error!! Error Socket = " + (String)shareKeys[i], e2);
                            }
                        }
                    }
                }
            }
        } catch(Exception e) {
            logger.error("ServerManagedJob - executeJob - Error", e);
            throw new BatchException(e);
        }

        logger.debug("ServerManagedJob - executeJob - end");
        return ret;
    }

}