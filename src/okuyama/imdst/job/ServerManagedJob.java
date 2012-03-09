package okuyama.imdst.job;

import java.util.*;
import java.io.*;
import java.net.*;

import okuyama.base.JavaMain;

import okuyama.base.lang.BatchException;
import okuyama.base.job.AbstractJob;
import okuyama.base.job.AbstractHelper;
import okuyama.base.job.IJob;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.imdst.util.KeyMapManager;
import okuyama.imdst.util.StatusUtil;
import okuyama.imdst.util.JavaSystemApi;
import okuyama.imdst.util.ImdstDefine;

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
    private int checkCycle = 4000;

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

        File serverStopMarkerFile = null;

        boolean serverRunning = true;

        Object[] shareKeys = null;
        ServerSocket serverSocket = null;
        boolean firstOver = true;
        long gcExecuteTime = System.currentTimeMillis();
        long executeGcInterval = 10000;
        long memoryLimitOverCount = 0;
        long maxMemoryLimitOverCount = 5;
        boolean gcOff = false;

        try{

            super.executeHelper("ServerControllerHelper", null, true);
            super.executeHelper("ServerTimerHelper", null, true);

            while (serverRunning) {

                if (StatusUtil.getStatus() != 0) break;

                StringBuilder memBuf = new StringBuilder();
                memBuf.append("JVM MaxMemory Size =[" + JavaSystemApi.getRuntimeMaxMem("M") + "];");
                memBuf.append("JVM TotalMemory Size =[" + JavaSystemApi.getRuntimeTotalMem("M") + "]; ");
                memBuf.append("JVM FreeMemory Size =[" + JavaSystemApi.getRuntimeFreeMem("M") + "]; ");
                memBuf.append("JVM Use Memory Percent=[" + JavaSystemApi.getUseMemoryPercent() + "]");
                StatusUtil.setNowMemoryStatus(memBuf.toString());

                if (optionParam != null &&  optionParam.equals("true")) {
                    // メモリ関係チェック
                    logger.debug("JVM MaxMemory Size =[" + JavaSystemApi.getRuntimeMaxMem("M") + "]");
                    logger.debug("JVM TotalMemory Size =[" + JavaSystemApi.getRuntimeTotalMem("M") + "]");
                    logger.debug("JVM FreeMemory Size =[" + JavaSystemApi.getRuntimeFreeMem("M") + "]");
                    logger.debug("JVM Use Memory Percent=[" + JavaSystemApi.getUseMemoryPercent() + "]");
                }
                if (gcOff == false  && JavaSystemApi.getUseMemoryPercentCache() > StatusUtil.getMemoryLimitMinSize()) {
                    memoryLimitOverCount++;

                    if (memoryLimitOverCount < maxMemoryLimitOverCount || ((System.currentTimeMillis() - gcExecuteTime) > executeGcInterval)) {

                        System.out.println(new Date().toString() + " FullGC - Execute - Start");
                        JavaSystemApi.manualGc();
                        System.out.println(new Date().toString() + " FullGC - Execute - End");
                        gcExecuteTime = System.currentTimeMillis();
                        //Thread.sleep(2000);
                    } else {
                        // 限界値を超えている
                        if (memoryLimitOverCount >= maxMemoryLimitOverCount) {
                            System.out.println(new Date().toString() + " JVM Limit MemorySize Over");
                            StatusUtil.useMemoryLimitOver();
                            gcOff = true;
                        }
                    }
                } else {
                    memoryLimitOverCount = 0;
                }
                // GC発行
                if (gcOff == false)
                    JavaSystemApi.autoGc();

                Thread.sleep(checkCycle);
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
        } finally {
            // 正常終了ではない
            if (StatusUtil.getStatus() == 1 || StatusUtil.getStatus() == 2) {
                logger.error("ServerManagedJob - executeJob - Error End Message=[" + StatusUtil.getStatusMessage() + "]");
                JavaMain.shutdownMainProcess();
            }
        }

        logger.debug("ServerManagedJob - executeJob - end");
        return ret;
    }

}