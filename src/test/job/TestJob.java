package test.job;

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
public class TestJob extends AbstractJob implements IJob {

    // 停止ファイルの監視サイクル時間(ミリ秒)
    private int checkCycle = 5000;

    /**
     * Logger.<br>
     */
    private static ILogger logger = LoggerFactory.createLogger(TestJob.class);

    // 初期化メソッド定義
    public void initJob(String initValue) {
        logger.debug("TestJob - initJob - start");
        logger.debug("TestJob - initJob - end");
    }

    // Jobメイン処理定義
    public String executeJob(String optionParam) throws BatchException {
        logger.debug("TestJob - executeJob - start");
        String ret = SUCCESS;

        try{
            int helperCode = super.executeHelper("TestHelper", null);

            Object[] helperRet = null;
            while(helperRet == null) {
                 helperRet = super.waitGetHelperReturnParam(helperCode, 10);
            }

        } catch(Exception e) {
            logger.error("TestJob - executeJob - Error", e);
            throw new BatchException(e);
        }

        logger.debug("TestJob - executeJob - end");
        return ret;
    }

}