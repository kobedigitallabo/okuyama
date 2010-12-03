package test.job;

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
import okuyama.imdst.util.JavaSystemApi;

import okuyama.imdst.util.FileBaseDataMap;


/**
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class TestJob extends AbstractJob implements IJob {

    // 停止ファイルの監視サイクル時間(ミリ秒)
    private int checkCycle = 5000;
    private static String[] dirs = {"./keymapfile/1.dir/","./keymapfile/2.dir/"};
    private static FileBaseDataMap fileBaseDataMap = new FileBaseDataMap(dirs, 25600, 0.2, 1024*1024);

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
            //String[] dirs = {"./keymapfile/1.dir/","./keymapfile/2.dir/"};
            //FileBaseDataMap fileBaseDataMap = new FileBaseDataMap(dirs, 25600, 0.2, 1024*1024);
long start = System.nanoTime();
            for (int i = 0; i < 50; i++) {
                fileBaseDataMap.put(optionParam + "key" + i, optionParam + "value" + i);
            }

long end = System.nanoTime();
System.out.println((end - start) / 1024 / 1024);

long start2 = System.nanoTime();
            for (int i = 0; i < 50; i++) {
                fileBaseDataMap.get(optionParam + "key" + i);
            }

long end2 = System.nanoTime();
System.out.println("2=" + ((end - start) / 1024 / 1024));

        } catch(Exception e) {
            logger.error("TestJob - executeJob - Error", e);
            throw new BatchException(e);
        }

        logger.debug("TestJob - executeJob - end");
        return ret;
    }

}