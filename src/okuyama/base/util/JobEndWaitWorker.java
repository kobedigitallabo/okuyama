package okuyama.base.util;

import java.util.ArrayList;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.base.lang.BatchDefine;
import okuyama.base.job.AbstractJob;

/**
 * Jobの終了を待機するスレッド.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class  JobEndWaitWorker extends Thread {

    // Logger
    private ILogger logger = LoggerFactory.createLogger(JobEndWaitWorker.class);

    private ArrayList jobs = null;

    // 自身のステータス
    private String status = BatchDefine.JOB_END_WAIT_WORKER_WAIT;

    /**
     * メイン実行部分
     *
     */
    public void run() {
        logger.debug("JobEndWaitWorker - run - start");
        try {
            this.status = BatchDefine.JOB_END_WAIT_WORKER_RUN;
            AbstractJob job = null;

            for (int i = 0; i < this.jobs.size(); i++) {
                job = (AbstractJob)this.jobs.get(i);
                logger.debug("[" + job.getJobName() + "] : End Wait Start");
                job.join();
                logger.debug("[" + job.getJobName() + "] : End Wait End");
            }

            this.status = BatchDefine.JOB_END_WAIT_WORKER_END;
        } catch (Exception e) {
            logger.error("JobEndWaitWorker - run - error", e);
            this.status = BatchDefine.JOB_END_WAIT_WORKER_ERR;
        }
        logger.debug("JobEndWaitWorker - run - end");
    }


    /**
     * 自身のステータスを返す.<br>
     *
     * @return String ステータス文字列
     */
    public String getStatus() {
        return this.status;
    }

    /**
     * 終了確認を行いたいJobを設定する.<br>
     *
     * @param jobs Jobリスト
     */
    public void setTargetJobs(ArrayList jobs) {
        this.jobs = jobs;
    }
}