package org.okuyama.base;

import java.util.ArrayList;
import java.util.Hashtable;
import java.io.File;

import org.okuyama.base.lang.BatchException;
import org.okuyama.base.lang.BatchDefine;
import org.okuyama.base.job.AbstractJob;
import org.okuyama.base.job.IJob;
import org.okuyama.base.parameter.config.ConfigFolder;
import org.okuyama.base.parameter.config.BatchConfig;
import org.okuyama.base.parameter.config.JobConfig;
import org.okuyama.base.parameter.config.JobConfigMap;
import org.okuyama.base.parameter.config.HelperConfigMap;
import org.okuyama.base.process.IErrorProcess;
import org.okuyama.base.process.IProcess;

import org.okuyama.base.util.ClassUtility;
import org.okuyama.base.util.ILogger;
import org.okuyama.base.util.HelperPool;
import org.okuyama.base.util.LoggerFactory;
import org.okuyama.base.util.JobEndWaitWorker;

/**
 * Job実行コントローラクラス.<br>
 * 標準版.<br>
 * JobConfigの情報に従って処理を実行.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class DefaultJobController implements IJobController {

    // Logger
    private ILogger logger = LoggerFactory.createLogger(DefaultJobController.class);


    // 監視回数
    private int maxLoop = 0;
    // 監視間隔時間(ミリ秒)
    private int loopTimewait = 0;

    // 開始確認ファイル
    private String startChkFile = null;
    // 開始確認NGの場合の一時停止時間(ミリ秒)
    private int startChkTimewait = 250;
    // 標準終了ファイル
    private String normalEndFile = null;
    // 強制終了ファイル
    private String endFile = null;

    // 最大枠ループ指定
    private boolean reloop = false;

    // preprocessクラス名
    private String preProcessClass = null;
    // preprocessオプション文字列
    private String preProcessOption = null;

    // postprocess名
    private String postProcessClass = null;
    // postprocessオプション文字列
    private String postProcessOption = null;

    // errprocess名
    private String errProcessClass = null;
    // errprocessオプション文字列
    private String errProcessOption = null;

    // 全Jobインスタンス格納テーブル
    private Hashtable jobTable = null;

    // 全Jobのステータス格納用テーブル
    private Hashtable allJobStatusTable = null;

    // 全Job名リスト
    private String[] jobNameList = null;

    // Job終了待ち受けクラス(標準終了ファイルが配置された場合に使用)
    private JobEndWaitWorker jobEndWaitWorker = null;


    /**
     * コントローラ実行メソッド.<br>
     * 
     * @throw BatchException
     */
    public void execute() throws BatchException {
        logger.info("execute - start");
        BatchConfig batchConfig = null;
        JobConfig jobConfig = null;

        try {
            batchConfig = ConfigFolder.getBatchConfig();
            jobConfig = ConfigFolder.getJobConfig();

            // 設定情報を自身に蓄える
            this.initConfigSet(batchConfig);

            // 処理の実行を委譲
            this.executeJob(jobConfig);
        } catch (BatchException be) {
            logger.error("execute - error", be);
            throw be;
        }
        logger.info("execute - end");
    }


    /**
     * 設定情報初期化メソッド.<br>
     * 自身に設定情報を蓄える.<br>
     *
     * @param batchConfig 設定情報
     * @throws BatchException
     */
    private void initConfigSet(BatchConfig batchConfig) throws BatchException {
        logger.debug("initConfigSet - start");
        try {
            // 必須設定情報を設定
            this.maxLoop = Integer.parseInt(batchConfig.getBatchParam(BatchDefine.BATCH_CONFIG_MAXLOOP));
            this.loopTimewait = Integer.parseInt(batchConfig.getBatchParam(BatchDefine.BATCH_CONFIG_LOOPTIMEWAIT));
            this.normalEndFile = batchConfig.getBatchParam(BatchDefine.BATCH_CONFIG_NORMALENDFILE);
            this.endFile = batchConfig.getBatchParam(BatchDefine.BATCH_CONFIG_ENDFILE);
            // 設定ファイル確認
            if (this.endFile == null || this.normalEndFile == null ) 
                throw new BatchException("バッチ設定(終了ファイル指定)エラー");


            // 必須ではない設定情報を設定
            // 開始チェックファイル
            this.startChkFile = batchConfig.getBatchParam(BatchDefine.BATCH_CONFIG_STARTCHKFILE);

            // reloopはチェック
            if (batchConfig.getBatchParam(BatchDefine.BATCH_CONFIG_RELOOP) != null) {
                String reloopCheck = batchConfig.getBatchParam(BatchDefine.BATCH_CONFIG_RELOOP);

                if (reloopCheck.equals("true") || reloopCheck.equals("false")) {
                    this.reloop = new Boolean(reloopCheck).booleanValue();
                } else {
                    throw new BatchException("バッチ設定(reloop指定)エラー [true] or [false]のみ許可");
                }
            }

            // Process設定格納
            this.preProcessClass = batchConfig.getBatchParam(BatchDefine.BATCH_CONFIG_PREPROCESS);
            this.preProcessOption = batchConfig.getBatchParam(BatchDefine.BATCH_CONFIG_PREPROCESSOPTION);
            this.postProcessClass = batchConfig.getBatchParam(BatchDefine.BATCH_CONFIG_POSTPROCESS);
            this.postProcessOption = batchConfig.getBatchParam(BatchDefine.BATCH_CONFIG_POSTPROCESSOPTION);
            this.errProcessClass = batchConfig.getBatchParam(BatchDefine.BATCH_CONFIG_ERRORPROCESS);
            this.errProcessOption = batchConfig.getBatchParam(BatchDefine.BATCH_CONFIG_ERRORPROCESSOPTION);

        } catch(BatchException be) {
            logger.error("initConfigSet - error", be);
            throw be;
        }
        logger.debug("initConfigSet - end");
    }


    /**
     * メイン処理.<br>
     * Job実行管理メソッド.<br>
     *
     * @param jobConfig Job設定情報
     * @throws BatchException
     */
    private void executeJob(JobConfig jobConfig) throws BatchException {
        logger.info("executeJob - start");

        // Job
        AbstractJob job = null; 
        // Job設定
        JobConfigMap jobConfigMap = null;
        // Helper設定
        HelperConfigMap helperConfigMap = null;


        // 現在の実行監視数
        int loopCnt = 0;
        // 全てのJobが終了しているかを格納
        boolean allEnd = true;

        // Jobがエラーで終了しているかを格納
        boolean jobErrorEnd = false;

        boolean reloopFlg = true;

        // PreProcessの戻り値
        String preProcessRet = null;
        // PostProcessの戻り値
        String postProcessRet = null;

        String[] helperNameList = null;

        try {

            // reloopがtrueの場合このループで周り続ける
            while (reloopFlg) {
                if (!this.checkBatchStart()) {
                    // reloop設定を変更
                    reloopFlg = this.reloop;
                    continue;
                }

                // 終了ファイル確認
                if (this.checkBatchEnd()) break;

                // Jobの名前一覧を取り出し
                this.jobNameList = jobConfig.getJobNameList();
                // Jobインスタンス格納用リスト初期化
                this.jobTable = new Hashtable(this.jobNameList.length);
                // 状態テーブル初期化
                this.allJobStatusTable = new Hashtable(this.jobNameList.length);

                // preprocessを実行
                preProcessRet = this.executeProcess(1);

                // 実行Jobをアイドル状態にする
                // インスタンス化
                for (int i = 0; i < this.jobNameList.length; i++) {

                    // JobConfig情報取り出し
                    jobConfigMap = jobConfig.getJobConfig(this.jobNameList[i]);

                    // Jobインスタンス化
                    job = ClassUtility.createJobInstance(jobConfigMap.getJobClassName());

                    // Jobに設定情報を格納
                    job.setConfig(jobConfigMap);

                    // PreProcess,PostProcessの戻り値を設定
                    job.setPreProcess(preProcessRet);
                    job.setPostProcess(postProcessRet);

                    // Jobステータスを管理テーブルに格納
                    this.allJobStatusTable.put(jobConfigMap.getJobName(), job.getStatus());

                    // アイドルTableに格納
                    this.jobTable.put(this.jobNameList[i], job);
                }

                // Helperをアイドル状態にする
                helperNameList = jobConfig.getHelperNameList();

                // HelperPoolを完全初期化
                HelperPool.initializeHelperPool();
                for (int helperNameListIndex = 0; helperNameListIndex < helperNameList.length; helperNameListIndex++) {
                    helperConfigMap = jobConfig.getHelperConfig(helperNameList[helperNameListIndex]);
                    HelperPool.managedHelperConfig(helperConfigMap);
                }
                
                HelperPool helperPool = new HelperPool();
                helperPool.start();
                // 標準終了ファイル確認(全てのJobアイドルになってから確認)
                if (this.checkNormalBatchEnd()) break;

                // 実行監視回数初期化
                loopCnt = 0;

                // 実行監視開始
                while (true) {
                    // 強制終了ファイル確認
                    if (this.checkBatchEnd()) break;
                    // 標準終了ファイル確認
                    if (this.checkNormalBatchEnd()) break;

                    logger.info("Job実行監視 - start");

                    // 全てのJob完了フラグを初期化
                    allEnd = true;
                    jobErrorEnd = false;

                    // Jobを1づつ実行OKか確認して実行
                    for (int jobIndex = 0; jobIndex < this.jobNameList.length; jobIndex++) {

                        // インスタンス化したJob
                        job = (AbstractJob)this.jobTable.get(this.jobNameList[jobIndex]);

                        // JobConfig情報取り出し
                        jobConfigMap = jobConfig.getJobConfig(this.jobNameList[jobIndex]);

                        logger.info("Job実行状態確認 Job名[" + jobConfigMap.getJobName() + "]");

                        // 実行状態に合わせて処理を変更
                        if(job.getStatus().equals(BatchDefine.JOB_STATUS_WAIT)) {
                            // All End ?
                            allEnd = false;

                            // 実行待ち状態
                            // Jobの依存状態確認
                            if (!jobConfigMap.isDepend()) {

                                // 依存関係無し
                                // 実行
                                job.start();
                                // Jobステータスを管理テーブルに格納
                                this.allJobStatusTable.put(jobConfigMap.getJobName(), BatchDefine.JOB_STATUS_RUN);
                            } else {

                                // 依存関係有り
                                // 依存関係のあるJobの状態確認
                                if (this.checkDepend(jobConfigMap)) {
                                    // 依存関係のあるJobが全て終了しているのでJob実行
                                    job.start();
                                    // Jobステータスを管理テーブルに格納
                                    this.allJobStatusTable.put(jobConfigMap.getJobName(), BatchDefine.JOB_STATUS_RUN);
                                }
                            }
                        } else if (job.getStatus().equals(BatchDefine.JOB_STATUS_ERR)) {

                            // エラー発生
                            // Jobステータスがエラーの場合はここで状態管理テーブルに格納
                            this.allJobStatusTable.put(jobConfigMap.getJobName(), job.getStatus());

                            // reloopの設定がtrueの場合1件でもJobがエラーの場合も終了せずに実行中のJobの終了を待つ
                            // reloopがfalseの場合は直ちに終了
                            // 両方とも後続ジョブを行わない
                            if (this.reloop) {
                                logger.error("Jobがエラーステータスで終了 Job名[" + jobConfigMap.getJobName() + "]");
                                this.runJobEndWait();
                                // ErrorProcess実行
                                this.executeErrorProcess();
                                jobErrorEnd = true;
                                break;
                            } else {
                                throw new BatchException("Jobがエラーステータスで終了 Job名[" + jobConfigMap.getJobName() + "]");
                            }

                        } else if (job.getStatus().equals(BatchDefine.JOB_STATUS_RUN)) {

                            // Jobステータスを管理テーブルに格納
                            this.allJobStatusTable.put(jobConfigMap.getJobName(), BatchDefine.JOB_STATUS_RUN);

                            // 現在実行中
                            allEnd = false;
                        } else if (job.getStatus().equals(BatchDefine.JOB_STATUS_END)) {

                            // Jobステータスを管理テーブルに格納
                            this.allJobStatusTable.put(jobConfigMap.getJobName(), BatchDefine.JOB_STATUS_END);
                        }

                        // JobTableに戻す
                        this.jobTable.put(this.jobNameList[jobIndex], job);
                    }

                    // 現在のJobステータスを出力
                    logger.info("現在の全Jobステータス:" + this.allJobStatusTable);
                    logger.info("Job実行監視 - end");
                    // Jobがエラーで終了しているか確認
                    
                    if (jobErrorEnd) {
                        logger.info("Job実行監視 - Jobがエラーで終了");
                        
                        break;
                    }

                    // 全てのJobが終了しているか確認
                    if (allEnd){
                        logger.info("Job実行監視 - 全Job終了");
                        break;
                    }


                    // 実行監視サイクル確認
                    if (maxLoop != -1) {

                        loopCnt++;
                        // 監視回数到達
                        if(maxLoop == loopCnt) {

                            logger.error("Job実行監視上限回数到達!!");
                            // reloopの設定がtrueの場合は上限でもバッチは終了せずに現在実行中のJobの終了を待つ、reloopがfalseの場合は直ちに終了
                            if (this.reloop) {
                                this.runJobEndWait();
                                // ErrorProcess実行
                                this.executeErrorProcess();
                                break;
                            } else {
                                throw new BatchException("Exception - Job実行監視上限回数到達!!");
                            }
                        }
                    }

                    // 一時停止
                    logger.info("Job実行監視 - sleep");
                    Thread.sleep(loopTimewait);
                }

                // Helperの監視を終了
                helperPool.poolEnd();
                helperPool.join(60000);
                helperPool = null;

                logger.info("Job実行監視 - finish");

                // PostProcess実行
                postProcessRet = this.executeProcess(2);

                // reloop設定を変更
                reloopFlg = this.reloop;
            } 
        } catch (BatchException be) {
            logger.error("executeJob - error", be);
            // ErrorProcess実行
            this.executeErrorProcess();
            throw be;
        } catch (Exception e) {
            logger.error("executeJob - error", e);
            throw new BatchException(e);
        }

        logger.info("executeJob - end");
    }


    /**
     * Pre、PostProcessクラスを実行.<br>
     *
     * @param type 1:preprocess 2:postprocess
     * @return String それぞれのprocessの戻り値を返す
     * @throws BatchException
     */
    public String executeProcess(int type) throws BatchException {
        logger.debug("executeProcess - start");
        IProcess processClass = null;
        String processOption = null;
        String ret = null;

        try {
            // 呼び出しパターンによってクラスを切り替え
            if (type == 1) {

                // PreProcess
                if(this.preProcessClass != null && !this.preProcessClass.equals("")) {
                    processClass = (IProcess)ClassUtility.createInstance(this.preProcessClass);
                    processOption = this.preProcessOption;
                }
            } else if(type == 2) {

                // PostProcess
                if(this.postProcessClass != null && !this.postProcessClass.equals("")) {
                    processClass = (IProcess)ClassUtility.createInstance(this.postProcessClass);
                    processOption = this.postProcessOption;
                }
            } else {

                // エラー
                ;
            }

            // 設定が存在する場合のみ実行
            if (processClass != null) 
                ret = processClass.process(processOption);
        } catch (BatchException be) {
            logger.error("executeProcess - error", be);
            throw be;
        } catch (Exception e) {
            logger.error("executeProcess - error", e);
            throw new BatchException(e);
        }
        logger.debug("executeProcess - end");
        return ret;
    }


    /**
     * ErrorProcessクラスを実行.<br>
     * 本メソットは例外をスローしない.<br>
     * 例外内容のみログに吐き出す.<br>
     */
    public void executeErrorProcess() {
        logger.debug("executeErrorProcess - start");
        IErrorProcess processClass = null;

        try {

            // ErrorProcessの設定を確認
            if(this.errProcessClass != null && !this.errProcessClass.equals("")) {
                // 設定が存在する場合のみ実行
                processClass = (IErrorProcess)ClassUtility.createInstance(this.errProcessClass);
                processClass.errorProcess(this.jobTable, this.allJobStatusTable, this.errProcessOption);
            }

        } catch (BatchException be) {
            logger.error("executeErrorProcess - error ", be);
            // スロー無し
        } catch (Exception e) {
            logger.error("executeErrorProcess - error ", e);
            // スロー無し
        }
        logger.debug("executeErrorProcess - end");
    }


    /**
     * バッチの開始確認ファイル有無を確認.<br>
     * 本メソッドを呼び出し、開始NGファイルがある場合CPUの負荷を分散するために<br>
     * startChkTimewaitに設定された時間停止する.<br>
     * 
     *
     * @return boolean true:開始OK false:開始NG
     * @throws BatchException
     */
    private boolean checkBatchStart() throws BatchException {
        logger.debug("checkBatchStart - start");
        boolean ret = true;
        try {
            // ファイルの設定がない場合は無条件で開始OK
            if (this.startChkFile == null) return ret;

            File file = new File(new File(this.startChkFile).getAbsolutePath());
            if (file.exists()) {
                ret = false;
                Thread.sleep(startChkTimewait);
                logger.info("checkBatchEnd - 開始NGファイルが存在します");
            }
        } catch (Exception e) {
            logger.error("checkBatchStart - error", e);
            throw new BatchException(e);
        }
        logger.debug("checkBatchStart - end");
        return ret;
    }


    /**
     * バッチの強制終了ファイル有無を確認.<br>
     *
     * @return boolean true:終了 false:続行
     * @throws BatchException
     */
    private boolean checkBatchEnd() throws BatchException {
        logger.debug("checkBatchEnd - start");
        boolean ret = false;
        try {
            File file = new File(new File(this.endFile).getAbsolutePath());
            if (file.exists()) {
                ret = true;
                logger.info("checkBatchEnd - 終了ファイルが存在します");
            }
        } catch (Exception e) {
            logger.error("checkBatchEnd - error", e);
            throw new BatchException(e);
        }
        logger.debug("checkBatchEnd - end");
        return ret;
    }

    /**
     * 現在RUNのステータスになっているJobを終了するのを待つ<br>
     * スレッドを起動する.<br>
     *
     * @throws Exception
     */
    public void runJobEndWait() throws Exception {
        logger.debug("runJobEndWait - start");
        AbstractJob job = null;
        ArrayList jobs = null;
        int runJobCount = 0;

        try {

            // 終了待ちスレッド起動準備

            // RUNになっているJobを数える
            if (this.jobNameList != null && this.jobNameList.length > 0) {

                jobs = new ArrayList();
                for (int i = 0; i < this.jobNameList.length; i++) {
                    job = (AbstractJob)this.jobTable.get(this.jobNameList[i]);
                    
                    if (job.getStatus().equals(BatchDefine.JOB_STATUS_RUN)) {
                        jobs.add(job);
                    }
                }

                // 終了待ちスレッド起動
                this.jobEndWaitWorker = new JobEndWaitWorker();
                this.jobEndWaitWorker.setTargetJobs(jobs);
                this.jobEndWaitWorker.start();

                // 終了待ちスレッドが終了するのを待つ
                while(true) {

                    // 終了待機スレッド確認
                    // エラーになっていないか?
                    if (this.jobEndWaitWorker.getStatus().equals(BatchDefine.JOB_END_WAIT_WORKER_ERR)) 
                        throw new BatchException("JobEndWaitWorkerでエラー発生");
                    // 終了していないか?
                    if (this.jobEndWaitWorker.getStatus().equals(BatchDefine.JOB_END_WAIT_WORKER_END)) break;

                    // 強制終了ファイル確認
                    if (this.checkBatchEnd()) break;

                    logger.info("runJobEndWait - 稼働中Job終了待ち");
                    Thread.sleep(BatchDefine.JOB_END_WAIT_WORKER_TIMER);
                }
            }
        } catch (Exception e) {
            logger.error("runJobEndWait - error", e);
            throw e;
        }
        logger.debug("runJobEndWait - end");
    }

    /**
     * 標準終了ファイルの存在を確認し存在する場合は<br>
     * 現在RUNのステータスになっているJobを終了するのを待つ<br>
     * スレッドを起動する.<br>
     * スレッド起動後再度このメソッドが呼ばれた場合はそのスレッドの<br>
     * 終了を確認するそのスレッドが終了したタイミングで<br>
     * 終了したことをリターンで通知する.<br>
     *
     * @return boolean true:終了ファイルあり false:終了ファイルなし
     * @throws Exception
     */
    public boolean checkNormalBatchEnd() throws Exception {
        logger.debug("checkNormalBatchEnd - start");
        boolean ret = false;
        AbstractJob job = null;
        ArrayList jobs = null;
        int runJobCount = 0;

        try {
            File file = new File(new File(this.normalEndFile).getAbsolutePath());
            if (file.exists()) {
                logger.info("checkNormalBatchEnd - 標準終了ファイルが存在します");

                ret = true;

                // 終了待ちスレッド起動準備

                // RUNになっているJobを数える
                if (this.jobNameList != null && this.jobNameList.length > 0) {

                    jobs = new ArrayList();
                    for (int i = 0; i < this.jobNameList.length; i++) {
                        job = (AbstractJob)this.jobTable.get(this.jobNameList[i]);
                        
                        if (job.getStatus().equals(BatchDefine.JOB_STATUS_RUN)) {
                            jobs.add(job);
                        }
                    }

                    // 終了待ちスレッド起動
                    this.jobEndWaitWorker = new JobEndWaitWorker();
                    this.jobEndWaitWorker.setTargetJobs(jobs);
                    this.jobEndWaitWorker.start();

                    // 終了待ちスレッドが終了するのを待つ
                    while(true) {

                        // 終了待機スレッド確認
                        // エラーになっていないか?
                        if (this.jobEndWaitWorker.getStatus().equals(BatchDefine.JOB_END_WAIT_WORKER_ERR)) 
                            throw new BatchException("JobEndWaitWorkerでエラー発生");
                        // 終了していないか?
                        if (this.jobEndWaitWorker.getStatus().equals(BatchDefine.JOB_END_WAIT_WORKER_END)) break;

                        // 強制終了ファイル確認
                        if (this.checkBatchEnd()) break;

                        logger.info("checkNormalBatchEnd - 稼働中Job終了待ち");
                        Thread.sleep(BatchDefine.JOB_END_WAIT_WORKER_TIMER);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("checkNormalBatchEnd - error", e);
            throw e;
        }
        logger.debug("checkNormalBatchEnd - end");
        return ret;
    }


    /**
     * 依存関係チェック.<br>
     *
     * @param jobConfigMap Job情報
     * @return boolean true:依存関係Job全終了 false:依存関係Jobが終了していない
     * @throws BatchException
     */
    private boolean checkDepend(JobConfigMap jobConfigMap) throws BatchException {
        logger.debug("checkDepend - start");
        boolean checkFlg = true;
        String[] dependJobList = null;
        String dependJobStatus = null;

        try {
            dependJobList = jobConfigMap.getJobDependList();
            // 依存関係のあるJobの状態を確認
            for (int dependIndex = 0; dependIndex < dependJobList.length; dependIndex++) {

                // 依存関係の設定確認
                if(this.allJobStatusTable.containsKey(dependJobList[dependIndex])) {

                    // 依存関係確認
                    dependJobStatus = (String)this.allJobStatusTable.get(dependJobList[dependIndex]);
                    // 1件でも終了していない場合は実行しない
                    if (!dependJobStatus.equals(BatchDefine.JOB_STATUS_END)) checkFlg = false;
                } else {
                    throw new BatchException("依存関係設定エラー:" + jobConfigMap.getJobName());
                }
            }
        } catch(BatchException be) {
            logger.error("checkDepend -error", be);
            throw be;
        }
        logger.debug("checkDepend - end");
        return checkFlg;
    }
}
