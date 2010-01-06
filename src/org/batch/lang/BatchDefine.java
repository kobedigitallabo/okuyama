package org.batch.lang;

/**
 * フレームワークが使用する定数値定義用クラス.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class BatchDefine {

    // AbstractJobのステータス文字列
    public static final String JOB_STATUS_WAIT = "job_wait";
    public static final String JOB_STATUS_RUN = "job_run";
    public static final String JOB_STATUS_ERR = "job_err";
    public static final String JOB_STATUS_END = "job_end";

    // JobEndWaitWorkerのステータス文字列とチェック間隔時間(ms)
    public static final String JOB_END_WAIT_WORKER_WAIT = "wait_worker_wait";
    public static final String JOB_END_WAIT_WORKER_RUN = "wait_worker_run";
    public static final String JOB_END_WAIT_WORKER_ERR = "wait_worker_err";
    public static final String JOB_END_WAIT_WORKER_END = "wait_worker_end";
    public static final long JOB_END_WAIT_WORKER_TIMER = 5000;

    // HelperPoolWorkerのステータス文字列とHelperチェック間隔時間(ms)
    public static final String HELPER_POOL_WORKER_WAIT = "helper_pool_worker_wait";
    public static final String HELPER_POOL_WORKER_RUN = "helper_pool_worker_run";
    public static final String HELPER_POOL_WORKER_ERR = "helper_pool_worker_err";
    public static final String HELPER_POOL_WORKER_END = "helper_pool_worker_end";
    public static final long HELPER_POOL_WORKER_TIMER = 5000;


    // ジョブ設定ファイルのキー文字列
    public static final String JOB_CONFIG_JOBLIST_KEY = "joblist";
    public static final String JOB_CONFIG_HELPERLIST_KEY = "helperlist";
    public static final String JOB_CONFIG_JOBLIST_SEP = ",";
    public static final String JOB_CONFIG_JOIN_SEP = ".";
    public static final String JOB_CONFIG_CLASS_KEY = "JobClass";
    public static final String JOB_CONFIG_HELPER_CLASS_KEY = "HelperClass";
    public static final String JOB_CONFIG_INIT_KEY = "Init";
    public static final String JOB_CONFIG_OPTION_KEY = "Option";
    public static final String JOB_CONFIG_HELPER_LIMIT_KEY = "Limit";
    public static final String JOB_CONFIG_HELPER_MAX_USE_KEY = "MaxUse";
    public static final String JOB_CONFIG_DEPEND_KEY = "Depend";
    public static final String JOB_CONFIG_DEPEND_SEP = ",";
    public static final String JOB_CONFIG_DBGROUP_KEY = "Dbgroup";
    public static final String JOB_CONFIG_DBGROUP_SEP = ",";
    public static final String JOB_CONFIG_COMMIT_KEY = "Commit";
    public static final String JOB_CONFIG_COMMIT_TYPE1 = "system";
    public static final String JOB_CONFIG_COMMIT_TYPE2 = "auto";
    public static final String JOB_CONFIG_COMMIT_TYPE3 = "user";

    public static final String JOB_CONFIG_HELPER_DBGROUP_KEY = "Dbgroup";
    public static final String JOB_CONFIG_HELPER_DBGROUP_SEP = ",";
    public static final String JOB_CONFIG_HELPER_COMMIT_KEY = "Commit";
    public static final String JOB_CONFIG_HELPER_COMMIT_TYPE1 = "system";
    public static final String JOB_CONFIG_HELPER_COMMIT_TYPE2 = "auto";
    public static final String JOB_CONFIG_HELPER_COMMIT_TYPE3 = "user";




    // バッチ設定ファイルのキー文字列
    public static final String BATCH_CONFIG_CONTROLLER = "controller";
    public static final String BATCH_CONFIG_MAXLOOP = "maxloop";
    public static final String BATCH_CONFIG_LOOPTIMEWAIT = "looptimewait";
    public static final String BATCH_CONFIG_STARTCHKFILE = "startchkfile";
    public static final String BATCH_CONFIG_NORMALENDFILE = "normalendfile";
    public static final String BATCH_CONFIG_ENDFILE = "endfile";
    public static final String BATCH_CONFIG_RELOOP = "reloop";
    public static final String BATCH_CONFIG_PREPROCESS = "preprocess";
    public static final String BATCH_CONFIG_PREPROCESSOPTION = "preprocessoption";
    public static final String BATCH_CONFIG_POSTPROCESS = "postprocess";
    public static final String BATCH_CONFIG_POSTPROCESSOPTION = "postprocessoption";
    public static final String BATCH_CONFIG_ERRORPROCESS = "errorprocess";
    public static final String BATCH_CONFIG_ERRORPROCESSOPTION = "errorprocessoption";
    public static final String BATCH_CONFIG_SQL_DUMP_LOG = "sqldumplog";
    public static final String BATCH_CONFIG_DATABASEDRIVER = "databasedriver";
    public static final String BATCH_CONFIG_DATABASENAME = "databasename";
    public static final String BATCH_CONFIG_DATABASEUSER = "databaseuser";
    public static final String BATCH_CONFIG_DATABASEPASS = "databasepass";
    public static final String BATCH_CONFIG_COMMIT = "commit";

    public static final String COMMIT_TYPE_SYSTEM = "system";
    public static final String COMMIT_TYPE_AUTO = "auto";
    public static final String COMMIT_TYPE_USER = "user";

}