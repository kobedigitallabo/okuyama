package okuyama.base.job;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import okuyama.base.parameter.config.ConfigFolder;
import okuyama.base.parameter.config.JobConfigMap;
import okuyama.base.parameter.config.HelperConfigMap;
import okuyama.base.lang.BatchException;
import okuyama.base.lang.BatchDefine;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.base.util.HelperPool;
import okuyama.base.util.ClassUtility;

/**
 * Jobの基底クラス.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
abstract public class AbstractJob extends Thread {

    // Logger
    private ILogger logger = LoggerFactory.createLogger(AbstractJob.class);

    // PreProcessの返値
    private String preProcessRet = null;

    // PostProcessの返値
    private String postProcessRet = null;

    // wait:実行待ち
    public static String WAIT = BatchDefine.JOB_STATUS_WAIT;

    // run:実行中
    public static String RUN = BatchDefine.JOB_STATUS_RUN;

    // err:エラー
    public static String ERR = BatchDefine.JOB_STATUS_ERR;

    // end:終了
    public static String END = BatchDefine.JOB_STATUS_END;

    // 正しく実行終了
    protected static String SUCCESS = END;

    // エラー実行終了
    protected static String ERROR = ERR;

    // 自身のステータス
    // wait:実行待ち
    // run:実行中
    // err:エラー
    // end:終了
    protected String status = WAIT;

    // 自身の設定情報
    protected JobConfigMap jobConfigMap = null;

    /**
     * Job間に値やり取りを行う領域.<br>
     * 一度登録した値はバッチが終了するまで維持される.<br>
     */
    private static ConcurrentHashMap jobParamShareMap = new ConcurrentHashMap(10, 10, 10);

    /**
     * コンストラクタ
     */
    public AbstractJob() {
    }

    /**
     * 自身の設定情報格納.<br>
     *
     * @param jobConfigMap Job設定情報
     */
    public void setConfig(JobConfigMap jobConfigMap) {
        this.jobConfigMap = jobConfigMap;
    }

    /**
     * initJob.<br>
     * ユーザJob実装部分.<br>
     * @param initValue
     */
    public abstract void initJob(String initValue);

    /**
     * executeJob.<br>
     * ユーザJob実装部分.<br>
     *
     * @param optionParam
     * @return String 実行結果
     * @throws BatchException
     */
    public abstract String executeJob(String optionParam) throws BatchException;



    /**
     * メイン実行部分.<br>
     * 自身のサブクラスを実行.<br>
     * 実行順序はinitJob - executeJob.<br>
     * 
     */
    public void run() {
        logger.debug("Execution - Start");

        String retStatus = null;

        // ステータスを実行中に変更
        this.status = RUN;


        try {

            // 初期化メソッド呼び出し
            initJob(this.jobConfigMap.getJobInit());
            // 実行メインメソッド呼び出し
            retStatus = executeJob(this.jobConfigMap.getJobOption());

            // 実行結果確認
            if (retStatus != null) {

                if (retStatus.equals(SUCCESS)) {
                    // 正常終了
                    this.status = retStatus;
                } else if(retStatus.equals(ERROR)) {
                    // エラー終了
                    this.status = retStatus;
                } else {
                    // 不明
                    throw new BatchException("Job End Return Value Error");
                }
            } else {
                throw new BatchException("Job End Return Value Error");
            }
        } catch (BatchException be) {
            logger.error("AbstractJob - Error",be);
            // ステータスをエラーにする
            this.status = ERR;
        } catch (Exception e) {
            logger.error("AbstractJob - Error",e);
            // ステータスをエラーにする
            this.status = ERR;
        }
        logger.debug("Execution End");
    }


    /**
     * Helperクラスインスタンスを返す.<br>
     * Helperが存在しない場合はExceptionを返す.<br>
     * 
     * 
     * @param helperName helperName名
     * @param helpreParams helperパラメータ配列
	 * @param inputHelperShareParam ヘルパー共有領域に事前に登録したいパラメータ Key-Valueのセットで配列登録すること(inputHelperShareParam[0]=1番目の要素のKey,inputHelperShareParam[1]=1番目の要素のValue,inputHelperShareParam[2]=2番目の要素のKey,inputHelperShareParam[3]=2番目の要素のValue)
     * @return AbstractHelper Helperインスタンス
     * @throws BatchException
     **/
    protected int executeHelperQueue(String helperName, Object[] helpreParams, Object[] inputHelperShareParam) throws BatchException {
        logger.debug("executeHelperQueue - start");
        int ret = 0;
        AbstractHelper helper = null;
        try {
            while (true) {
                helper = HelperPool.getHelper(helperName);
                if (helper != null) break;
            }

			for (int i = 0; i < inputHelperShareParam.length; i=i+2) {
				helper.setHelperShareParam(inputHelperShareParam[i], inputHelperShareParam[i+1]);
			}

            helper.addParameterQueue(helpreParams);
            helper.setParameters(null);
            // ExecutorService を使用するために変更
            //helper.start();

            HelperPool.returnHelper(helperName,helper);
            ret = helper.hashCode();
        } catch (BatchException be) {
            logger.error("createHelper - BatchException");
            throw be;
        } catch (Exception e) {
            logger.error("createHelper - Exception");
            throw new BatchException(e);
        }
        logger.debug("executeHelper - end");
        return ret;
	}


    /**
     * Helperクラスインスタンスを返す.<br>
     * Helperが存在しない場合はExceptionを返す.<br>
     * 
     * 
     * @param helperName helperName名
     * @param helpreParams helperパラメータ配列
     * @return AbstractHelper Helperインスタンス
     * @throws BatchException
     **/
    protected int executeHelperQueue(String helperName, Object[] helpreParams) throws BatchException {
        logger.debug("executeHelperQueue - start");
        int ret = 0;
        AbstractHelper helper = null;
        try {
            while (true) {
                helper = HelperPool.getHelper(helperName);
                if (helper != null) break;
            }

            helper.addParameterQueue(helpreParams);
            helper.setParameters(null);
            // ExecutorService を使用するために変更
            //helper.start();

            HelperPool.returnHelper(helperName,helper);
            ret = helper.hashCode();
        } catch (BatchException be) {
            logger.error("createHelper - BatchException");
            throw be;
        } catch (Exception e) {
            logger.error("createHelper - Exception");
            throw new BatchException(e);
        }
        logger.debug("executeHelper - end");
        return ret;
    }


    /**
     * オリジナルのキュー領域を作成する.<br>
     *
     */
    protected void createUniqueHelperParamQueue(String helperName, int size) throws Exception {
        try {
            HelperPool.createUniqueHelperParamQueue(helperName, size);
        } catch (Exception e) {
            throw e;
        }
    }


    /**
     * キューに対する追加メソッド.<br>
     * キュー指定あり.<br>
     *
     */
    protected void addSpecificationParameterQueue(String helperName, Object[] params) throws Exception {
        try {
            HelperPool.addSpecificationParameterQueue(helperName, params);
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * キューに対する追加メソッド.<br>
     * 全共通キュー.<br>
     *
     */
    protected void addHelperQueueParam(Object[] params) throws Exception {
        try {
            HelperPool.addParameterQueue(params);
        } catch (Exception e) {
            throw e;
        }
    }


    /**
     * Helperクラスインスタンスを返す.<br>
     * Helperが存在しない場合はExceptionを返す.<br>
     * 
     * 
     * @param helperName helperName名
     * @param helpreParams helperパラメータ配列
     * @return AbstractHelper Helperインスタンス
     * @throws BatchException
     **/
    protected int executeHelper(String helperName, Object[] helpreParams) throws BatchException {
        return executeHelper(helperName, helpreParams, false);
    }


    /**
     * Helperクラスインスタンスを返す.<br>
     * Helperが存在しない場合はExceptionを返す.<br>
     * 
     * 
     * @param helperName helperName名
     * @param helpreParams helperパラメータ配列
     * @return AbstractHelper Helperインスタンス
     * @throws BatchException
     **/
    protected int executeHelper(String helperName, Object[] helpreParams, boolean reboot) throws BatchException {
        logger.debug("executeHelper - start");
        int ret = 0;
        AbstractHelper helper = null;
        try {
            while (true) {
                helper = HelperPool.getHelper(helperName);
                if (helper != null) break;
            }

            helper.setParameters(helpreParams);
            helper.setReboot(reboot);
            // ExecutorService を使用するために変更
            //helper.start();

            HelperPool.returnHelper(helperName,helper);
            ret = helper.hashCode();
        } catch (BatchException be) {
            logger.error("createHelper - BatchException");
            throw be;
        } catch (Exception e) {
            logger.error("createHelper - Exception");
            throw new BatchException(e);
        }
        logger.debug("executeHelper - end");
        return ret;
    }


    /**
     * Helperクラスインスタンスを返す.<br>
     * Helperが存在しない場合はExceptionを返す.<br>
     * 
     * 
     * @param helperName helperName名
     * @param helpreParams helperパラメータ配列
	 * @param inputHelperShareParam ヘルパー共有領域に事前に登録したいパラメータ Key-Valueのセットで配列登録すること(inputHelperShareParam[0]=1番目の要素のKey,inputHelperShareParam[1]=1番目の要素のValue,inputHelperShareParam[2]=2番目の要素のKey,inputHelperShareParam[3]=2番目の要素のValue)
     * @return AbstractHelper Helperインスタンス
     * @throws BatchException
     **/
    protected int executeHelper(String helperName, Object[] helpreParams, boolean reboot, Object[] inputHelperShareParam) throws BatchException {
        logger.debug("executeHelper - start");
        int ret = 0;
        AbstractHelper helper = null;
        try {
            while (true) {
                helper = HelperPool.getHelper(helperName);
                if (helper != null) break;
            }

			for (int i = 0; i < inputHelperShareParam.length; i=i+2) {
				helper.setHelperShareParam(inputHelperShareParam[i], inputHelperShareParam[i+1]);
			}

            helper.setParameters(helpreParams);
            helper.setReboot(reboot);
            // ExecutorService を使用するために変更
            //helper.start();

            HelperPool.returnHelper(helperName,helper);
            ret = helper.hashCode();
        } catch (BatchException be) {
            logger.error("createHelper - BatchException");
            throw be;
        } catch (Exception e) {
            logger.error("createHelper - Exception");
            throw new BatchException(e);
        }
        logger.debug("executeHelper - end");
        return ret;
    }


    /**
     * Job設定ファイルの自由に設定出来る値を取得する.<br>
     * 設定情報が存在しない場合はnullを返す.<br>
     * 
     * @param key 設定情報のキー名
     * @return String ユーザ設定パラメータ
     **/
    protected String getPropertiesValue(String key) {
        return ConfigFolder.getJobUserParam(key);
    }

    /**
     * Job設定ファイルの変更をチェック.<br>
     * 
     * @return boolean 変更有無
     **/
    protected boolean isJobFileChange() throws BatchException {
        return ConfigFolder.isJobFileChange();
    }

    /**
     * Job設定ファイルの指定のキー値を再読み込みする.<br>
     * 
     * @param String[] キー値
     **/
    protected void reloadJobFileParameter(String[] keys) throws BatchException {
        ConfigFolder.reloadJobFileParameter(keys);
    }



    /**
     * 自身のステータスを返す
     * 
     * @return String ステータス文字列
     */
    public String getStatus() {
        return this.status;
    }

    /**
     * Helperのステータスを返す
     * 
     * @param hashCode Helperのコード
     * @return String ステータス文字列
     */
    public String getHelperStatus(int code) {
        return HelperPool.getHelperStatus(code);
    }

    /**
     * 指定のHelperの実行中の数を返す
     * 
     * @param hashName Helper名
     * @return int 実行中の数
     */
    public int getActiveHelperCount(String helperName) {
        return HelperPool.getActiveHelperInstanceCount(helperName);
    }

    /**
     * Helperの戻り値を返す.<br>
     * 本メソットで値が取得できるのは1度のみ.<br>
     * 対象Helperが終了していない状態で本クラスを呼び出すとBatchExceptionがthrowされる.<br>
     * 必ずgetHelperStatusメソッドで終了を確認してから呼び出すこと.<br>
     * 実装例)Jobクラス内で呼び出し
     * int helperCode = executeHelper("TestHelper",new {"param"});
     * Object helperRet = null;
     *
     *  // Helperからの戻り値を取得する
     *  System.out.println("Helper End Wait Start");
     *      while(true) {
     *          System.out.println("Wait...");
     *          if (super.getHelperStatus(helperCode).equals(BatchDefine.JOB_STATUS_END)) {
     *              helperRet = super.removeHelperReturnParam(helperCode);
     *              break;
     *          }
     *
     *          if (super.getHelperStatus(helperCode).equals(BatchDefine.JOB_STATUS_ERR)) {
     *              break;
     *          }
     *          // 100ミリ停止
     *          Thread.sleep(100);
     *
     *      }
     *      System.out.println(helperRet);
     *
     *
     * @param hashCode Helperのコード
     * @return Object 戻り値
     */
    public Object removeHelperReturnParam(int code) throws BatchException {
        Object ret = null;
        if (HelperPool.getHelperStatus(code).equals(BatchDefine.JOB_STATUS_END)) {
            ret = HelperPool.getReturnParam(code);
            HelperPool.cleanEndHelper(code);
        } else {
            throw new BatchException("Helper No End Status!!");
        }
        return ret;
    }


    /**
     * Helperの戻り値を返す.<br>
     * 本メソットで値が取得できるのは1度のみ.<br>
     * 本メソッドはHelperが終了もしくはエラーになるまで待ち、その後値を返す.<br>
     * 返却値はObjectの配列で1番目の要素がステータスとなり、2番目の要素が<br>
     * 返却値となる.<br>
     * ERRORステータスの場合値はnullとなる.<br>
     * HelperPoolが終了の値を回収するのに10秒のタイムラグがある為、10秒以上は停止するべきである.<br>
     *
     *
     * @param hashCode Helperのコード
     * @param limitTile リターン値が指定秒以内に返却されない場合はnullを返す.<br>
     *                  指定は秒.<br>
     * @return Object[] 1番目の要素がステータス、2番目の要素が返却値
     */
    public Object[] waitGetHelperReturnParam(int helperCode, int limitTime) throws BatchException {
        Object[] ret = null;
        Object helperRet = null;
        try {
            
            for(int i = 0; i < limitTime; i++) {
                if (getHelperStatus(helperCode).equals(BatchDefine.JOB_STATUS_END)) {
                    helperRet = removeHelperReturnParam(helperCode);
                    ret = new Object[2];
                    ret[0] = BatchDefine.JOB_STATUS_END;
                    ret[1] = helperRet;
                    break;
                }

                if (getHelperStatus(helperCode).equals(BatchDefine.JOB_STATUS_ERR)) {
                    ret = new Object[2];
                    ret[0] = BatchDefine.JOB_STATUS_ERR;
                    ret[1] = null;
                    break;
                }

                // 980ミリ停止
                Thread.sleep(980);
            }
        } catch (BatchException be) {
            throw be;
        } catch (Exception e) {
            throw new BatchException(e);
        }
        return ret;
    }

    /**
     * 自身の名前を返す
     *
     * @return String Job名文字列
     */
    public String getJobName() {
        return this.jobConfigMap.getJobName();
    }

    /**
     * Job間で共有する値をセットする.<br>
     * 
     * @param key キー値
     * @param val 値
     */
    public void setJobShareParam(Object key, Object val) {
        jobParamShareMap.put(key, val);
    }

    /**
     * Job間で共有する値を取得する.<br>
     *  存在しない場合はNullを返す.<br>
     *
     * @param key キー値
     * @return Object 値
     */
    public Object getJobShareParam(Object key) {
        Object ret = null;

        if (jobParamShareMap.containsKey(key)) {

            ret = jobParamShareMap.get(key);
        }
        return ret;
    }

    /**
     * Job間で共有する値のキー一覧を返す.<br>
     * 
     * @return Object[] キー値配列
     */
    public Object[] getJobShareParamKeys() {
        Set keys = jobParamShareMap.keySet();
        Object[] keyList = new Object[keys.size()];
        int index = 0;

        if (keys != null) {
            for (Iterator iterator = keys.iterator(); iterator.hasNext();) {
                keyList[index] = iterator.next();
                index++;
            }
        }

        return keyList;
    }

    /**
     * PreProcessの返り値値をセット.<br>
     *
     * @param preProcessRet PreProcessの返り値
     */ 
    public void setPreProcess(String preProcessRet) {
        this.preProcessRet = preProcessRet;
    }

    /**
     * PostProcessの値をセット.<br>
     *
     * @param postProcessRet PostProcessの返り値
     */ 
    public void setPostProcess(String postProcessRet) {
        this.postProcessRet = postProcessRet;
    }

    /**
     * PreProcessの戻り値を返す.<br>
     *
     * @return String PreProcessの戻り値
     */ 
    protected String getPreProcess() {
        return preProcessRet;
    }

    /**
     * PostProcessの戻り値を返す.<br>
     * PostProcessは全てのJobが終了してから呼び出されるので、<br>
     * この値は、reloopをtrueにした場合に前回処理のPostProcessの戻り値が取得可能となる<br>
     *
     * @return String PostProcessの戻り値
     */ 
    protected String getPostProcess() {
        return postProcessRet;
    }

}
