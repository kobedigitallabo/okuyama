package org.batch.job;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.HashMap;

import org.batch.parameter.config.ConfigFolder;
import org.batch.parameter.config.HelperConfigMap;
import org.batch.lang.BatchException;
import org.batch.lang.BatchDefine;
import org.batch.util.ILogger;
import org.batch.util.LoggerFactory;
/**
 * JobHelperの基底クラス.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
abstract public class AbstractHelper extends Thread {

    // Logger
    private ILogger logger = LoggerFactory.createLogger(AbstractHelper.class);

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
    protected HelperConfigMap helperConfigMap = null;

    // 使用回数
    private int useCount = 0;

    /**
     * Helper同士で値やり取りを行う領域.<br>
     * 一度登録した値はHelperが終了するまで維持される.<br>
     */
    private static Hashtable helperParamShareMap = new Hashtable();

    // 呼び出し時に直接渡すパラメータ
    private Object[] parameters = null;

    // Helper実行後に返したい値を格納する
    private Object returnParameter = null;

    /**
     * コンストラクタ
     */
    public AbstractHelper() {
    }

    /**
     * 初期化メソッド.<br>
     */
    public void initialize() {
        this.status = WAIT;
        this.parameters = null;
        this.returnParameter = null;
        this.useCount++;
    }

    /**
     * 自身の設定情報格納
     * @param helperConfigMap Job設定情報
     */
    public void setConfig(HelperConfigMap helperConfigMap) {
        this.helperConfigMap = helperConfigMap;
    }

    /**
     * 自身の設定情報を返す
     * @return HelperConfigMap Job設定情報
     */
    public HelperConfigMap getConfig() {
        return this.helperConfigMap;
    }


    /**
     * initHelper.<br>
     * ユーザHelper実装部分.<br>
     * @param initValue
     */
    public abstract void initHelper(String initValue);


    /**
     * executeHelper.<br>
     * ユーザHelper実装部分.<br>
     *
     * @param optionParam
     * @return String 実行結果
     */
    public abstract String executeHelper(String optionParam) throws BatchException;

    /**
     * endHelper.<br>
     * ユーザHelper終了処理部分.<br>
     *
     * @return String 実行結果
     */
    public abstract void endHelper();

    /**
     * メイン実行部分.<br>
     * 自身のサブクラスを実行.<br>
     * 実行順序はinitHelper - executeHelper.<br>
     * 
     */
    public void run() {
        logger.debug("Helper - run - start");

        String retStatus = null;

        // ステータスを実行中に変更
        this.status = RUN;


        try {


            // 初期化メソッド呼び出し
            initHelper(this.helperConfigMap.getHelperInit());
            // 実行メインメソッド呼び出し
            retStatus = executeHelper(this.helperConfigMap.getHelperOption());

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
                    throw new BatchException("Job終了方法が不正:必ずSUCCESSかERRORを返す必要あり");
                }
            } else {
                throw new BatchException("Job終了方法が不正:必ずSUCCESSかERRORを返す必要あり");
            }
        } catch (BatchException be) {
            logger.error("AbstractHelperJob - run - BatchException ",be);

            // ステータスをエラーにする
            this.status = ERR;
        } catch (Exception e) {
            logger.error("AbstractHelperJob - run - Exception",e);
            // ステータスをエラーにする
            this.status = ERR;
        }
        logger.debug("Helper - run - end");
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
     * Helper間で共有する値をセットする.<br>
     * 
     * @param key キー値
     * @param val 値
     */
    public void setHelperShareParam(Object key, Object val) {
        helperParamShareMap.put(key, val);
    }

    /**
     * Helper間で共有する値を取得する.<br>
     *  存在しない場合はNullを返す.<br>
     *
     * @param key キー値
     * @return Object 値
     */
    public Object getHelperShareParam(Object key) {
        Object ret = null;

        if (helperParamShareMap.containsKey(key)) {

            ret = helperParamShareMap.get(key);
        }
        return ret;
    }

    /**
     * Helper用のパラメータ設定.<br>
     *
     * @param parameters パラメータ値
     */
    public void setParameters(Object[] parameters) {
        this.parameters = parameters;
    }

    /**
     * Helper用のパラメータ取得.<br>
     *
     * @return Object[] パラメータ値
     */
    public Object[] getParameters() {
        return this.parameters;
    }


    /**
     * Helperが終了した際に外部に伝播したいパラメータをセット.<br>
     *
     * @param Object パラメータ値
     */
    public void setReturnParameter(Object value) {
        this.returnParameter = value;
    }

    /**
     * Helperが終了した際に外部に伝播したいパラメータを取得.<br>
     *
     * @return Object パラメータ値
     */
    public Object getReturnParameter() {
        return this.returnParameter;
    }

    // 
    public int getUseCount() {
        return this.useCount;
    }

}
