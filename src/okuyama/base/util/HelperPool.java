package okuyama.base.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import java.util.ArrayList;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.base.lang.BatchDefine;
import okuyama.base.lang.BatchException;
import okuyama.base.job.AbstractJob;
import okuyama.base.job.AbstractHelper;
import okuyama.base.parameter.config.HelperConfigMap;


/**
 * Helperの蓄積と状態の管理を行うスレッド.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class  HelperPool extends Thread {

    // Logger
    private static ILogger logger = LoggerFactory.createLogger(HelperPool.class);

    // Helper管理用
    private static CopyOnWriteArrayList helperNameList = new CopyOnWriteArrayList();
    private static ConcurrentHashMap configMap = new ConcurrentHashMap(1024, 1000, 512);
    private static ConcurrentHashMap helperMap = new ConcurrentHashMap(1024, 1000, 512);
    private static ConcurrentHashMap helperReturnParamMap = new ConcurrentHashMap(1024, 1000, 512);
    private static ConcurrentHashMap helperStatusMap = new ConcurrentHashMap(8192, 1000, 512);
    private static ConcurrentHashMap executorServiceMap = new ConcurrentHashMap(1024, 1000, 512);
    private static ConcurrentHashMap serviceParameterQueueMap = new ConcurrentHashMap(1024, 1000, 512);
    private static CopyOnWriteArrayList allExecuteHelperList = new CopyOnWriteArrayList();


    // Helperチェック間隔
    private static int helperCheckTime = 2500;

    // HelperThread Join待機時間
    private static int helperThreadJoinTime = 5000;

    // スレッド内でのチェックをコントロール
    private boolean poolRunning = true;


    // 呼び出し時に直接渡すパラメータ
    private static ArrayBlockingQueue helperParamQueue = new ArrayBlockingQueue(1000);

    public HelperPool() {
        poolRunning = true;
        
    }

    public void run () {
        logger.debug("HelperPool - run - start");
        try {

            while(this.poolRunning) {

                Thread.sleep(helperCheckTime);
                int size = allExecuteHelperList.size();

                for (int i = 0; i < size; i++) {

                    AbstractHelper helper = (AbstractHelper)allExecuteHelperList.get(size - 1 - i);

                    if(helper.getThreadEnd()) {

                        cleanEndHelper(helper.hashCode());

                        allExecuteHelperList.remove(size - 1 - i);

                        if(helper.getReboot()) {

                            logger.debug("Helper[" + helper.getName() + " Reboot Start");
                            AbstractHelper newHelper = getHelper(helper.getName());
                            newHelper.setParameters(helper.getParameters());
                            newHelper.setReboot(true);
                            returnHelper(newHelper.getName(), newHelper);
                            allExecuteHelperList.add(newHelper);
                            logger.debug("Helper[" + helper.getName() + " Reboot Success");
                        }

                        ((ThreadPoolExecutor)executorServiceMap.get(helper.getName())).remove(helper);
                    }
                }
            }
            logger.debug("HelperPool - End Process Start");
            // システムの停止が要求されているのでHelperを制終了
            for (int i = 0; i < helperNameList.size(); i++) {
                ((ThreadPoolExecutor)executorServiceMap.get((String)helperNameList.get(i))).shutdown();
            }


            logger.debug("HelperPool - End Process End");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("HelperPool - run - Error", e);
        }
    }

    /**
     * 明示的に初期化を行う
     */
    public static void initializeHelperPool() {
        helperNameList = new CopyOnWriteArrayList();
        configMap = new ConcurrentHashMap(1024, 1000, 512);
        helperMap = new ConcurrentHashMap(1024, 1000, 512);
        helperReturnParamMap = new ConcurrentHashMap(1024, 1000, 512);
        helperStatusMap = new ConcurrentHashMap(1024, 1000, 512);
        serviceParameterQueueMap = new ConcurrentHashMap(1024, 1000, 512);
    }

    /**
     * Helperインスタンスを貸し出す.<br>
     * Helperが定義として存在しない場合BatchExceptionがthrowされる.<br>
     * 全て貸し出し中の場合はNullが返る.<br>
     *
     * @param String helperName
     * @return AbstractHelper ヘルパー
     * @throw  BatchException
     */
    public static AbstractHelper getHelper(String helperName) throws BatchException {
        logger.debug("HelperPool - getHelper - start");
        AbstractHelper helper = null;
        Integer useCount = null;
        ArrayList helperList = null;

        try {
            if (!helperMap.containsKey(helperName)) 
                throw new BatchException("Helper [" + helperName + "] Not Found");

            HelperConfigMap helperConfigMap = (HelperConfigMap)configMap.get(helperName);
            helper = ClassUtility.createHelperInstance(helperConfigMap.getHelperClassName());

            // Jobに設定情報を格納
            helper.setName(helperName);
            helper.setConfig(helperConfigMap);
            helper.initialize();
            helperStatusMap.put(new Integer(helper.hashCode()), helper.getStatus());

        } catch (Exception e) {
            logger.error("HelperPool - getHelper - BatchException");
            throw new BatchException(e);
        }
        logger.debug("HelperPool - getHelper - end");
        return helper;
    
    }


    /**
     * Helperインスタンスを返す.<br>
     * Helperが定義として存在しない場合BatchExceptionがthrowされる.<br>
     *
     * @param String helperName
     * @param AbstractHelper ヘルパーインスタンス
     * @throw  BatchException
     */
    public static void returnHelper(String helperName, AbstractHelper helper) throws BatchException {

        try {
            // ExecutorService を使用するために変更. 2010/03/22
            ((ThreadPoolExecutor)executorServiceMap.get(helperName)).execute(helper);
            allExecuteHelperList.add(helper);
        } catch (Exception be) {
            logger.error("HelperPool - returnHelper - BatchException");
            throw new BatchException(be);
        }
    }

    public static int getActiveHelperInstanceCount(String helperName) {
        return ((ThreadPoolExecutor)executorServiceMap.get(helperName)).getActiveCount();
    }

    /**
     * Helperのプールを初期化する.<br>
     * 
     * @param HelperConfigMap helperConfigMap プールしたHelperの設定ファイル
     */
    public static void managedHelperConfig(HelperConfigMap helperConfigMap) throws BatchException {

        helperNameList.add(helperConfigMap.getHelperName());
        configMap.put(helperConfigMap.getHelperName(),helperConfigMap);
        helperMap.put(helperConfigMap.getHelperName(),new ArrayList());
        executorServiceMap.put(helperConfigMap.getHelperName(), Executors.newCachedThreadPool());
        serviceParameterQueueMap.put(helperConfigMap.getHelperName(), new ArrayBlockingQueue(20000));
    }


    /**
     * オリジナルのキュー領域を作成する.<br>
     *
     * @param uniqueQueueName キュー名
     * @param size キューの最大サイズ
     */
    public static void createUniqueHelperParamQueue(String uniqueQueueName, int size) {
        serviceParameterQueueMap.put(uniqueQueueName, new ArrayBlockingQueue(size));
    }


    /**
     * パラメータキューのサイズを返す.<br>
     *
     * @param queueName 名前
     */
    public static int getParameterQueueSize(String queueName) throws Exception {
        int ret = -1;
        try {
            ret = ((ArrayBlockingQueue)serviceParameterQueueMap.get(queueName)).size();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        return ret;
    }


    /**
     * Helper用のパラメータキューに追加.<br>
     * 全てのHelper共通.<br>
     *
     * @param params パラメータ
     */
    public static void addParameterQueue(Object[] params) throws Exception {
        try {
            helperParamQueue.put(params);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }


    /**
     * Helper用のパラメータキューに追加.<br>
     * Helper個別.<br>
     *
     * @param params パラメータ
     */
    public static void addSpecificationParameterQueue(String helperName, Object[] params) throws Exception {
        try {
            addSpecificationParameterQueue(helperName, params, false);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }


    /**
     * Helper用のパラメータキューに追加.<br>
     * Helper個別.<br>
     *
     * @param params パラメータ
     */
    public static void addSpecificationParameterQueue(String helperName, Object[] params, boolean debug) throws Exception {
        try {
            if (false) {
                System.out.println(helperName + " =[" + ((ArrayBlockingQueue)serviceParameterQueueMap.get(helperName)).size() + "]");
            }
            ((ArrayBlockingQueue)serviceParameterQueueMap.get(helperName)).put(params);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }





    /**
     * Helper用のパラメータキューから取得
     * 全てのHelper共通
     *
     * @return Object[] パラメータ
     */
    public static Object[] pollParameterQueue() {
        Object[] ret = null;
        try {
            
            ret = (Object[])helperParamQueue.take();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }


    /**
     * Helper用のパラメータキューに追加|.<br>
     * Helper個別.<br>
     *
     * @param params パラメータ
     */
    public static Object[] pollSpecificationParameterQueue(String helperName) {
        Object[] ret = null;

        try {
            ret = (Object[])((ArrayBlockingQueue)serviceParameterQueueMap.get(helperName)).take();
        } catch (Exception e) {
            logger.error("pollSpecificationParameterQueue - ERROR [" + helperName  +"]");
            logger.error(serviceParameterQueueMap);
            e.printStackTrace();
        }

        return ret;
    }


    /**
     * Helperが終了時にセットした返却値を取得する.<br>
     *
     * @param helperHashCode 対象HelperオブジェクトのhashCode
     * @return Object 返却値 値がセットされていない場合はnullが返る
     */
    public static Object getReturnParam(int helperHashCode) {
        Object ret = null;
        if (helperReturnParamMap.containsKey(new Integer(helperHashCode))) {
            ret = helperReturnParamMap.get(new Integer(helperHashCode));
        }
        return ret;
    }

    /**
     * Helperが終了後のリソースを破棄する.<br>
     *
     * @param helperHashCode 対象HelperオブジェクトのhashCode
     */
    public static void cleanEndHelper(int helperHashCode) {

        if (helperStatusMap.containsKey(new Integer(helperHashCode))) {
            helperStatusMap.put(new Integer(helperHashCode), BatchDefine.JOB_STATUS_END);
        }

        if (helperReturnParamMap.containsKey(new Integer(helperHashCode))) {
            helperReturnParamMap.remove(new Integer(helperHashCode));
        }
    }


    /**
     * 指定したHelperのステータスを返す.<br>
     *
     * @param helperHashCode 対象HelperオブジェクトのhashCode
     * @return String ステータス文字列　Helperが存在しない場合はnullが返る
     */
    public static String getHelperStatus(int helperHashCode) {
        String ret = null;
        if (helperStatusMap.containsKey(new Integer(helperHashCode))) {
            ret = (String)helperStatusMap.get(new Integer(helperHashCode));
        }
        return ret;
    }

    public void poolEnd() {
        this.poolRunning = false;
    }


}