package okuyama.base;

import okuyama.base.lang.BatchDefine;
import okuyama.base.lang.BatchException;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.base.util.ClassUtility;
import okuyama.base.parameter.config.BatchConfig;
import okuyama.base.parameter.config.ConfigFolder;
import okuyama.base.parameter.config.JobConfig;

/**
 * メインクラス.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class JavaMain {

    // Logger
    private static ILogger logger = LoggerFactory.createLogger(JavaMain.class);

    /**
     * 起動メソッド
     * @param args [0]=バッチ用設定ファイル名, [1]=Job用設定ファイル名
     */
    public static void main(String[] args) {
        logger.info("JavaMain - start");
        try {

            if (args == null || args.length < 2) {
                if (args == null) {
                    System.err.println("Error : JavaMain The argument is illega [Main.properties, Node.properties]");
                    throw new BatchException("JavaMain Configure File Not Found");
                } else {
                    System.err.println("Error : JavaMain The argument is illega [Node.properties]");
                    throw new BatchException("JavaMain Configure File Not Found");
                }
            }

            JavaMain me = new JavaMain();

            // 起動オプションを取り込み
            if (args.length > 2) {
                for (int i = 2; i < args.length; i++) {
                    BatchDefine.USER_OPTION_STR = BatchDefine.USER_OPTION_STR + " " + args[i];
                }
                BatchDefine.USER_OPTION_STR = BatchDefine.USER_OPTION_STR.trim();
            } else {
                BatchDefine.USER_OPTION_STR = null;
            }

            // 本体処理実行
            me.exec(args[0],args[1]);

        } catch (BatchException be) {
            logger.error("JavaMain - error", be);
            System.exit(1);
        }

        logger.info("JavaMain - end");
    }

    /**
     * メイン処理.<br>
     *
     * @param batchConfPath バッチ設定ファイルパス
     * @param jobConfPath Job設定ファイルパス
     * @throws BatchException
     */
    public void exec(String batchConfPath, String jobConfPath) throws BatchException {
        logger.debug("JavaMain - exec - start");
        String controllerClassName = null;

        try {
            BatchConfig batchConfig = new BatchConfig(batchConfPath);
            JobConfig jobConfig = new JobConfig(jobConfPath);

            // Folderに設定情報を格納
            ConfigFolder.setConfig(batchConfig, jobConfig);

            // Controller作成
            IJobController jobController = (IJobController)ClassUtility.createInstance(batchConfig.getBatchParam("controller"));
            // 実行
            jobController.execute();
        } catch (BatchException be) {
            logger.info("JavaMain - exec - error1");
            throw be;
        } catch (Exception e) {
            logger.info("JavaMain - exec - error2");
            throw new BatchException(e);
        }
        logger.debug("JavaMain - exec - end");
    }

    public static void shutdownMainProccess() {
        System.exit(0);
    }
}