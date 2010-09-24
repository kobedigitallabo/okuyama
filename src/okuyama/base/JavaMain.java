package okuyama.base;

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
        logger.info("main - start");
        try {

            if (args == null || args.length < 2) {
                throw new BatchException("設定ファイルが引数として渡されませんでした");
            }

            JavaMain me = new JavaMain();
            me.exec(args[0],args[1]);

        } catch (BatchException be) {
            logger.error("main - error", be);
            System.exit(9);
        }

        logger.info("main - end");
    }

    /**
     * メイン処理.<br>
     *
     * @param batchConfPath バッチ設定ファイルパス
     * @param jobConfPath Job設定ファイルパス
     * @throws BatchException
     */
    public void exec(String batchConfPath, String jobConfPath) throws BatchException {
        logger.info("exec - start");
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
            logger.info("exec - error1");
            throw be;
        } catch (Exception e) {
            logger.info("exec - error2");
            throw new BatchException(e);
        }
        logger.info("exec - end");
    }
}