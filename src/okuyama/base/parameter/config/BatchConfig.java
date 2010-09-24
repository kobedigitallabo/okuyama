package okuyama.base.parameter.config;

import java.io.InputStream;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Hashtable;

import okuyama.base.lang.BatchException;
import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;
import okuyama.base.util.ClassUtility;

/** 
 * Batchの設定を読み込み保持する.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class BatchConfig {

    // Logger
    private ILogger logger = LoggerFactory.createLogger(BatchConfig.class);

    // 設定情報格納用Table
    private Hashtable configTable = null;

    /**
     * 設定ファイル名を渡すことにより、生成.<br>
     * 
     * @param fileName
     * @throws BatchException
     */
    public BatchConfig(String fileName) throws BatchException {
        this.configTable = new Hashtable();
        this.initConfig(BatchConfig.class.getResourceAsStream(fileName));
    }

    /**
     * 設定ファイルを解析し自身に蓄える.<br>
     *
     * @param is ファイルストリーム
     * @throws BatchException
     */
    private void initConfig(InputStream is) throws BatchException {
        logger.debug("BatchConfig - initConfig - start");
        Properties prop = null;

        String key = null;

        String[] kyes;
        int index = 0;

        try {
            prop = new Properties();
            prop.load(is);

            Set keys = prop.keySet();   

            for (Iterator iterator = keys.iterator(); iterator.hasNext();) {

                key = (String)iterator.next();

                this.configTable.put(key, prop.getProperty(key));
            }

            // 設定ファイルをチェック
            this.checkConfig();

        } catch(Exception e) {
            logger.error("BatchConfig - initConfig - Exception", e);
            throw new BatchException(e);
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (IOException ie) {
                // 無視
                logger.error("initConfig - ファイルストリームのcloseに失敗");
            }
        }
        logger.debug("BatchConfig - initConfig - end");
    }

    /**
     * 設定情報をチェック
     */
    private void checkConfig() {
        return ;
    }

    /**
     * 設定されている設定情報を取り出す.<br>
     * 存在しない場合はnullを返す.<br>
     *
     * @param key バッチ設定ファイルに記述されているキー値
     * @return String バッチ設定ファイルに記述されている設定値
     */
    public String getBatchParam(String key) {
        String ret = null;

        if (this.configTable.containsKey(key)) {

            ret = (String)this.configTable.get(key);
        }
        return ret;
    }
}