package okuyama.base.util;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * 標準ログクラス.<br>
 * Log4jをラップしたのみ<br>
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class DefaultLogger implements ILogger {

    Logger logger = null;

    public DefaultLogger(Class clazz) {
        this.logger = Logger.getLogger(clazz);

        //設定ファイルを読み込む
        PropertyConfigurator.configure("log4j.properties");
    }

    public DefaultLogger(Class clazz, String configFile) {
        this.logger = Logger.getLogger(clazz);

        //設定ファイルを読み込む
        PropertyConfigurator.configure(configFile);
    }


    public void trace(Object message) {
        this.logger.trace(message); 
    }

    public void trace(Object message, Throwable t) {
        this.logger.trace(message, t);
    }

    public void debug(Object message) {
        this.logger.debug(message);
    }

    public void debug(Object message, Throwable t) {
        this.logger.debug(message, t);
    }

    public void info(Object message) {
        this.logger.info(message);
    }

    public void info(Object message, Throwable t) {
        this.logger.info(message, t);
    }

    public void error(Object message) {
        this.logger.error(message);
    }

    public void error(Object message, Throwable t) {
        this.logger.error(message, t);
    }

    public void fatal(Object message) {
        this.logger.fatal(message);
    }

    public void fatal(Object message, Throwable t) {
        this.logger.fatal(message, t);
    }

    public void warn(Object message) {
        this.logger.warn(message);
    }

    public void warn(Object message, Throwable t) {
        this.logger.warn(message, t);
    }

}