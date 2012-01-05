package okuyama.base.util;

import java.io.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.Level;

/**
 * 標準ログクラス.<br>
 * Log4jをラップしたのみ<br>
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class DefaultLogger implements ILogger {

   public static String propertiesFileName = "log4j.properties";

    Logger logger = null;


    public DefaultLogger(Class clazz) {

        //設定ファイルを読み込む
        if (new File(propertiesFileName).exists()) {

            this.logger = Logger.getLogger(clazz);
            PropertyConfigurator.configure(propertiesFileName);
        } else if (DefaultLogger.class.getResource("/log4j.properties") != null) {

            this.logger = Logger.getLogger(clazz);
            PropertyConfigurator.configure(DefaultLogger.class.getResource("/log4j.properties"));
        } else {

            RollingFileAppender rollingFileAppender = null;

            this.logger = Logger.getLogger(clazz);
            this.logger.setLevel(Level.ERROR);

            PatternLayout layout = new PatternLayout("%d %5p %c{1} - %m%n");

            try{

                rollingFileAppender = new RollingFileAppender(layout, "okuyama_default.log");
                rollingFileAppender.setMaxFileSize("128MB");
            }catch(Exception e){
                e.printStackTrace();
                System.exit(1);
            }

            this.logger.addAppender(rollingFileAppender);
        }
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


    public boolean isDebugEnabled() {
        return this.logger.isDebugEnabled();
    }


    public boolean isInfoEnabled() {
        return this.logger.isInfoEnabled();
    }

}