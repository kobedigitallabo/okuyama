package org.okuyama.base.util;

/**
 * ロガーファクトリ.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class LoggerFactory {

    // まだ何も特殊なことはしていない
    public static ILogger createLogger(Class clazz) {
        return new DefaultLogger(clazz);
    }

    // まだ何も特殊なことはしていない
    public static ILogger createLogger(Class clazz, String configFile) {
        return new DefaultLogger(clazz, configFile);
    }
}