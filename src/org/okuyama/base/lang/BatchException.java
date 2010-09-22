package org.okuyama.base.lang;

/**
 * Exceptionの共通クラス.<br>
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class BatchException extends Exception {

    public BatchException() {
        super();
    }

    /**
     * コンストラクタ
     *
     * @param message 例外文字列
     */
    public BatchException(String message) {
        super(message);
    }

    /**
     * コンストラクタ
     *
     * @param message 例外文字列
     * @param th   例外
     */
    public BatchException(String message, Throwable th) {
        super(message, th);
    }

    /**
     * コンストラクタ
     *
     * @param th      例外
     */
    public BatchException(Throwable th) {
        super(th);
    }
}