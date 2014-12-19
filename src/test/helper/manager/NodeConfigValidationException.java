package test.helper.manager;

/**
 * Nodeのpropertiesファイルの設定ミスを通知するための例外クラス。
 * @author s-ito
 *
 */
public class NodeConfigValidationException extends Exception {
	
	/**
	 * コンストラクタ。
	 * @param key - 設定ミスがあるkey。
	 * @param msg - メッセージ。
	 */
	public NodeConfigValidationException(String key, String msg) {
		super("[" + key + "] " + msg);
	}
	
	/**
	 * コンストラクタ。
	 * @param key - 設定ミスがあるkey。
	 * @param thrown - 原因となった例外。
	 */
	public NodeConfigValidationException(String key, Throwable thrown) {
		super("[" + key + "] " + thrown.getClass().getName() + " : " + thrown.getMessage(), thrown);
	}
}
