package test.helper.manager;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * OkuyamaTerminalで実行されたプロセスへのインタフェース。
 * @author s-ito
 *
 */
public interface TerminalProcess {

	/**
	 * プロセスの標準出力を取得する。
	 * @return 標準出力のデータを取得するためのInputStream。
	 */
	InputStream getStandardOutput();
	/**
	 * プロセスの標準エラー出力を取得する。
	 * @return 標準エラー出力のデータを取得するためのInputStream。
	 */
	InputStream getStandardError();
	/**
	 * プロセスの標準入力を取得する。
	 * @return 標準入力にデータを渡すためのOutputStream。
	 */
	OutputStream getStandardInput();
	/**
	 * プロセスの終了を確認する。
	 * @return プロセスが終了していればtrueを返す。
	 */
	boolean isEnd();
}
