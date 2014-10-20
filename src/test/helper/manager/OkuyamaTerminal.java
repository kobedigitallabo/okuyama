package test.helper.manager;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Okuyamaの操作を行うための端末インタフェース。
 * @author s-ito
 *
 */
public interface OkuyamaTerminal {

	/**
	 * ディレクトリを作成する。
	 * @param path - 作成するディレクトリのパス。
	 * @return 作成に成功すればtrueを返す。
	 */
	boolean mkdir(String path);
	/**
	 * ファイル・ディレクトリを削除する。
	 * @param path - 削除するファイル・ディレクトリのパス。
	 * @return 削除に成功すればtrueを返す。
	 */
	boolean delete(String path);
	/**
	 * 指定ディレクトリ内の一覧を取得する。
	 * @param path - 一覧取得対象ディレクトリのパス。
	 * @return 指定ディレクトリ内の一覧。
	 */
	String[] ls(String path);
	/**
	 * ファイルの存在を確認する。
	 * @param path - 確認対象のパス。
	 * @return 存在する場合はtrueを返す。
	 */
	boolean exist(String path);
	/**
	 * 親パスを取得する。
	 * @param path - 取得対象。
	 * @return 引数pathの親パス。
	 */
	String getParentPath(String path);
	/**
	 * パスを連結する。
	 * @param parent - 連結先。
	 * @param child - 連結するパス。
	 * @returen 連結したパス。
	 */
	String concatPath(String parent, String child);
	/**
	 * ファイルをロードする。
	 * @param path - ロード対象ファイルパス。
	 * @return ロードのためのInputStream。
	 */
	InputStream load(String path) throws IOException;
	/**
	 * ファイルを出力する。
	 * @param path - 出力先ファイルのパス。
	 * @return 出力用OutputStream。
	 */
	OutputStream write(String path) throws IOException;
	/**
	 * Nodeを起動する。
	 * @param currentDir - カレントディレクトリ。
	 * @param classpath - クラスパス。
	 * @param mainProperties - Main.propertiesのパス。
	 * @param nodeProperties - Nodeのpropertiesのパス。
	 * @param jvmOption - JVMのオプション。
	 * @param okuyamaOption - okuyamaの起動引数。
	 * @return 実行プロセス。
	 */
	TerminalProcess executeNode(String currentDir, String[] classpath, String mainProperties,
								String nodeProperties, String[] jvmOption, String[] okuyamaOption) throws IOException;
	/**
	 * telnetでNodeに接続する。
	 * @param host - 接続先ホスト。
	 * @param port - 接続先ポート。
	 * @param command - 実行コマンド。
	 * @return コマンドの実行結果。
	 */
	String connectNodeByTelnet(String host, int port, String command) throws Exception;
}
