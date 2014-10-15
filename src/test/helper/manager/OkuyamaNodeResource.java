package test.helper.manager;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * OkuyamaのNodeが使うリソース操作用クラス。
 * @author s-ito
 *
 */
public class OkuyamaNodeResource {

	private Logger logger = Logger.getLogger(OkuyamaNodeResource.class.getName());
	/**
	 * 操作用端末。
	 */
	private OkuyamaTerminal terminal;
	/**
	 * Nodeのカレントディレクトリ。
	 */
	private String current;
	/**
	 * Nodeのクラスパス。
	 */
	private String[] classpath;
	
	/**
	 * コンストラクタ。
	 * @param terminal - 操作用端末。
	 * @param current - Nodeのカレントディレクトリ。
	 * @param classpath - Nodeのクラスパス。
	 */
	public OkuyamaNodeResource(OkuyamaTerminal terminal, String current, String[] classpath) {
		this.terminal = terminal;
		this.current = current;
		this.classpath = classpath;
	}
	
	/**
	 * リソースを削除する。
	 * @param path - 削除対象リソースのパス。
	 * @return 成功すればtrueを返す。
	 */
	public boolean delete(String path) {
		path = this.resolvePath(path);
		return this.terminal.delete(path);
	}
	
	/**
	 * ファイルを読み込む。
	 * @param path - 読み込み対象のパス。
	 * @return 読み込み用ストリーム。
	 */
	public InputStream load(String path) throws IOException {
		path = this.resolvePath(path);
		return this.terminal.load(path);
	}
	
	/**
	 * propertiesを読み込む。
	 * @param path - 読み込み対象properties。
	 * @return 読み込み結果。
	 */
	public Properties loadProperties(String path) throws IOException {
		path = this.resolvePath(path);
		BufferedReader stream = null;
		Properties prop = new Properties();
		try {
			stream = new BufferedReader(new InputStreamReader(this.terminal.load(path), "UTF-8"));
			prop.load(stream);
			this.logger.fine("Load \"" + path + "\" properties.");
		} catch (Exception e) {
			this.logger.log(Level.WARNING, "「" + path + "」propertiesファイルの読み込みに失敗しました");
			throw e;
		} finally {
			if (stream != null) {
				try {
					stream.close();
				} catch (Exception e) {
					this.logger.log(Level.SEVERE, "propertiesのストリームclsoeに失敗しました");
				}
			}
		}
		return prop;
	}
	
	/**
	 * 指定ディレクトリ内の一覧を取得する。
	 * @param path - 一覧取得対象ディレクトリのパス。
	 * @return 指定ディレクトリ内の一覧。
	 */
	public String[] ls(String path) {
		path = this.resolvePath(path);
		return this.terminal.ls(path);
	}
	
	/**
	 * ファイルの存在を確認する。
	 * @param path - 確認対象のパス。
	 * @return 存在する場合はtrueを返す。
	 */
	public boolean exist(String path) {
		path = this.resolvePath(path);
		return this.terminal.exist(path);
	}
	/**
	 * 親パスを取得する。
	 * @param path - 取得対象。
	 * @return 引数pathの親パス。
	 */
	public String getParentPath(String path) {
		path = this.resolvePath(path);
		return this.terminal.getParentPath(path);
	}
	/**
	 * パスを連結する。
	 * @param parent - 連結先。
	 * @param child - 連結するパス。
	 * @returen 連結したパス。
	 */
	public String concatPath(String parent, String child) {
		return this.terminal.concatPath(parent, child);
	}
	
	/**
	 * パス解決。
	 * @param path - 解決対象パス。
	 */
	public String resolvePath(String path) {
		if (terminal.exist(path)) {
			return path;
		}
		if (current != null) {
			String path2 = terminal.concatPath(current, path);
			if (terminal.exist(path2)) {
				return path2;
			}
		}
		if (classpath != null) {
			for (String p : classpath) {
				String path2 = terminal.concatPath(p, path);
				if (terminal.exist(path2)) {
					return path2;
				}
				if (current != null) {
					path2 = terminal.concatPath(current, path2);
					if (terminal.exist(path2)) {
						return path2;
					}
				}
			}
		}
		URL url = OkuyamaUtil.class.getClassLoader().getResource(path);
		if (url == null) {
			return path;
		}
		try {
			File file = new File(url.toURI());
			return file.getAbsolutePath();
		} catch (URISyntaxException e) {
		}
		return path;
	}
}
