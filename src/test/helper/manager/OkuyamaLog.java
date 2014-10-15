package test.helper.manager;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

/**
 * Nodeのログ管理用クラス。
 * @author s-ito
 *
 */
public class OkuyamaLog {

	/**
	 * ログの設定情報。
	 */
	private Properties log4j;
	/**
	 * Okuyamaリソース操作用。
	 */
	private OkuyamaNodeResource resource;
	
	/**
	 * コンストラクタ。
	 * @param log4j - ログの設定情報のパス。
	 * @param resource - okuyamaリソース操作用。
	 */
	public OkuyamaLog(Properties log4j, OkuyamaNodeResource resource) {
		this.log4j = log4j;
		this.resource = resource;
	}
	
	/**
	 * ログを削除する。
	 */
	public void deleteLog() {
		// log4j内のファイル関連設定を全て取得
		Set<Object> keySet = this.log4j.keySet();
		ArrayList<String> files = new ArrayList<>();
		for (Object key : keySet) {
			files.add(this.log4j.getProperty((String) key));
		}
		// ファイルを削除
		for (String file : files) {
			if (!(this.resource.exist(file))) {
				continue;
			}
			String parent = this.resource.getParentPath(file);
			this.resource.delete(file);
			// ファイルを削除したことによりディレクトリが空になれば削除
			String[] list = this.resource.ls(parent);
			if (list == null || list.length <= 0) {
				this.resource.delete(parent);
			}
		}
	}
}
