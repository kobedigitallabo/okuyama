package test.helper.manager;

import java.util.ArrayList;
import java.util.Properties;

import okuyama.imdst.helper.ServerControllerHelper;

/**
 * Nodeのpropertiesの管理クラス。
 * @author s-ito
 *
 */
public class NodeConfig {

	/**
	 * Nodeのproperties本体。
	 */
	protected Properties prop;
	/**
	 * Job名。
	 */
	private String jobName;
	
	/**
	 * コンストラクタ。
	 * @param prop - Nodeのproperties本体。
	 * @param jobName - NodeのJob名。
	 */
	public NodeConfig(Properties prop, String jobName) {
		this.prop = prop;
		this.jobName = jobName;
	}
	
	/**
	 * NodeのJob名を取得する。
	 * @return NodeのJob名。
	 */
	public String getNodeJobName() {
		return this.jobName;
	}
	
	/**
	 * Nodeのpropertiesファイルのバリデーションをする。<br>
	 * joblistとhelperリストと管理ポートのチェックをする。
	 */
	public void validate() throws NodeConfigValidationException {
		// joblistのチェック
		String joblistStr = prop.getProperty("joblist");
		if (joblistStr == null || joblistStr.isEmpty()) {
			throw new NodeConfigValidationException("joblist", "joblistが設定されていません");
		}
		// joblistで指定されたjobにクラスが指定されているか確認
		String[] joblist = OkuyamaUtil.splitList(joblistStr);
		for (String job : joblist) {
			String className = prop.getProperty(job + ".JobClass");
			if (className == null || className.isEmpty()) {
				throw new NodeConfigValidationException(job + ".JobClass", "クラスが設定されていません");
			}
		}
		// helperlistのチェック
		String helperlistStr = prop.getProperty("helperlist");
		if (helperlistStr == null || helperlistStr.isEmpty()) {
			throw new NodeConfigValidationException("helperlist", "helperlistが設定されていません");
		}
		// helperlistで指定されたhelperにクラスが指定されているか確認
		String[] helperlist = OkuyamaUtil.splitList(helperlistStr);
		for (String helper : helperlist) {
			String className = prop.getProperty(helper + ".HelperClass");
			if (className == null || className.isEmpty()) {
				throw new NodeConfigValidationException(helper + ".HelperClass", "クラスが設定されていません");
			}
		}
		// 管理ポートが設定されているか確認する
		String[] managementPortHelper = NodeConfig.getHelper(ServerControllerHelper.class.getName(), this.prop);
		if (managementPortHelper.length != 1) {
			throw new NodeConfigValidationException("helperlist",
					ServerControllerHelper.class.getName() + "をHelperClassに指定したhelperを1つだけ作成してください");
		}
		String managementPortStr = prop.getProperty(managementPortHelper[0] + ".Init");
		if (managementPortStr == null || managementPortStr.isEmpty()) {
			throw new NodeConfigValidationException(managementPortHelper[0] + ".Init", "管理ポートが設定されていません");
		}
		try {
			Integer.valueOf(managementPortStr);
		} catch (Exception e) {
			throw new NodeConfigValidationException(managementPortHelper[0] + ".Init", e);
		}
	}
	
	/**
	 * classNameで指定されたクラスを指定されたjobを取得する。
	 * @param className - 指定クラス名。
	 * @param prop - NodeのProperties本体。
	 * @return classNameで指定されたクラスを指定されたjobの名前のリスト。
	 * @throws NodeConfigValidationException 
	 */
	public static String[] getJob(String className, Properties prop) throws NodeConfigValidationException {
		String listStr = prop.getProperty("joblist");
		if (listStr == null || listStr.isEmpty()) {
			throw new NodeConfigValidationException("joblist", "joblistが設定されていません");
		}
		String[] list = OkuyamaUtil.splitList(listStr);
		ArrayList<String> result = new ArrayList<String>();
		for (String name : list) {
			String className2 = prop.getProperty(name + ".JobClass");
			if (className == null || className.isEmpty()) {
				throw new NodeConfigValidationException(name + ".JobClass", "クラスが設定されていません");
			}
			if (className2.equals(className)) {
				result.add(name);
			}
		}
		return result.toArray(new String[result.size()]);
	}
	
	/**
	 * classNameで指定されたクラスを指定されたhelperを取得する。
	 * @param className - 指定クラス名。
	 * @param prop - NodeのProperties本体。
	 * @return classNameで指定されたクラスを指定されたhelperの名前のリスト。
	 * @throws NodeConfigValidationException 
	 */
	public static String[] getHelper(String className, Properties prop) throws NodeConfigValidationException {
		String listStr = prop.getProperty("helperlist");
		if (listStr == null || listStr.isEmpty()) {
			throw new NodeConfigValidationException("helperlist", "helperlistが設定されていません");
		}
		String[] list = OkuyamaUtil.splitList(listStr);
		ArrayList<String> result = new ArrayList<String>();
		for (String name : list) {
			String className2 = prop.getProperty(name + ".HelperClass");
			if (className == null || className.isEmpty()) {
				throw new NodeConfigValidationException(name + ".HelperClass", "クラスが設定されていません");
			}
			if (className2.equals(className)) {
				result.add(name);
			}
		}
		return result.toArray(new String[result.size()]);
	}
}
