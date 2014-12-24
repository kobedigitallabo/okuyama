package test.helper.manager;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * okuyamaのNodeを管理するクラス。
 * @author s-ito
 *
 */
public abstract class OkuyamaNode {

	private Logger logger = Logger.getLogger(OkuyamaNode.class.getName());
	/**
	 * NodeのProperties。
	 */
	private Properties nodeProperties;
	/**
	 * MainのProperties。
	 */
	private Properties mainProperties;
	/**
	 * LogのProperties。
	 */
	private Properties logProperties;
	/**
	 * NodeのJob名。
	 */
	private String nodeJobName;
	/**
	 * Nodeのホスト名。
	 */
	private String nodeHostName;
	/**
	 * リソース操作用オブジェクト。
	 */
	protected OkuyamaNodeResource resource;

	/**
	 * コンストラクタ。
	 * @param nodeJobName - NodeのJob名。
	 * @param nodeHostName - Nodeのホスト名。
	 * @param nodeProperties - NodeのProperties。
	 * @param mainProperties - Main.properties。
	 * @param logProperties - log4j.properties。
	 * @param resource - リソース操作用オブジェクト。
	 */
	public OkuyamaNode(String nodeJobName, String nodeHostName, Properties nodeProperties,
						Properties mainProperties, Properties logProperties, OkuyamaNodeResource resource) {
		this.logger.config("OkuyamaNode JobName  = " + nodeJobName);
		this.logger.config("OkuyamaNode HostName = " + nodeHostName);
		this.nodeJobName = nodeJobName;
		this.nodeHostName = nodeHostName;
		this.nodeProperties = nodeProperties;
		this.mainProperties = mainProperties;
		this.logProperties = logProperties;
		this.resource = resource;
	}

	/**
	 * Nodeの疎通確認を行う。
	 * @return Nodeに正常にアクセスできればtrueを返す。
	 */
	public abstract boolean ping();

	/**
	 * Nodeのポート番号を取得する。
	 * @return Nodeのポート番号。
	 */
	public int getNodePort() {
		String portStr = this.nodeProperties.getProperty(this.nodeJobName + ".Init");
		try {
			return Integer.valueOf(portStr);
		} catch (Exception e) {
			this.logger.log(Level.SEVERE, "ポート番号が不正です", e);
			return -1;
		}
	}

	/**
	 * Nodeの設定情報を取得する。
	 * @return Nodeの設定情報。
	 */
	public abstract NodeConfig getNodeConfig();

	/**
	 * Nodeの管理ポート番号を取得する。
	 * @return Nodeの管理ポート番号。
	 */
	public int getManagementPort() {
		try {
			// 管理ポートを設定している項目を探す。
			String helperListStr = this.nodeProperties.getProperty("helperlist");
			String[] helperList = helperListStr.split(",");
			String portKey = null;
			for (String helper : helperList) {
				String helperClass = this.nodeProperties.getProperty(helper + ".HelperClass");
				if ("okuyama.imdst.helper.ServerControllerHelper".equals(helperClass)) {
					portKey = helper + ".Init";
				}
			}
			// 管理ポート取得
			String portStr = this.nodeProperties.getProperty(portKey);
			return Integer.valueOf(portStr);
		} catch (Exception e) {
			this.logger.log(Level.SEVERE, "管理ポート番号が不正です", e);
			return -1;
		}
	}

	/**
	 * Nodeのpropertiesを取得する。
	 * @return Nodeのproperties。
	 */
	public Properties getNodeProperties() {
		return this.nodeProperties;
	}

	/**
	 * Nodeが使用するMain.proeprtiesを取得する。
	 * @return Nodeが使用するMain.properties。
	 */
	public Properties getMainProeprties() {
		return this.mainProperties;
	}

	/**
	 * Nodeのホスト名を取得する。
	 * @return Nodeのホスト名。
	 */
	public String getNodeHostName() {
		return this.nodeHostName;
	}

	/**
	 * NodeのJob名を取得する。
	 * @return NodeのJob名。
	 */
	public String getNodeJobName() {
		return this.nodeJobName;
	}

	/**
	 * Nodeが使用するlog4j.propertiesを取得する。
	 * @return Nodeが使用するlog4j.properties。
	 */
	protected Properties getLogProperties() {
		return this.logProperties;
	}
}
