package test.helper.manager;

import java.util.Properties;
import java.util.logging.Logger;

/**
 * MasterNodeを管理するためのクラス。
 * @author s-ito
 *
 */
public class MasterNode extends OkuyamaNode {

	private Logger logger = Logger.getLogger(MasterNode.class.getName());

	/**
	 * コンストラクタ。
	 * @param nodeJobName - NodeのJob名。
	 * @param nodeHostName - Nodeのホスト名。
	 * @param nodeProperties - NodeのProperties。
	 * @param mainProperties - Main.properties。
	 * @param logProperties - log4j.properties。
	 * @param resource - リソース操作用オブジェクト。
	 */
	public MasterNode(String nodeJobName, String nodeHostName, Properties nodeProperties,
								Properties mainProperties, Properties logProperties, OkuyamaNodeResource resource) {
		super(nodeJobName, nodeHostName, nodeProperties, mainProperties, logProperties, resource);
		this.logger.config("OkuyamaNode          = Master");
	}

	@Override
	public boolean ping() {
		String resultStr = null;
		try {
			resultStr = this.resource.getTerminal().connectNodeByTelnet(this.getNodeHostName(),
																		this.getNodePort(), "999");
		} catch (Exception e) {
		}
		if (resultStr == null) {
			return false;
		}
		this.logger.info("Ping Result : " + resultStr);
		return true;
	}

	/**
	 * MainMasterNodeであるか確認する。
	 * @return MainMasterNodeであればtrueを返す。
	 */
	public boolean isMain() {
		Properties prop = this.getNodeProperties();
		String mainNodeName = prop.getProperty("MainMasterNodeInfo");
		return OkuyamaUtil.equalsNodeNmae(mainNodeName, this.getNodeHostName() + ":" + this.getNodePort());
	}

	@Override
	public NodeConfig getNodeConfig() {
		return new MasterNodeConfig(this.getNodeProperties(), this.getNodeJobName());
	}
}
