package test.helper.manager;

import java.util.Properties;
import java.util.logging.Level;
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
	 */
	public MasterNode(String nodeJobName, String nodeHostName,
						Properties nodeProperties, Properties mainProperties, Properties logProperties) {
		super(nodeJobName, nodeHostName, nodeProperties, mainProperties, logProperties);
		this.logger.config("OkuyamaNode          = Master");
	}

	@Override
	public boolean ping() {
		try {
			String resultStr = OkuyamaUtil.connectByTelnet(this.getNodeHostName(), this.getNodePort(), "999");
			if (resultStr == null) {
				return false;
			}
			this.logger.info("Ping Result : " + resultStr);
			return true;
		} catch (Exception e) {
			this.logger.log(Level.WARNING, this.getNodeHostName() + ":" + this.getNodePort() +  "へのPing失敗", e);
			return false;
		}
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
}
