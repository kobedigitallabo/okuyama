package test.helper.manager;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DataNodeを管理するためのクラス。
 * @author s-ito
 *
 */
 public class DataNode extends OkuyamaNode {

	private Logger logger = Logger.getLogger(DataNode.class.getName());
	
	/**
	 * コンストラクタ。
	 * @param nodeJobName - NodeのJob名。
	 * @param nodeHostName - Nodeのホスト名。
	 * @param nodeProperties - NodeのProperties。
	 * @param mainProperties - Main.properties。
	 * @param logProperties - log4j.properties。
	 * @param resource - リソース操作用オブジェクト。
	 */
	public DataNode(String nodeJobName, String nodeHostName, Properties nodeProperties,
								Properties mainProperties, Properties logProperties, OkuyamaNodeResource resource) {
		super(nodeJobName, nodeHostName, nodeProperties, mainProperties, logProperties, resource);
		this.logger.config("OkuyamaNode          = Data");
	}

	@Override
	public boolean ping() {
		try {
			String resultStr = this.resource.getTerminal().connectNodeByTelnet(this.getNodeHostName(), this.getNodePort(), "10");
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
	 * keymapfileを削除する。
	 */
	public void deleteKeymapfile() {
		String filesStr = this.getNodeProperties().getProperty(this.getNodeJobName() + ".Option");
		String[] files = filesStr.split(",");
		for (String file : files) {
			resource.delete(file);
		}
		resource.delete(files[0] + ".obj");
	}

	@Override
	public NodeConfig getNodeConfig() {
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}

}
