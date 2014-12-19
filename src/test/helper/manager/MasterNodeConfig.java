package test.helper.manager;

import java.util.Properties;

/**
 * MasterNodeのpropertiesの管理クラス。
 * @author s-ito
 *
 */
public class MasterNodeConfig extends NodeConfig {

	/**
	 * コンストラクタ。
	 * @param prop - Nodeのproperties本体。
	 * @param jobName - NodeのJob名。
	 */
	public MasterNodeConfig(Properties prop, String jobName) {
		super(prop, jobName);
	}
	
	/**
	 * このMasterNodeに割り当てられたパーティション名を取得する。
	 * @return このMasterNodeに割り当てられたパーティション名。パーティション機能を使用しない場合はnullを返す。
	 */
	public String getPartitionName() {
		Properties prop = this.prop;
		if (!("true".equals(prop.getProperty("IsolationMode")))) {
			return null;
		}
		String name = prop.getProperty("IsolationPrefix");
		if (name == null || name.isEmpty()) {
			return null;
		}
		return name;
	}
	
	/**
	 * MainMasterNodeの識別名を取得する。
	 * @return MainMasterNodeの識別名。
	 */
	public String getMainMasterNode() {
		Properties prop = this.prop;
		String name = prop.getProperty("MainMasterNodeInfo");
		if (name == null || name.isEmpty()) {
			return null;
		}
		return name;
	}
	
	/**
	 * 全MasterNodeの識別名を取得する。
	 * @return 全MasterNodeの識別名。
	 */
	public String[] getAllMasterNodes() {
		Properties prop = this.prop;
		String name = prop.getProperty("MainMasterNodeInfo");
		if (name == null || name.isEmpty()) {
			return null;
		}
		String[] nameList = name.split(",");
		for (int i = 0;i < nameList.length;i++) {
			nameList[i] = nameList[i].trim();
		}
		return nameList;
	}
	
	/**
	 * 全DataNodeの識別名を取得する。
	 * @return 全DataNodeの識別名。<br>
	 * String[0～n-1][0]: MainDataNode<br>
	 * String[0～n-1][1]: SubDataNode<br>
	 * String[0～n-1][2]: ThirdDataNode<br>
	 * (nは冗長化セットの数)
	 */
	public String[][] getAllDataNodes() {
		Properties prop = this.prop;
		String nodeCountStr = prop.getProperty("KeyMapNodesRule");
		String mainListStr = prop.getProperty("KeyMapNodesInfo");
		String subListStr = prop.getProperty("SubKeyMapNodesInfo");
		String thirdListStr = prop.getProperty("ThirdKeyMapNodesInfo");
		String[][] nodeList = new String[3][];
		int nodeCount = 0;
		if (!(nodeCountStr == null || mainListStr.isEmpty())) {
			try {
				nodeCount = Integer.valueOf(nodeCountStr);
			} catch (Exception e) {
			}
		}
		if (nodeCount == 0) {
			return new String[0][];
		}
		int count = 0;
		if (mainListStr == null || mainListStr.isEmpty()) {
			nodeList[count] = new String[0];
		} else {
			nodeList[count] = mainListStr.split(",");
			count++;
			if (subListStr == null || subListStr.isEmpty()) {
				nodeList[count] = new String[0];
			} else {
				nodeList[count] = subListStr.split(",");
				count++;
				if (thirdListStr == null || thirdListStr.isEmpty()) {
					nodeList[count] = new String[0];
				} else {
					nodeList[count] = thirdListStr.split(",");
					count++;
				}
			}
		}
		String[][] list = new String[nodeCount][count];
		for (int i = 0;i < nodeCount;i++) {
			for (int j = 0;j < count;j++) {
				list[i][j] = nodeList[j][i];
			}
		}
		return list;
	}

}
