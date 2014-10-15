package test.helper.manager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * okuyama全体を管理するためのクラス。
 * @author s-ito
 *
 */
public class OkuyamaManager {

	private Logger logger = Logger.getLogger(OkuyamaManager.class.getName());
	/**
	 * 管理情報。
	 */
	private Properties managementInfo;
	
	/**
	 * 管理情報を読み込む。
	 * @param path - 管理情報のパス。
	 */
	public OkuyamaManager(File path) throws IOException {
		BufferedReader stream = null;
		try {
			this.managementInfo = new Properties();
			stream = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
			this.managementInfo.load(stream);
			this.logger.fine("Load \"" + path.getAbsolutePath() +  "\" Management Info.");
		} catch (IOException e) {
			throw e;
		} finally {
			if (stream != null) {
				stream.close();
			}
		}
	}
	
	/**
	 * 管理情報を設定する。
	 * @param info - 管理情報。
	 */
	public OkuyamaManager(Properties info) {
		this.managementInfo = info;
	}
	
	/**
	 * 管理情報を取得する。
	 * @return 管理情報。
	 */
	public Properties getManagementInfo() {
		return this.managementInfo;
	}
	
	/**
	 * 全OkuyamaMachineの名前を取得する。
	 * @return 全マシンの名前。
	 */
	public String[] getAllMachineName() {
		String machineListStr = this.managementInfo.getProperty("OkuyamaManager.machines");
		if (machineListStr == null || machineListStr.isEmpty()) {
			return new String[0];
		}
		String[] machines = machineListStr.split(",");
		for (int i = 0;i < machines.length;i++) {
			machines[i] = machines[i].trim();
		}
		return machines;
	}
	
	/**
	 * okuyamaのマシン管理用オブジェクトを取得する。
	 * @param name - 取得対象のマシン名。
	 * @return okuyamaのマシン管理用オブジェクト。
	 * @throws Exception 
	 */
	public OkuyamaMachine getOkuyamaMachine(String name) throws Exception {
		OkuyamaTerminalFactory factory = new OkuyamaTerminalFactory();
		return new OkuyamaMachine(factory.build(this.managementInfo, name), name, this.managementInfo);
	}
	
	/**
	 * okuyamaを停止させる。<br>
	 * MasterNode、MainMasterNode、DataNodeの順で停止させます。
	 * @return 全Nodeを停止できればtrueを返す。
	 * @throws Exception 
	 */
	public boolean shutdownOkuyama() throws Exception {
		// DataNode、MainMasterNode、MasterNodeに分ける
		ArrayList<OkuyamaProcess> dataNodes = new ArrayList<>();
		ArrayList<OkuyamaProcess> masterNodes = new ArrayList<>();
		OkuyamaProcess mainMaster = this.coordinateNodes(masterNodes, dataNodes);
		// MasterNodeを停止
		boolean judge = true;
		for (OkuyamaProcess process : masterNodes) {
			judge &= this.shutdownOkuyamaProcess(process);
		}
		// MainMasterNodeを停止
		judge &= this.shutdownOkuyamaProcess(mainMaster);
		// DataNodeを停止
		for (OkuyamaProcess process : dataNodes) {
			judge &= this.shutdownOkuyamaProcess(process);
		}
		return judge;
	}
	
	/**
	 * okuyamaを起動する。<br>
	 * 管理対象の全Nodeを別スレッドで起動します。
	 * @param output - 起動プロセスの標準出力の接続先。
	 * @param error - 起動プロセスの標準エラー出力の接続先。
	 * @throws Exception 
	 */
	public void bootOkuyama(OutputStream output, OutputStream error) throws Exception {
		// DataNode、MainMasterNode、MasterNodeに分ける
		ArrayList<OkuyamaProcess> dataNodes = new ArrayList<>();
		ArrayList<OkuyamaProcess> masterNodes = new ArrayList<>();
		OkuyamaProcess mainMaster = this.coordinateNodes(masterNodes, dataNodes);
		// DataNodeを起動
		for (OkuyamaProcess process : dataNodes) {
			this.bootOkuyamaProcess(process, output, error);
		}
		// MainMasterNodeを起動
		this.bootOkuyamaProcess(mainMaster, output, error);
		// MasterNodeを起動
		for (OkuyamaProcess process : masterNodes) {
			this.bootOkuyamaProcess(process, output, error);
		}
	}
	
	/**
	 * Nodeのプロセスを整理する。
	 * @param masterNodes - MasterNodeのプロセスの保存先。
	 * @param dataNodes - DataNodeのプロセスの保存先。
	 * @return MainMasterNodeのプロセスを返す。
	 * @throws Exception 
	 */
	private OkuyamaProcess coordinateNodes(ArrayList<OkuyamaProcess> masterNodes,
															ArrayList<OkuyamaProcess> dataNodes) throws Exception {
		OkuyamaProcess mainMaster = null;
		// 全プロセスを取得
		String[] machineNames = this.getAllMachineName();
		ArrayList<OkuyamaProcess> processList = new ArrayList<>();
		for (String machineName : machineNames) {
			OkuyamaMachine machine = this.getOkuyamaMachine(machineName);
			String[] processNames = machine.getAllProcessName();
			for (String processName : processNames) {
				processList.add(machine.getNodeProcess(processName));
			}
		}
		// DataNode、MainMasterNode、MasterNodeに分ける
		for (OkuyamaProcess process : processList) {
			OkuyamaNode[] nodes = process.getNodes();
			for (OkuyamaNode node : nodes) {
				if (node instanceof DataNode) {
					dataNodes.add(process);
					break;
				}
				if (node instanceof MasterNode) {
					if (((MasterNode)node).isMain()) {
						mainMaster = process;
					} else {
						masterNodes.add(process);
					}
				}
			}
		}
		return mainMaster;
	}
	
	/**
	 * okuyamaのプロセスを起動する。
	 * @param process - 起動対象プロセス。
	 * @param output - 起動プロセスの標準出力の接続先。
	 * @param error - 起動プロセスの標準エラー出力の接続先。
	 * @return 起動プロセス。
	 * @throws Exception 
	 */
	private TerminalProcess bootOkuyamaProcess(OkuyamaProcess process, OutputStream output, OutputStream error) throws Exception {
		String prefix = "OkuyamaProcess." + process.getPorcessName();
		int optionCount = Integer.valueOf(this.managementInfo.getProperty(prefix + ".jvmOptionCount", "0"));
		String[] jvmOptions = new String[optionCount];
		String optionPrefix = prefix + ".jvmOption.";
		for (int i = 0;i < optionCount;i++) {
			jvmOptions[i] = this.managementInfo.getProperty(optionPrefix + (i + 1));
		}
		optionCount = Integer.valueOf(this.managementInfo.getProperty(prefix + ".okuyamaOptionCount", "0"));
		String[] okuyamaOptions = new String[optionCount];
		optionPrefix = prefix + ".okuyamaOption.";
		for (int i = 0;i < optionCount;i++) {
			okuyamaOptions[i] = this.managementInfo.getProperty(optionPrefix + (i + 1));
		}
		TerminalProcess result = process.bootNode(jvmOptions, okuyamaOptions);
		OkuyamaUtil.redirectProcess(result, result.getStandardOutput(), output);
		OkuyamaUtil.redirectProcess(result, result.getStandardError(), error);
		// 起動確認
		OkuyamaNode[] nodes = process.getNodes();
		int waitingCount = nodes.length;
		for (int i = 0;i < 100;i++) {
			if (waitingCount <= 0) {
				return result;
			}
			Thread.sleep(100);
			for (int j = 0;j < nodes.length;j++) {
				if (nodes[j] != null) {
					if (nodes[j].ping()) {
						waitingCount--;
						nodes[j] = null;
					}
				}
			}
		}
		return result;
	}
	
	/**
	 * okuyamaのプロセスを停止する。
	 * @param process - 停止対象プロセス。
	 * @throws Exception 
	 */
	private boolean shutdownOkuyamaProcess(OkuyamaProcess process) throws Exception {
		boolean judge = process.shutdown();
		// 停止確認
		OkuyamaNode[] nodes = process.getNodes();
		int waitingCount = nodes.length;
		for (int i = 0;i < 100;i++) {
			if (waitingCount <= 0) {
				break;
			}
			Thread.sleep(100);
			for (int j = 0;j < nodes.length;j++) {
				if (nodes[j] != null) {
					if (!(nodes[j].ping())) {
						waitingCount--;
						nodes[j] = null;
					}
				}
			}
		}
		if (0 < waitingCount) {
			return false;
		}
		return judge;
	}
	

}
