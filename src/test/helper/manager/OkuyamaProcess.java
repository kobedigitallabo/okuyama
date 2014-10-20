package test.helper.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import okuyama.base.JavaMain;

/**
 * OkuyamaのNodeプロセスを管理するためのクラス。
 * @author s-ito
 *
 */
public class OkuyamaProcess {

	private Logger logger = Logger.getLogger(OkuyamaProcess.class.getName());
	/**
	 * Nodeのpropertiesのパス。
	 */
	private String nodeProperties;
	/**
	 * Main.propertiesのパス。
	 */
	private String mainProperties;
	/**
	 * 起動時のクラスパス。
	 */
	private String[] classpath;
	/**
	 * 操作用端末。
	 */
	private OkuyamaTerminal terminal;
	/**
	 * ホスト名。
	 */
	private String nodeHostName;
	/**
	 * プロセス名。
	 */
	private String processName;
	/**
	 * カレントディレクトリ。
	 */
	private String currentDir;
	/**
	 * Nodeのリソース管理用クラス。
	 */
	private OkuyamaNodeResource nodeResource;
	
	/**
	 * コンストラクタ。
	 * @param processName - プロセス名。
	 * @param currentDir - 実行時のカレントディレクトリ。
	 * @param nodeProperties - Nodeのpropertiesのパス。
	 * @param mainProperties - Main.propertiesのパス。
	 * @param classpath - 起動時のクラスパス。
	 * @param terminal - 操作用端末。
	 * @param nodeHostName - ホスト名。
	 */
	public OkuyamaProcess(String processName, String currentDir, String nodeProperties,
							String mainProperties, String[] classpath, OkuyamaTerminal terminal, String nodeHostName) {
		this.logger.config("OkuyamaProcess name           = " + processName);
		this.logger.config("OkuyamaProcess host           = " + nodeHostName);
		this.logger.config("OkuyamaProcess currentDir     = " + currentDir);
		this.logger.config("OkuyamaProcess nodeProperties = " + nodeProperties);
		this.logger.config("OkuyamaProcess mainProperties = " + mainProperties);
		this.logger.config("OkuyamaProcess classpath      = " + classpath);
		this.processName = processName;
		this.nodeProperties = nodeProperties;
		this.mainProperties = mainProperties;
		this.classpath = classpath;
		this.terminal = terminal;
		this.nodeHostName = nodeHostName;
		this.currentDir = currentDir;
		this.nodeResource = new OkuyamaNodeResource(this.terminal, this.currentDir, this.classpath);
	}
	
	/**
	 * Nodeを起動する。<br>
	 * このメソッドではNodeを別プロセスとして起動します。
	 * @param jvmOption - JVMの起動オプション。起動オプションを使わない場合は要素数0の配列を渡す。
	 * @param okuyamaOption - okuyamaの起動引数。起動引数を使わない場合は要素数0の配列を渡す。
	 * @return 実行されたプロセス。
	 */
	public TerminalProcess bootNode(String[] jvmOption, String[] okuyamaOption) throws Exception {
		if (this.classpath == null) {
			throw new Exception("クラスパスが存在しないので別プロセスとして実行できません");
		}
		TerminalProcess process = this.terminal.executeNode(this.currentDir, this.classpath,
											this.mainProperties, this.nodeProperties, jvmOption, okuyamaOption);
		this.logger.info("boot \"" + this.processName + "\" process.");
		return process;
	}
	
	/**
	 * Nodeを起動する。<br>
	 * このメソッドではNodeを同じプロセスとして起動します。
	 */
	public void bootNode() throws Exception {
		JavaMain.main(new String[]{this.mainProperties, this.nodeProperties});
		this.logger.info("boot \"" + this.processName + "\" JavaMain class.");
	}
	
	/**
	 * Nodeのプロセスを停止させる。
	 * @return 停止コマンドの発行に成功すればtrueを返す。
	 */
	public boolean shutdown() throws Exception {
		OkuyamaNode[] nodes = this.getNodes();
		if (nodes.length <= 0) {
			return false;
		}
		try {
			String resultStr = OkuyamaUtil.connectByTelnet(nodes[0].getNodeHostName(),
															nodes[0].getManagementPort(), "shutdown");
			if (resultStr == null) {
				return false;
			}
			this.logger.info("Shutdown Result : " + resultStr);
			return true;
		} catch (Exception e) {
			this.logger.log(Level.WARNING, this.getPorcessName() +  "のシャットダウン失敗", e);
			return false;
		}
	}
	
	/**
	 * プロセス名を取得する。
	 * @return プロセス名。
	 */
	public String getPorcessName() {
		return this.processName;
	}
	
	/**
	 * このプロセスのNodeのリソース操作用オブジェクトを取得する。
	 * @return このプロセスのNodeのリソース操作用オブジェクト。
	 */
	public OkuyamaNodeResource getOkuyamaNodeResource() {
		return this.nodeResource;
	}
	
	/**
	 * このプロセスのログを取得する。
	 * @return プロセスのログ。
	 * @throws IOException 
	 */
	public OkuyamaLog getLog() throws IOException {
		// 使用するpropertiesを読み込む
		Properties main = this.nodeResource.loadProperties(this.mainProperties);
		// logのpropertiesを読み込む
		Properties log = new Properties();
		String logPath = main.getProperty("loggerproperties", null);
		try {
			if (logPath == null || logPath.isEmpty()) {
				log = this.nodeResource.loadProperties("log4j.properties");
			} else {
				log = this.nodeResource.loadProperties(logPath);
			}
		} catch (Exception e) {
		}
		return new OkuyamaLog(log, this.nodeResource);
	}
	
	/**
	 * プロセス内のNodeを全て取得する。
	 * @return プロセス内のNode。
	 */
	public OkuyamaNode[] getNodes() throws Exception {
		// 使用するpropertiesを読み込む
		Properties main = this.nodeResource.loadProperties(this.mainProperties);
		Properties node = this.nodeResource.loadProperties(this.nodeProperties);
		// logのpropertiesを読み込む
		Properties log = new Properties();
		String logPath = main.getProperty("loggerproperties", null);
		try {
			if (logPath == null || logPath.isEmpty()) {
				log = this.nodeResource.loadProperties("log4j.properties");
			} else {
				log = this.nodeResource.loadProperties(logPath);
			}
		} catch (Exception e) {
		}
		// OkuyamaNodeを作成する
		return this.createOkuyamaNode(main, node, log);
	}
	
	/**
	 * OkuyamaNodeオブジェクトを作成する。
	 * @param main - Main.properties。
	 * @param node - Nodeのproperties。
	 * @param log - ログのproperties。
	 * @return 作成されたOkuyamaNodeオブジェクト。
	 */
	private OkuyamaNode[] createOkuyamaNode(Properties main, Properties node, Properties log) {
		// Jobリストを取得
		String jobListStr = node.getProperty("joblist");
		String[] jobList = jobListStr.split(",");
		// Jobの中からNodeのJobを取得する
		ArrayList<OkuyamaNode> nodes = new ArrayList<>();
		for (String job : jobList) {
			String jobClass = node.getProperty(job + ".JobClass");
			if ("okuyama.imdst.job.KeyManagerJob".equals(jobClass)) {
				nodes.add(new DataNode(job, this.nodeHostName, node, main, log, this.nodeResource));
			} else if ("okuyama.imdst.job.MasterManagerJob".equals(jobClass)) {
				nodes.add(new MasterNode(job, this.nodeHostName, node, main, log, this.nodeResource));
			}
		}
		return nodes.toArray(new OkuyamaNode[nodes.size()]);
	}
}
