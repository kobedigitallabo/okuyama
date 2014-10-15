package test.helper.manager;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Okuyamaを稼動させるマシン全体を管理するためのクラス。
 * @author s-ito
 *
 */
public class OkuyamaMachine {
	
	private Logger logger = Logger.getLogger(OkuyamaMachine.class.getName());
	/**
	 * okuyama操作用端末。
	 */
	private OkuyamaTerminal terminal;
	/**
	 * マシンのホスト名。
	 */
	private String hostName;
	/**
	 * Nodeプロパティリスト。<br>
	 * Key=プロセス名、Value=Nodeのpropertiesファイルのパス。
	 */
	private Map<String, String> nodeProperties = new HashMap<>();
	/**
	 * Main.propertiesリスト。<br>
	 * Key=プロセス名、Value=Main.propertiesファイルのパス。
	 */
	private Map<String, String> mainProperties = new HashMap<>();
	/**
	 * クラスパスリスト。<br>
	 * Key=プロセス名、Value=クラスパス。
	 */
	private Map<String, String[]> classpaths = new HashMap<>();
	/**
	 * カレントディレクトリリスト。
	 * Key=プロセス名、Value=カレントディレクトリパス。
	 */
	private Map<String, String> currentDirs = new HashMap<>();
	
	/**
	 * コンストラクタ。
	 * @param terminal - okuyama操作用端末。
	 * @param machineName - マシン名。
	 * @param config - マシンの構成情報。
	 */
	public OkuyamaMachine(OkuyamaTerminal terminal, String machineName, Properties config) {
		this.logger.config("OkuyamaMachine name = " + machineName);
		this.terminal = terminal;
		// 構成情報から情報読み出し
		String prefix = "OkuyamaMachine." + machineName;
		this.hostName = config.getProperty(prefix + ".host");
		String processNameListStr = config.getProperty(prefix + ".process", new String());
		String[] processNameList = processNameListStr.split(",");
		for (String processName : processNameList) {
			processName = processName.trim();
			String prefix2 = "OkuyamaProcess." + processName;
			String node = config.getProperty(prefix2 + ".node");
			String main = config.getProperty(prefix2 + ".main");
			String classpathStr = config.getProperty(prefix2 + ".classpath");
			String[] classpath = classpathStr.split(";");
			String currentDir = config.getProperty(prefix2 + ".current");
			this.nodeProperties.put(processName, node);
			this.mainProperties.put(processName, main);
			this.classpaths.put(processName, classpath);
			this.currentDirs.put(processName, currentDir);
		}
	}
	
	/**
	 * Nodeのプロパティを追加する。
	 * @param name - プロセス名。
	 * @param path - Nodeのpropertiesのパス。
	 */
	public void addNodeProperties(String name, String path) {
		this.nodeProperties.put(name, path);
		this.logger.fine("Add \"" + path + "\" NodeProperties for \"" + name + "\".");
	}
	
	/**
	 * Nodeのプロパティを削除する。
	 * @param name - 削除対象のプロセス名。
	 */
	public void deleteNodeProperties(String name) {
		String path = this.nodeProperties.remove(name);
		this.logger.fine("Delete \"" + path + "\" NodeProperties for \"" + name + "\".");
	}
	
	/**
	 * Nodeのプロセスを取得する。
	 * @param name - 取得対象のプロセス名。
	 * @return Nodeのプロセス管理オブジェクト。
	 */
	public OkuyamaProcess getNodeProcess(String name) {
		String node = this.nodeProperties.get(name);
		if (node == null) {
			return null;
		}
		String main = this.mainProperties.get(name);
		String[] classpath = this.classpaths.get(name);
		String currentDir = this.currentDirs.get(name);
		return new OkuyamaProcess(name, currentDir, node, main, classpath, this.terminal, this.hostName);
	}
	
	/**
	 * 指定NodeのプロセスにMain.propertiesを設定する。
	 * @param name - 設定対象のプロセス名。
	 * @param path - 設定するMain.propertiesのパス。
	 */
	public void setMainProperties(String name, String path) {
		this.mainProperties.put(name, path);
		this.logger.fine("Add \"" + path + "\" MainProperties for \"" + name + "\".");
	}
	
	/**
	 * マシン内の全NodeプロセスにMain.propertiesを設定する。
	 * @param path - 設定するMain.propertiesのパス。
	 */
	public void setMainProperties(String path) {
		for (String name : this.getAllProcessName()) {
			this.setMainProperties(name, path);
		}
	}
	
	/**
	 * 指定Nodeのプロセスにクラスパスを設定する。
	 * @param name - 設定対象のプロセス名。
	 * @param classpath - 設定するクラスパス。
	 */
	public void setClasspath(String name, String[] classpath) {
		this.classpaths.put(name, classpath);
		this.logger.fine("Add \"" + classpath + "\" classpath for \"" + name + "\".");
	}
	
	/**
	 * マシン内の全NodeプロセスにMain.propertiesを設定する。
	 * @param classpath - 設定するクラスパス。
	 */
	public void setClasspath(String[] classpath) {
		for (String name : this.getAllProcessName()) {
			this.setClasspath(name, classpath);
		}
	}
	
	/**
	 * 指定Nodeのプロセスにカレントディレクトリを設定する。
	 * @param name - 設定対象のプロセス名。
	 * @param classpath - 設定するクラスパス。
	 */
	public void setCurrentDir(String name, String currentDir) {
		this.currentDirs.put(name, currentDir);
		this.logger.fine("Add \"" + currentDir + "\" current directory for \"" + name + "\".");
	}
	
	/**
	 * マシン内の全NodeプロセスにMain.propertiesを設定する。
	 * @param classpath - 設定するクラスパス。
	 */
	public void setCurrentDir(String currentDir) {
		for (String name : this.getAllProcessName()) {
			this.setCurrentDir(name, currentDir);
		}
	}
	
	/**
	 * 全プロセスの名前を取得する。
	 * @return マシン内の全プロセスの名前。
	 */
	public String[] getAllProcessName() {
		Set<String> keys = this.nodeProperties.keySet();
		return keys.toArray(new String[keys.size()]);
	}
	
	/**
	 * このマシンが使用するOkuyamaTerminalを取得する。
	 * @return マシンが使用するOkuyamaTerminal。
	 */
	public OkuyamaTerminal getOkuyamaTerminal() {
		return this.terminal;
	}
}
