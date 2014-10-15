package test.helper.manager;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

/**
 * テストでokuyamaを操作するためのクラス。
 * 
 * @author s-ito
 *
 */
public class OkuyamaHelper {

	/**
	 * ant serverrunと同じ構成のOkuyamaManagerを取得する。
	 * @return ant serverrunと同じ構成のOkuyamaManagerオブジェクト。
	 * @throws IOException 
	 * @throws URISyntaxException 
	 */
	public OkuyamaManager getServerrun() throws Exception {
		Reader reader = null;
		Properties prop = null;
		try {
			prop = new Properties();
			URL url = getClass().getClassLoader().getResource("test/serverrun.properties");
			reader = new InputStreamReader(new FileInputStream(new File(url.toURI())), "UTF-8");
			prop.load(reader);
		} catch (Exception e) {
			throw e;
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
		String classpath = System.getProperty("java.class.path");
		if (!(System.getProperty("os.name").startsWith("Windows"))) {
			classpath = classpath.replaceAll(":", ";");
		}
		prop.setProperty("OkuyamaProcess.DataNode.classpath", classpath);
		prop.setProperty("OkuyamaProcess.MasterNode.classpath", classpath);
		// keymapfile作成
		new File("./keymapfile").mkdir();
		return new OkuyamaManager(prop);
	}
	
	/**
	 * okuyamaのkeymapfileとlogsを掃除する。
	 * @param okuyama - 掃除対象のokuyama。
	 */
	public void cleanOkuyama(OkuyamaManager okuyama) throws Exception {
		for (String machineName : okuyama.getAllMachineName()) {
			OkuyamaMachine machine = okuyama.getOkuyamaMachine(machineName);
			for (String processName : machine.getAllProcessName()) {
				OkuyamaProcess process = machine.getNodeProcess(processName);
				for (OkuyamaNode node : process.getNodes()) {
					if (node instanceof DataNode) {
						((DataNode)node).deleteKeymapfile(process.getOkuyamaNodeResource());
					}
				}
				OkuyamaLog log = process.getLog();
				log.deleteLog();
			}
		}
	}
	
	/**
	 * okuyama全体のpingを行う。
	 * @param okuyama - チェック対象。
	 * @return 1つでも停止したNodeがあればfalseを返す。
	 * @throws Exception 
	 */
	public boolean pingOkuyama(OkuyamaManager okuyama) throws Exception {
		for (String machineName : okuyama.getAllMachineName()) {
			OkuyamaMachine machine = okuyama.getOkuyamaMachine(machineName);
			for (String processName : machine.getAllProcessName()) {
				OkuyamaProcess process = machine.getNodeProcess(processName);
				for (OkuyamaNode node : process.getNodes()) {
					if (!(node.ping())) {
						return false;
					}
				}
			}
		}
		return true;
	}
}
