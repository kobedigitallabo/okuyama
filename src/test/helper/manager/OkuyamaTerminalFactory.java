package test.helper.manager;

import java.util.Properties;

/**
 * OkuyamaTerminal作成用クラス。
 * @author s-ito
 *
 */
public class OkuyamaTerminalFactory {

	/**
	 * OkuyamaTerminalを作成する。
	 * @param info - 作成情報。
	 * @param machineName - マシン名。
	 * @return 指定マシンで使うOkuyamaTerminal。
	 * @throws Exception 
	 */
	public OkuyamaTerminal build(Properties info, String machineName) throws Exception {
		String prefix = "OkuyamaMachine." + machineName + ".terminal";
		String terminalName = info.getProperty(prefix + ".name");
		switch (terminalName) {
		case "Windows":
			String javaPath = info.getProperty(prefix + ".java");
			return new WindowsTerminal(javaPath);
		default:
			throw new Exception("Not support terminal " + terminalName);
		}
	}
}
