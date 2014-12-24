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
		String javaPath = null;
		if ("Windows".equals(terminalName)) {
			 javaPath = info.getProperty(prefix + ".java");
			return new WindowsTerminal(javaPath);
		} else if ("Linux".equals(terminalName)) {
			 javaPath = info.getProperty(prefix + ".java");
			return new LinuxTerminal(javaPath);
		} else {
			throw new Exception("Not support terminal " + terminalName);
		}
	}
}
