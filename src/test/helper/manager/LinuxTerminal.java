package test.helper.manager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;

import okuyama.base.JavaMain;

public class LinuxTerminal extends LocalOkuyamaTerminal {

	private Logger logger = Logger.getLogger(WindowsTerminal.class.getName());
	/**
	 * JAVA本体のパス。
	 */
	private String javaPath;
	/**
	 * コンストラクタ。
	 * @param javaPath - JAVA本体のパス。
	 */
	public LinuxTerminal(String javaPath) {
		this.javaPath = javaPath;
	}

	@Override
	public TerminalProcess executeNode(String currentDir, String[] classpath, String mainProperties,
								String nodeProperties, String[] jvmOption, String[] okuyamaOption) throws IOException {
		// クラスパス作成
		StringBuilder classpathBuilder = new StringBuilder();
		for (String path : classpath) {
			classpathBuilder = classpathBuilder.append(new File(path).getAbsolutePath());
			classpathBuilder = classpathBuilder.append(":");
		}
		// プロセス実行準備
		ArrayList<String> command = new ArrayList<String>();
		command.add(this.javaPath);
		command.add("-cp");
		command.add(classpathBuilder.toString());
		for (String option : jvmOption) {
			command.add(option);
		}
		command.add(JavaMain.class.getName());
		command.add(mainProperties);
		command.add(nodeProperties);
		for (String option : okuyamaOption) {
			command.add(option);
		}
		// Node起動
		ProcessBuilder processBuilder = new ProcessBuilder(command);
		processBuilder.directory(new File(currentDir));
		this.logger.info("Execute 「" + command.toString() + "」");
		this.logger.info("CurrentDirectory 「" + currentDir + "」");
		Process process = processBuilder.start();
		if (process == null) {
			throw new IOException("Execute failure");
		}
		return new LocalTerminalProcess(process);
	}

}
