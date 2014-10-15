package test.helper.manager;

import java.io.InputStream;
import java.io.OutputStream;

public class LocalTerminalProcess implements TerminalProcess {

	/**
	 * プロセス本体。
	 */
	private Process process;
	
	/**
	 * コンストラクタ。
	 * @param process - プロセス本体。
	 */
	public LocalTerminalProcess(Process process) {
		this.process = process;
	}
	
	@Override
	public InputStream getStandardOutput() {
		return this.process.getInputStream();
	}

	@Override
	public InputStream getStandardError() {
		return this.process.getErrorStream();
	}

	@Override
	public OutputStream getStandardInput() {
		return this.process.getOutputStream();
	}

	@Override
	public boolean isEnd() {
		try {
			this.process.exitValue();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

}
