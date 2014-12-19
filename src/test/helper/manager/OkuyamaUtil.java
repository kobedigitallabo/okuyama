package test.helper.manager;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.net.telnet.TelnetClient;

/**
 * Okuyama管理で使用するUtiltiesクラス。
 * @author s-ito
 *
 */
public class OkuyamaUtil {

	private OkuyamaUtil() {}
	
	/**
	 * Node名を比較する。
	 * @param nodeName1 - 比較対象1。
	 * @param nodeName2 - 比較対象2。
	 * @return 比較対象が等しければtrueを返す。
	 */
	public static boolean equalsNodeNmae(String nodeName1, String nodeName2) {
		String[] hostAndPort1 = nodeName1.split(":");
		String[] hostAndPort2 = nodeName2.split(":");
		if (hostAndPort1.length != hostAndPort2.length) {
			return false;
		}
		// ホスト名を全てIPアドレスに変換
		try {
			InetAddress host = InetAddress.getByName(hostAndPort1[0]);
			hostAndPort1[0] = host.getHostAddress();
		} catch (Exception e) {
		}
		try {
			InetAddress host = InetAddress.getByName(hostAndPort2[0]);
			hostAndPort2[0] = host.getHostAddress();
		} catch (Exception e) {
		}
		if (hostAndPort1[0].equals(hostAndPort2[0])) {
			if (2 <= hostAndPort1.length) {
				if (hostAndPort1[1].equals(hostAndPort2[1])) {
					return true;
				}
				return false;
			}
			return true;
		}
		return false;
	}
	
	/**
	 * telnetでNodeと通信する。
	 * @param host - 接続先ホスト名。
	 * @param port - 接続先ポート。
	 * @param command - 入力コマンド。
	 * @return コマンドの結果。接続に失敗した場合はnull。
	 */
	public static String connectByTelnet(String host, int port, String command) {
		Logger logger = Logger.getLogger(OkuyamaUtil.class.getName());
		TelnetClient client = null;
		BufferedWriter output = null;
		BufferedReader input = null;
		try {
			// telnet接続
			client = new TelnetClient();
			client.connect(host, port);
			// 接続できたか確認
			if (!(client.isConnected())) {
				logger.warning("telnet接続失敗");
				return null;
			}
			logger.fine("telnet接続完了");
			// バージョン情報確認コマンドにて疎通確認
			output = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));
			input = new BufferedReader(new InputStreamReader(client.getInputStream()));
			output.write(command + "\n");
			output.flush();
			StringBuilder builder = new StringBuilder();
			String line = input.readLine();
			builder = builder.append(line);
			if (input.ready()) {
				for (line = input.readLine();line != null;line = input.readLine()) {
					builder = builder.append("\n");
					builder = builder.append(line);
				}
			}
			return builder.toString();
		} catch (Exception e) {
			logger.log(Level.WARNING, host + ":" + port +  "へのコマンド実行失敗", e);
			return null;
		} finally {
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					logger.log(Level.SEVERE, "telnetへの出力用ストリームのcloseに失敗しました", e);
				}
			}
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					logger.log(Level.SEVERE, "telnetへの入力用ストリームのcloseに失敗しました", e);
				}
			}
			if (client != null) {
				try {
					client.disconnect();
					logger.fine("telnet通信切断");
				} catch (IOException e) {
					logger.log(Level.SEVERE, "telnet通信切断に失敗しました", e);
				}
			}
		}
	}
	
	/**
	 * プロセスの出力をリダイレクトする。
	 * @param process - リダイレクト対象のプロセス。
	 * @param src - リダイレクト元。
	 * @param dest - リダイレクト先。
	 */
	public static void redirectProcess(TerminalProcess process, InputStream src, OutputStream dest) {
		if (process == null || src == null || dest == null) {
			return;
		}
		Runnable run = new OkuyamaUtil.ObserverThread(dest, process, src);
		new Thread(run, "ProcessRedirect").start();
	}
	
	/**
	 * propertiesのカンマ区切りのリストを分解する。
	 * @param value - propertiesのカンマ区切りのリストの値。
	 * @return 分解後の配列。
	 */
	public static String[] splitList(String value) {
		String[] result = value.split(",");
		for (int i = 0;i < result.length;i++) {
			result[i] = result[i].trim();
		}
		return result;
	}
	
	/**
	 * プロセスの出力監視用スレッドクラス。
	 * @author s-ito
	 *
	 */
	private static class ObserverThread implements Runnable {
		
		private OutputStream stream;
		
		private TerminalProcess target;
		
		private InputStream input;
		
		public ObserverThread(OutputStream stream , TerminalProcess target, InputStream input) {
			this.stream = stream;
			this.target = target;
			this.input = input;
		}

		@Override
		public void run() {
			byte[] buf = new byte[1024];
			try {
				for (int size = this.input.read(buf);0 <= size || !(target.isEnd());size = input.read(buf)) {
					this.stream.write(buf, 0, size);
					this.stream.flush();
				}
			} catch (Exception e) {
			} finally {
				try {
					this.input.close();
				} catch (IOException e) {
				}
			}
		}
	}
}
