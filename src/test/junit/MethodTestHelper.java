package test.junit;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Properties;
import java.util.Random;

import okuyama.imdst.client.OkuyamaClient;
import okuyama.imdst.client.OkuyamaClientException;

/**
 * MethodTest用Helperクラス。
 * @author s-ito
 *
 */
public class MethodTestHelper {

	private String masterNodeName;

	private int masterNodePort;

	private int testCount;

	private String bigCharacter;

	private int count;

	private int start;

	/**
	 * 初期化する。
	 */
	public void init() throws Exception {
		Reader reader = null;
		Exception exception = null;
		Properties prop = null;
		try {
			prop = new Properties();
			URL url = getClass().getClassLoader().getResource("test/junit/MethodTest.properties");
			reader = new InputStreamReader(new FileInputStream(new File(url.toURI())), "UTF-8");
			prop.load(reader);
		} catch (IOException e) {
			exception = e;
		}
		reader.close();
		if (exception != null) {
			throw exception;
		}
		String str = prop.getProperty("MasterNode");
		String[] strList = str.split(":");
		this.masterNodeName = strList[0];
		this.masterNodePort = Integer.valueOf(strList[1]);
		this.start = Integer.valueOf(prop.getProperty("start"));

		this.count = 5000;

		StringBuilder strBuf = new StringBuilder(6000*10);
		Random rnd = new Random();
		for (int i = 0; i < 300; i++) {
			strBuf.append(rnd.nextInt(1999999999));
		}
        this.bigCharacter = strBuf.toString();
	}

	/**
	 * 接続済みのOkuyamaクライアントを取得する。
	 * @return 接続済みのOkuyamaクライアント。
	 * @throws OkuyamaClientException
	 */
	public OkuyamaClient getConnectedOkuyamaClient() throws OkuyamaClientException {
		OkuyamaClient client = new OkuyamaClient();
		client.connect(this.masterNodeName, this.masterNodePort);
		return client;
	}

	/**
	 * 現在のテスト回数を取得する。
	 */
	public int getTestCount() {
		return this.testCount;
	}

	/**
	 * 全テストの終了を通知する。
	 */
	public void notifyTestEnd() {
		this.testCount++;
	}

	public String getBigCharacter() {
		return this.bigCharacter;
	}

	public int getCount() {
		return this.count;
	}

	public int getStart() {
		return this.start;
	}

}
