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

	private String masterNodeHost;

	private int masterNodePort;

	private int testCount;

	private int start;

	private String bigCharacter;

	/**
	 * 初期化する。
	 */
	public void init() throws Exception {
		Reader reader = null;
		Properties prop = null;
		try {
			prop = new Properties();
			URL url = getClass().getClassLoader().getResource("test/junit/MethodTest.properties");
			reader = new InputStreamReader(new FileInputStream(new File(url.toURI())), "UTF-8");
			prop.load(reader);
		} catch (IOException e) {
			throw e;
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
		String masterNode = prop.getProperty("MasterNode");
		String[] masterNodeAddress = masterNode.split(":");
		this.masterNodeHost = masterNodeAddress[0];
		this.masterNodePort = Integer.valueOf(masterNodeAddress[1]);
		this.start = Integer.valueOf(prop.getProperty("start", "0"));

	}

	/**
	 * 大きいデータ作成のための初期化をする。<br>
	 * createTestDataValueメソッド、createTestDataKeyメソッドを呼び出す前にこのメソッドを呼び出す必要がある。
	 */
	public void initBigData() {
		StringBuilder sb = new StringBuilder(6000*10);
		Random rnd = new Random();
		for (int i = 0; i < 300; i++) {
			sb.append(rnd.nextInt(1999999999));
		}
		this.bigCharacter = sb.toString();
	}

	/**
	 * 接続済みのOkuyamaクライアントを取得する。
	 * @return 接続済みのOkuyamaクライアント。
	 * @throws OkuyamaClientException
	 */
	public OkuyamaClient getConnectedOkuyamaClient() throws OkuyamaClientException {
		OkuyamaClient client = new OkuyamaClient();
		client.connect(this.masterNodeHost, this.masterNodePort);
		return client;
	}

	/**
	 * 全テストの終了を通知する。
	 */
	public void notifyTestEnd() {
		this.testCount++;
	}

	/**
	 * テストデータのキーを作成する。<br>
	 * 大きいデータを作るときは、事前にinitBigDataメソッドaを呼び出す必要があります。
	 * @param isBigData - 大きいデータ用のキーを作る場合はtrue。
	 * @return テストデータのキー。
	 */
	public String createTestDataKey(boolean isBigData) {
		StringBuilder builder = new StringBuilder();
		builder = builder.append(this.testCount);
		builder = builder.append(isBigData ? "datasavekey_bigdata_" : "datasavekey_");
		return builder.toString();
	}

	/**
	 * テストデータのキーを作成する。<br>
	 * 大きいデータを作るときは、事前にinitBigDataメソッドaを呼び出す必要があります。
	 * @param isBigData - 大きいデータ用のキーを作る場合はtrue。
	 * @return テストデータの値。
	 */
	public String createTestDataValue(boolean isBigData) {
		// 値作成
		StringBuilder builder = new StringBuilder();
		builder = builder.append(this.testCount);
		if (isBigData) {
			builder = builder.append("savetestbigdata_");
			builder = builder.append(this.bigCharacter);
			builder = builder.append("_");
		} else {
			builder = builder.append("testdata1234567891011121314151617181920212223242526272829_savedatavaluestr_");
		}
		return builder.toString();
	}

	public int getStart() {
		return this.start;
	}
}
