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

	private int start;

	private String bigCharacter;

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
	 * 全テストの終了を通知する。
	 */
	public void notifyTestEnd() {
		this.testCount++;
	}

	/**
	 * テストデータを作成する。
	 * @param index - テストデータに付けられる添字。
	 * @param isBigData - 大きいデータを作る場合はtrue。
	 * @return [0]:キー、[1]:値。
	 */
	public String[] getTestData(int index, boolean isBigData) {
		String[] result = new String[2];
		// Key作成
		StringBuilder builder = new StringBuilder();
		builder = builder.append(this.testCount);
		builder = builder.append(isBigData ? "datasavekey_bigdata_" : "datasavekey_");
		builder = builder.append(index);
		result[0] = builder.toString();
		// 値作成
		builder = new StringBuilder();
		builder = builder.append(this.testCount);
		if (isBigData) {
			builder = builder.append("savetestbigdata_");
			builder = builder.append(index);
			builder = builder.append("_");
			builder = builder.append(this.bigCharacter);
			builder = builder.append("_");
		} else {
			builder = builder.append("testdata1234567891011121314151617181920212223242526272829_savedatavaluestr_");
		}
		builder = builder.append(index);
		result[1] = builder.toString();
		return result;
	}

	public int getStart() {
		return this.start;
	}
}
