package test.junit;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Map;
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

	private int start;

	private String bigTestDataSeed;

	private String testDataSeed;

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
	public void initBigTestData() {
		StringBuilder sb = new StringBuilder(6000*10);
		Random rnd = new Random();
		for (int i = 0; i < 300; i++) {
			sb.append(rnd.nextInt(1999999999));
		}
		this.bigTestDataSeed = sb.toString();
	}

	/**
	 * テストデータのための初期化をする。<br>
	 * createTestDataValueメソッド、createTestDataKeyメソッドを呼び出す前にこのメソッドを呼び出す必要がある。
	 */
	public void initTestData() {
		StringBuilder sb = new StringBuilder();
		Random rnd = new Random();
		sb.append(rnd.nextInt(10000));
		this.testDataSeed = sb.toString();
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
	 * テストデータのタグを作成する。<br>
	 * 事前にinitTestDataメソッドを呼び出す必要があります。
	 * @return テストデータのタグ。
	 */
	public String createTestDataTag() {
		StringBuilder builder = new StringBuilder();
		builder = builder.append("datasavetag_");
		builder = builder.append(this.testDataSeed);
		return builder.toString();
	}

	/**
	 * テストデータのタグを作成する。<br>
	 * 事前にinitTestDataメソッドを呼び出す必要があります。
	 * @paran index - 作成したキーの添え字。
	 * @return テストデータのタグ。
	 */
	public String createTestDataTag(int index) {
		index += this.start;
		return this.createTestDataTag() + "_" + index;
	}

	/**
	 * テストデータのキーを作成する。<br>
	 * 通常データを作るときはinitTestData、大きいデータを作るときはinitBigTestDataを事前に呼び出す必要があります。
	 * @param isBigData - 大きいデータ用のキーを作る場合はtrue。
	 * @return テストデータのキー。
	 */
	public String createTestDataKey(boolean isBigData) {
		StringBuilder builder = new StringBuilder();
		builder = builder.append(isBigData ? "datasavekey_bigdata_" : "datasavekey_");
		builder = builder.append(this.testDataSeed);
		return builder.toString();
	}

	/**
	 * テストデータのキーを作成する。<br>
	 * 通常データを作るときはinitTestData、大きいデータを作るときはinitBigTestDataを事前に呼び出す必要があります。
	 * @param isBigData - 大きいデータ用のキーを作る場合はtrue。
	 * @paran index - 作成したキーの添え字。
	 * @return テストデータのキー。
	 */
	public String createTestDataKey(boolean isBigData, int index) {
		index += this.start;
		return this.createTestDataKey(isBigData) + "_" + index;
	}

	/**
	 * テストデータの値を作成する。<br>
	 * 通常データを作るときはinitTestData、大きいデータを作るときはinitBigTestDataを事前に呼び出す必要があります。
	 * @param isBigData - 大きいデータを作る場合はtrue。
	 * @return テストデータの値。
	 */
	public String createTestDataValue(boolean isBigData) {
		// 値作成
		StringBuilder builder = new StringBuilder();
		if (isBigData) {
			builder = builder.append("savetestbigdata_");
			builder = builder.append(this.bigTestDataSeed);
		} else {
			builder = builder.append("testdata_");
			builder = builder.append(this.testDataSeed);
		}
		return builder.toString();
	}

	/**
	 * テストデータの値を作成する。<br>
	 * 通常データを作るときはinitTestData、大きいデータを作るときはinitBigTestDataを事前に呼び出す必要があります。
	 * @param isBigData - 大きいデータを作る場合はtrue。
	 * @paran index - 作成した値の添え字。
	 * @return テストデータの値。
	 */
	public String createTestDataValue(boolean isBigData, int index) {
		index += this.start;
		return this.createTestDataValue(isBigData) + "_" + index;
	}


	/**
	 * 指定されたキーに指定されたタグが全て紐付けられているか確認する。
	 * @param answer - 答え。Keyがキー値、Valueがキー値に紐付けられたタグ値のリスト。
	 * @param key - 確認対象のキー。
	 * @param tags - 確認対象のタグ値リスト。このリストは値の重複が無いものとする。
	 * @return 全て紐付けられていればtrueを返す。
	 */
	public static boolean checkTagAnd(Map<String, String[]> answer, String key, String[] tags) {
		String[] answerTags = answer.get(key);
		if (answerTags == null || answerTags.length < tags.length) {
			return false;
		}
		for (String tag : tags) {
			boolean result = false;
			for (String answerTag : answerTags) {
				if (answerTag.equals(tag)) {
					result = true;
					break;
				}
			}
			if (!result) {
				return false;
			}
		}
		return true;
	}

	/**
	 * 指定されたキーに指定されたタグのどれかが紐付けられているか確認する。
	 * @param answer - 答え。Keyがキー値、Valueがキー値に紐付けられたタグ値のリスト。
	 * @param key - 確認対象のキー。
	 * @param tags - 確認対象のタグ値リスト。このリストは値の重複が無いものとする。
	 * @return タグのどれかが紐付けられていればtrueを返す。
	 */
	public static boolean checkTagOr(Map<String, String[]> answer, String key, String[] tags) {
		String[] answerTags = answer.get(key);
		if (answerTags == null || answerTags.length <= 0) {
			return false;
		}
		boolean result = false;
		for (String tag : tags) {
			for (String answerTag : answerTags) {
				if (answerTag.equals(tag)) {
					result = true;
					break;
				}
			}
		}
		return result;
	}
}
