package test.junit.simple;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;
import okuyama.imdst.client.OkuyamaClientException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import test.junit.MethodTestHelper;

/**
 * getメソッドの簡単なテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class GetMethodSimpleTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	private String testDataKey;

	private String testDataValue;

	@Before
	public void setUp() throws Exception {
		GetMethodSimpleTest.helper.init();
		GetMethodSimpleTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  GetMethodSimpleTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		this.testDataKey = GetMethodSimpleTest.helper.createTestDataKey(false);
		this.testDataValue = GetMethodSimpleTest.helper.createTestDataValue(false);
		this.okuyamaClient.setValue(this.testDataKey, this.testDataValue);
		this.okuyamaClient.setObjectValue(this.testDataKey + "_Object", this.testDataValue);
		this.okuyamaClient.setValue(this.testDataKey + "日本語", this.testDataValue + "日本語");
	}

	@After
	public void tearDown() throws Exception {
		GetMethodSimpleTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void キーに対応した値を取得する() throws Exception {
		String[] result = this.okuyamaClient.getValue(this.testDataKey);
		assertEquals(result[0], "true");
		assertEquals(result[1], this.testDataValue);
	}

	@Test
	public void マルチバイト文字列を含むキーに対応した値を取得する() throws Exception {
		String[] result = this.okuyamaClient.getValue(this.testDataKey + "日本語");
		assertEquals(result[0], "true");
		assertEquals(result[1], this.testDataValue + "日本語");
	}

	@Test
	public void 値をObjectとして取得する() throws Exception {
		Object[] result = this.okuyamaClient.getObjectValue(this.testDataKey + "_Object");
		assertEquals(result[0], "true");
		assertEquals(result[1], this.testDataValue);
	}

	@Test
	public void 存在しないキーを取得しようとして失敗する() throws Exception {
		String[] result = this.okuyamaClient.getValue(this.testDataKey + "_foo");
		assertEquals(result[0], "false");
	}

	@Test
	public void nullのキーを指定して例外を発生させる() throws Exception {
		thrown.expect(OkuyamaClientException.class);
		thrown.expectMessage("The blank is not admitted on a key");
		this.okuyamaClient.getValue(null);
	}

	@Test
	public void サーバとのセッションが無い状態でgetすることで例外を発生させる() throws Exception {
		thrown.expect(OkuyamaClientException.class);
		thrown.expectMessage("No ServerConnect!!");
		this.okuyamaClient.close();
		this.okuyamaClient.getValue(this.testDataKey);
	}

}
