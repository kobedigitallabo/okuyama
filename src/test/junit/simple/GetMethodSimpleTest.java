package test.junit.simple;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;
import okuyama.imdst.client.OkuyamaClientException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import test.junit.MethodTestHelper;

/**
 * getメソッドの簡単なテスト。
 * @author s-ito
 *
 */
public class GetMethodSimpleTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	private String testDataKey;

	private String testDataValue;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

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
	}

	@After
	public void tearDown() throws Exception {
		// テストデータを破棄
		try {
			this.okuyamaClient.removeValue(this.testDataKey);
		} catch (OkuyamaClientException e) {
		}

		this.okuyamaClient.close();
	}

	@Test
	public void キーに対応した値を取得する() throws Exception {
		String[] result = this.okuyamaClient.getValue(this.testDataKey);
		if (result[0].equals("true")) {
			assertEquals(result[1], this.testDataValue);
		} else {
			fail("getメソッドエラー");
		}
	}

	@Test
	public void 存在しないキーを取得しようとすることでfalseを返させる() throws Exception {
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
		this.okuyamaClient.removeValue(this.testDataKey);
		this.okuyamaClient.close();
		this.okuyamaClient.getValue(this.testDataKey);
	}

}
