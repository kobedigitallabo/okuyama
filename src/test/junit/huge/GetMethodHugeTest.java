package test.junit.huge;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import test.junit.MethodTestHelper;

/**
 * 巨大データに対するgetメソッドテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class GetMethodHugeTest {

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	private String testDataKey;

	private String testDataValue;

	@Before
	public void setUp() throws Exception {
		GetMethodHugeTest.helper.init();
		GetMethodHugeTest.helper.initBigTestData();
		// okuyamaに接続
		this.okuyamaClient =  GetMethodHugeTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		this.testDataKey = GetMethodHugeTest.helper.createTestDataKey(false);
		this.testDataValue = GetMethodHugeTest.helper.createTestDataValue(false);
		this.okuyamaClient.setValue(this.testDataKey, this.testDataValue);
		this.okuyamaClient.setObjectValue(this.testDataKey + "_Object", this.testDataValue);
	}

	@After
	public void tearDown() throws Exception {
		GetMethodHugeTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void キーに対応した巨大な値を取得する() throws Exception {
		String[] result = this.okuyamaClient.getValue(this.testDataKey);
		if (result[0].equals("true")) {
			assertEquals(result[1], this.testDataValue);
		} else {
			fail("getメソッドエラー");
		}
	}

	@Test
	public void キーに対応した巨大な値をObjectとして取得する() throws Exception {
		Object[] result = this.okuyamaClient.getObjectValue(this.testDataKey + "_Object");
		if (result[0].equals("true")) {
			assertEquals(result[1], this.testDataValue);
		} else {
			fail("getメソッドエラー");
		}
	}
}
