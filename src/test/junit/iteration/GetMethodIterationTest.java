package test.junit.iteration;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import test.junit.MethodTestHelper;

/**
 * getメソッドの繰り返しテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class GetMethodIterationTest {

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	@Before
	public void setUp() throws Exception {
		GetMethodIterationTest.helper.init();
		GetMethodIterationTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  GetMethodIterationTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		for (int i = 0;i < 5000;i++) {
			String key = GetMethodIterationTest.helper.createTestDataKey(false, i);
			String value = GetMethodIterationTest.helper.createTestDataValue(false, i);
			this.okuyamaClient.setValue(key, value);
			this.okuyamaClient.setObjectValue(key + "_Object", value);
		}
	}

	@After
	public void tearDown() throws Exception {
		GetMethodIterationTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void キーに対応した値を5000個取得する() throws Exception {
		for (int i = 0;i < 5000;i++) {
			String testDataKey = GetMethodIterationTest.helper.createTestDataKey(false, i);
			String testDataValue = GetMethodIterationTest.helper.createTestDataValue(false, i);
			String[] result = this.okuyamaClient.getValue(testDataKey);
			if (result[0].equals("true")) {
				assertEquals(result[1], testDataValue);
			} else {
				fail("getメソッドエラー");
			}
		}
	}

	@Test
	public void 値をObjectとして5000個取得する() throws Exception {
		for (int i = 0;i < 5000;i++) {
			String testDataKey = GetMethodIterationTest.helper.createTestDataKey(false, i);
			String testDataValue = GetMethodIterationTest.helper.createTestDataValue(false, i);
			Object[] result = this.okuyamaClient.getObjectValue(testDataKey + "_Object");
			if (result[0].equals("true")) {
				assertEquals(result[1], testDataValue);
			} else {
				fail("getメソッドエラー");
			}
		}
	}
}
