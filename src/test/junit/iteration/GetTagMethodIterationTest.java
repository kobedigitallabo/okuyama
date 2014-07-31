package test.junit.iteration;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import test.junit.MethodTestHelper;

/**
 * getTagの繰り返しテスト
 * @author s-ito
 *
 */
public class GetTagMethodIterationTest {

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	@Before
	public void setUp() throws Exception {
		GetTagMethodIterationTest.helper.init();
		GetTagMethodIterationTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  GetTagMethodIterationTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		for (int i = 0;i < 5000;i++) {
			this.okuyamaClient.setValue(GetTagMethodIterationTest.helper.createTestDataKey(false, i),
										new String[]{GetTagMethodIterationTest.helper.createTestDataTag(i)},
										GetTagMethodIterationTest.helper.createTestDataValue(false, i));
		}
	}

	@After
	public void tearDown() throws Exception {
		GetTagMethodIterationTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void タグに対応したキーを5000個取得する() throws Exception {
		for (int i = 0;i < 5000;i++) {
			String testDataKey = GetTagMethodIterationTest.helper.createTestDataKey(false, i);
			String testDataTag = GetTagMethodIterationTest.helper.createTestDataTag(i);
			Object[] result = this.okuyamaClient.getTagKeys(testDataTag);
			if (result[0].equals("true")) {
				String[] keys = (String[]) result[1];
				assertEquals(keys[0], testDataKey);
			} else {
				fail("getメソッドエラー");
			}
		}
	}
}
