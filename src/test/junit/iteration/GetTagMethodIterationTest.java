package test.junit.iteration;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;
import okuyama.imdst.client.OkuyamaClientException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		GetTagMethodIterationTest.helper.init();
		GetTagMethodIterationTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  GetTagMethodIterationTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		for (int i = 0;i < 5000;i++) {
			String testDataKey = GetTagMethodIterationTest.helper.createTestDataKey(false, i);
			String[] testDataTag = new String[]{GetTagMethodIterationTest.helper.createTestDataTag(i)};
			String testDataValue = GetTagMethodIterationTest.helper.createTestDataValue(false, i);
			this.okuyamaClient.setValue(testDataKey, testDataTag, testDataValue);
		}
	}

	@After
	public void tearDown() throws Exception {
		// テストデータを破棄
		try {
			for (int i = 0;i < 5000;i++) {
				String testDataKey = GetTagMethodIterationTest.helper.createTestDataKey(false, i);
				String testDataTag = GetTagMethodIterationTest.helper.createTestDataTag(i);
				this.okuyamaClient.removeTagFromKey(testDataKey, testDataTag);
				this.okuyamaClient.removeValue(testDataKey);
			}
		} catch (OkuyamaClientException e) {
		}

		this.okuyamaClient.close();
	}

	@Test
	public void タグに対応したキーを5000回取得する() throws Exception {
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
