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

public class RemoveTagMethodIterationTest {

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
		RemoveTagMethodIterationTest.helper.init();
		RemoveTagMethodIterationTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  RemoveTagMethodIterationTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		for (int i = 0;i < 5000;i++) {
			String testDataKey = RemoveTagMethodIterationTest.helper.createTestDataKey(false, i);
			String[] testDataTag = new String[]{RemoveTagMethodIterationTest.helper.createTestDataTag(i)};
			String testDataValue = RemoveTagMethodIterationTest.helper.createTestDataValue(false, i);
			this.okuyamaClient.setValue(testDataKey, testDataTag, testDataValue);
		}
	}

	@After
	public void tearDown() throws Exception {
		// テストデータを破棄
		try {
			for (int i = 0;i < 5000;i++) {
				String testDataKey = RemoveTagMethodIterationTest.helper.createTestDataKey(false, i);
				String testDataTag = RemoveTagMethodIterationTest.helper.createTestDataTag(i);
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
			String testDataKey = RemoveTagMethodIterationTest.helper.createTestDataKey(false, i);
			String testDataTag = RemoveTagMethodIterationTest.helper.createTestDataTag(i);
			assertTrue(this.okuyamaClient.removeTagFromKey(testDataKey, testDataTag));
			Object[] result = this.okuyamaClient.getTagKeys(testDataTag);
			assertEquals(result[0], "false");
		}
	}
}
