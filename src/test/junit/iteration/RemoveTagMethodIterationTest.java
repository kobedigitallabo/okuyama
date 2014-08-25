package test.junit.iteration;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import test.junit.MethodTestHelper;

/**
 * removeTagの繰り返しテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class RemoveTagMethodIterationTest {

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	@Before
	public void setUp() throws Exception {
		RemoveTagMethodIterationTest.helper.init();
		RemoveTagMethodIterationTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  RemoveTagMethodIterationTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		for (int i = 0;i < 5000;i++) {
			this.okuyamaClient.setValue(RemoveTagMethodIterationTest.helper.createTestDataKey(false, i),
										new String[]{RemoveTagMethodIterationTest.helper.createTestDataTag(i)},
										RemoveTagMethodIterationTest.helper.createTestDataValue(false, i));
		}
	}

	@After
	public void tearDown() throws Exception {
		RemoveTagMethodIterationTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void タグに対応したデータを5000個削除する() throws Exception {
		for (int i = 0;i < 5000;i++) {
			String testDataKey = RemoveTagMethodIterationTest.helper.createTestDataKey(false, i);
			String testDataTag = RemoveTagMethodIterationTest.helper.createTestDataTag(i);
			assertTrue(this.okuyamaClient.removeTagFromKey(testDataKey, testDataTag));
			Object[] result = this.okuyamaClient.getTagKeys(testDataTag);
			assertEquals(result[0], "false");
		}
	}
}
