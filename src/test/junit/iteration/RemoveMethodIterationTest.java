package test.junit.iteration;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import test.junit.MethodTestHelper;

/**
 * removeメソッドの繰り返しテスト。
 * @author s-ito
 *
 */
public class RemoveMethodIterationTest {

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	@Before
	public void setUp() throws Exception {
		RemoveMethodIterationTest.helper.init();
		RemoveMethodIterationTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  RemoveMethodIterationTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		for (int i = 0;i < 5000;i++) {
			this.okuyamaClient.setValue(RemoveMethodIterationTest.helper.createTestDataKey(false, i),
										RemoveMethodIterationTest.helper.createTestDataValue(false, i));
		}
	}

	@After
	public void tearDown() throws Exception {
		RemoveMethodIterationTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void キーに対応した値を5000個削除する() throws Exception {
		for (int i = 0;i < 5000;i++) {
			String testDataKey = RemoveMethodIterationTest.helper.createTestDataKey(false, i);
			String testDataValue = RemoveMethodIterationTest.helper.createTestDataValue(false, i);
			String[] result = this.okuyamaClient.removeValue(testDataKey);
			if (result[0].equals("true")) {
				assertEquals(result[1], testDataValue);
			} else {
				fail("removeメソッドエラー");
			}
		}
	}
}
