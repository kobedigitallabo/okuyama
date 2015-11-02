package test.junit.iteration;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import test.junit.MethodTestHelper;

/**
 * setメソッドの繰り返しテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class SetMethodIterationTest {

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	@Before
	public void setUp() throws Exception {
		SetMethodIterationTest.helper.init();
		SetMethodIterationTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  SetMethodIterationTest.helper.getConnectedOkuyamaClient();
	}

	@After
	public void tearDown() throws Exception {
		SetMethodIterationTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void データ5000個をsetする() throws Exception {
		for (int i = 0;i < 5000;i++) {
			assertTrue(this.okuyamaClient.setValue(SetMethodIterationTest.helper.createTestDataKey(false, i),
												SetMethodIterationTest.helper.createTestDataValue(false, i)));
		}
	}

	@Test
	public void 値をObjectとしたデータ5000個をsetする() throws Exception {
		for (int i = 0;i < 5000;i++) {
			assertTrue(this.okuyamaClient.setObjectValue(SetMethodIterationTest.helper.createTestDataKey(false, i),
														SetMethodIterationTest.helper.createTestDataValue(false, i)));
		}
	}
}
