package test.junit.iteration;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import test.junit.MethodTestHelper;

/**
 * 新規登録の繰り返しテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class SetNewValueMethodIterationTest {

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	@Before
	public void setUp() throws Exception {
		SetNewValueMethodIterationTest.helper.init();
		SetNewValueMethodIterationTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  SetNewValueMethodIterationTest.helper.getConnectedOkuyamaClient();
	}

	@After
	public void tearDown() throws Exception {
		SetNewValueMethodIterationTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void データ5000個を新規登録する() throws Exception {
		String[] result;
		for (int i = 0;i < 5000;i++) {
			result = this.okuyamaClient.setNewValue(SetNewValueMethodIterationTest.helper.createTestDataKey(false, i),
												SetNewValueMethodIterationTest.helper.createTestDataValue(false, i));
			assertEquals(result[0], "true");
		}
	}

	@Test
	public void 値をObjectとしたデータ5000個を新規登録する() throws Exception {
		String[] result;
		for (int i = 0;i < 5000;i++) {
			result = this.okuyamaClient.setNewObjectValue(SetNewValueMethodIterationTest.helper.createTestDataKey(false, i),
														SetNewValueMethodIterationTest.helper.createTestDataValue(false, i));
			assertEquals(result[0], "true");
		}
	}

}
