package test.junit.iteration.huge;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import test.junit.MethodTestHelper;

/**
 * 巨大データに対する新規登録の繰り返しテスト
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class SetNewValueMethodIterationHugeTest {
	
	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	@Before
	public void setUp() throws Exception {
		SetNewValueMethodIterationHugeTest.helper.init();
		SetNewValueMethodIterationHugeTest.helper.initBigTestData();
		// okuyamaに接続
		this.okuyamaClient =  SetNewValueMethodIterationHugeTest.helper.getConnectedOkuyamaClient();
	}

	@After
	public void tearDown() throws Exception {
		SetNewValueMethodIterationHugeTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void 巨大なデータを500個setする() throws Exception {
		String[] result;
		for (int i = 0;i < 500;i++) {
			result = this.okuyamaClient.setNewValue(SetNewValueMethodIterationHugeTest.helper.createTestDataKey(true, i),
												SetNewValueMethodIterationHugeTest.helper.createTestDataValue(true, i));
			assertEquals(result[0], "true");
		}
	}

	@Test
	public void 値をObjectとした巨大なデータを500個setする() throws Exception {
		String[] result;
		for (int i = 0;i < 500;i++) {
			result = this.okuyamaClient.setNewObjectValue(SetNewValueMethodIterationHugeTest.helper.createTestDataKey(true, i),
												SetNewValueMethodIterationHugeTest.helper.createTestDataValue(true, i));
			assertEquals(result[0], "true");
		}
	}

}
