package test.junit.iteration;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import test.junit.MethodTestHelper;

public class CasMethodIterationTest {

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	@Before
	public void setUp() throws Exception {
		CasMethodIterationTest.helper.init();
		CasMethodIterationTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  CasMethodIterationTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		for (int i = 0;i < 5000;i++) {
			String key = CasMethodIterationTest.helper.createTestDataKey(false, i);
			String value = CasMethodIterationTest.helper.createTestDataValue(false, i);
			this.okuyamaClient.setValue(key, value);
		}
	}

	@After
	public void tearDown() throws Exception {
		CasMethodIterationTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}
	
	@Test
	public void キーに対応した値を5000個取得する() throws Exception {
		for (int i = 0;i < 5000;i++) {
			String testDataKey = CasMethodIterationTest.helper.createTestDataKey(false, i);
			String testDataValue = CasMethodIterationTest.helper.createTestDataValue(false, i);
			Object[] getsRet = this.okuyamaClient.getValueVersionCheck(testDataKey);
			assertEquals(getsRet[0], "true");
			assertEquals(getsRet[1], testDataValue);
			String[] retParam
				= this.okuyamaClient.setValueVersionCheck(testDataKey, testDataValue + "_Updated", (String)getsRet[2]);
			assertEquals(retParam[0], "true");
			String[] getRet = this.okuyamaClient.getValue(testDataKey);
			assertEquals(getRet[0], "true");
			assertEquals(getRet[1], testDataValue + "_Updated");
		}
	}

}
