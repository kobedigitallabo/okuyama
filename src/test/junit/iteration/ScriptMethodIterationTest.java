package test.junit.iteration;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import test.junit.MethodTestHelper;

/**
 * JavaScript実行の繰り返しテスト
 * @author s-ito
 *
 */
public class ScriptMethodIterationTest {

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;
	
	private static final String JAVASCRIPT = "var dataValue; var retValue = dataValue.replace('___Data', '___NewData'); var execRet = '1';";

	@Before
	public void setUp() throws Exception {
		ScriptMethodIterationTest.helper.init();
		ScriptMethodIterationTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  ScriptMethodIterationTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		for (int i = 0;i < 5000;i++) {
			String key = ScriptMethodIterationTest.helper.createTestDataKey(false, i);
			String value = ScriptMethodIterationTest.helper.createTestDataValue(false, i);
			this.okuyamaClient.setValue(key, value + "___Data");
		}
	}

	@After
	public void tearDown() throws Exception {
		ScriptMethodIterationTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void キーに対応した値を5000個取得するときにJavaScriptを実行する() throws Exception {
		for (int i = 0;i < 5000;i++) {
			String testDataKey = ScriptMethodIterationTest.helper.createTestDataKey(false, i);
			String testDataValue = ScriptMethodIterationTest.helper.createTestDataValue(false, i);
			String[] result = this.okuyamaClient.getValueScript(testDataKey, ScriptMethodIterationTest.JAVASCRIPT);
			assertEquals(result[0], "true");
			assertEquals(result[1], testDataValue + "___NewData");
		}
	}

}
