package test.junit.simple;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import test.junit.MethodTestHelper;

/**
 * JavaScript実行の簡単なテスト
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class ScriptMethodSimpleTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();
	
	private static final String JAVASCRIPT = "var dataValue; var retValue = dataValue.replace('___Data', '___NewData'); var execRet = '1';";

	private OkuyamaClient okuyamaClient;

	private String testDataKey;

	private String testDataValue;

	@Before
	public void setUp() throws Exception {
		ScriptMethodSimpleTest.helper.init();
		ScriptMethodSimpleTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  ScriptMethodSimpleTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		this.testDataKey = ScriptMethodSimpleTest.helper.createTestDataKey(false);
		this.testDataValue = ScriptMethodSimpleTest.helper.createTestDataValue(false);
		this.okuyamaClient.setValue(this.testDataKey, this.testDataValue + "___Data");
	}

	@After
	public void tearDown() throws Exception {
		ScriptMethodSimpleTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void get時にJavaScriptを実行する() throws Exception {
		String[] result = this.okuyamaClient.getValueScript(this.testDataKey, ScriptMethodSimpleTest.JAVASCRIPT);
		assertEquals(result[0], "true");
		assertEquals(result[1], this.testDataValue + "___NewData");
	}
}
