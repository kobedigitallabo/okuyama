package test.junit.parallel;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;
import okuyama.imdst.client.OkuyamaClientException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import test.junit.MethodTestHelper;
import test.junit.MultiThreadRule;

/**
 * JavaScript実行の並列処理テスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class ScriptMethodParallelTest {

	@Rule
	public MultiThreadRule thread = new MultiThreadRule(100);

	private static MethodTestHelper helper = new MethodTestHelper();
	
	private static final String JAVASCRIPT = "var dataValue; var retValue = dataValue.replace('___Data', '___NewData'); var execRet = '1';";

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		ScriptMethodParallelTest.helper.init();
		ScriptMethodParallelTest.helper.initTestData();
		OkuyamaClient client = ScriptMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			client.setValue(ScriptMethodParallelTest.helper.createTestDataKey(false, i),
							ScriptMethodParallelTest.helper.createTestDataValue(false, i) + "___Data");
		}
		client.close();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		ScriptMethodParallelTest.helper.deleteAllData();
	}

	@Before
	public void setUp() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = ScriptMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			String key = ScriptMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id;
			client.setValue(key, ScriptMethodParallelTest.helper.createTestDataValue(false, i) + "_thread_" + id + "___Data");
		}
		client.close();
	}

	@After
	public void tearDown() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = ScriptMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			try {
				String key = ScriptMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id;
				client.removeValue(key);
			} catch (OkuyamaClientException e) {
			}
		}
		client.close();
	}

	@Test
	public void スレッドごとに違うキーに対応した値を50回取得するときにJavaScriptを実行する() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = ScriptMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			String testDataKey = ScriptMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id;
			String testDataValue = ScriptMethodParallelTest.helper.createTestDataValue(false, i) + "_thread_" + id;
			String[] result = client.getValueScript(testDataKey, ScriptMethodParallelTest.JAVASCRIPT);
			assertEquals(result[0], "true");
			assertEquals(result[1], testDataValue + "___NewData");
		}
		client.close();
	}

	@Test
	public void スレッドごとに同じキーに対応した値を50回取得するときにJavaScriptを実行する() throws Exception {
		OkuyamaClient client = ScriptMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			String testDataKey = ScriptMethodParallelTest.helper.createTestDataKey(false, i);
			String testDataValue = ScriptMethodParallelTest.helper.createTestDataValue(false, i);
			String[] result = client.getValueScript(testDataKey, ScriptMethodParallelTest.JAVASCRIPT);
			assertEquals(result[0], "true");
			assertEquals(result[1], testDataValue + "___NewData");
		}
		client.close();
	}

}
