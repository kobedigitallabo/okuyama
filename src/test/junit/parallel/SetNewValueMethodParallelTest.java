package test.junit.parallel;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import test.junit.MethodTestHelper;
import test.junit.MultiThreadRule;

/**
 * 新規登録の並列処理テスト
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class SetNewValueMethodParallelTest {

	@Rule
	public MultiThreadRule thread = new MultiThreadRule(100);

	private static MethodTestHelper helper = new MethodTestHelper();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		SetNewValueMethodParallelTest.helper.init();
		SetNewValueMethodParallelTest.helper.initTestData();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		SetNewValueMethodParallelTest.helper.deleteAllData();
	}

	@After
	public void tearDown() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = SetNewValueMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			client.removeValue(SetNewValueMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id);
		}
		client.close();
	}

	@Test
	public void スレッドで違うキーのsetを50回行う() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = SetNewValueMethodParallelTest.helper.getConnectedOkuyamaClient();
		String[] result;
		for (int i = 0;i < 50;i++) {
			result = client.setNewValue(SetNewValueMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id ,
										SetNewValueMethodParallelTest.helper.createTestDataValue(false, i) + "_thread_" + id);
			assertEquals(result[0], "true");
		}
		client.close();
	}

	@Test
	public void スレッドで違うキーのsetを値をObjectとして50回行う() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = SetNewValueMethodParallelTest.helper.getConnectedOkuyamaClient();
		String[] result;
		for (int i = 0;i < 50;i++) {
			result = client.setNewObjectValue(SetNewValueMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id,
										SetNewValueMethodParallelTest.helper.createTestDataValue(false, i) + "_thread_" + id);
			assertEquals(result[0], "true");
		}
		client.close();
	}

}
