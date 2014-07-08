package test.junit.parallel;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;
import okuyama.imdst.client.OkuyamaClientException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import test.junit.MethodTestHelper;
import test.junit.MultiThreadRule;

/**
 * 並列でのsetテスト。
 * @author s-ito
 *
 */
public class SetMethodParallelTest {

	@Rule
	public MultiThreadRule thread = new MultiThreadRule(100);

	private static MethodTestHelper helper = new MethodTestHelper();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		SetMethodParallelTest.helper.init();
		SetMethodParallelTest.helper.initTestData();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		OkuyamaClient client = SetMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			try {
				client.removeValue(SetMethodParallelTest.helper.createTestDataKey(false, i));
			} catch (OkuyamaClientException e) {
			}
		}
		client.close();
	}

	@After
	public void tearDown() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = SetMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			client.removeValue(SetMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id);
		}
		client.close();
	}

	@Test
	public void スレッドで違うキーのsetを50回成功して全てtrueを返す() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = SetMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			assertTrue(client.setValue(SetMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id ,
										SetMethodParallelTest.helper.createTestDataValue(false, i) + "_thread_" + id));
		}
		client.close();
	}

	@Test
	public void スレッドで同じキーのsetを50回成功して全てtrueを返す() throws Exception {
		OkuyamaClient client = SetMethodParallelTest.helper.getConnectedOkuyamaClient();
		long id = Thread.currentThread().getId();
		for (int i = 0;i < 50;i++) {
			assertTrue(client.setValue(SetMethodParallelTest.helper.createTestDataKey(false, i),
										SetMethodParallelTest.helper.createTestDataValue(false, i) + "_thread_" + id));
		}
		client.close();
	}

}
