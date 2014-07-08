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
 * 並列でのsetTagテスト。
 * @author s-ito
 *
 */
public class SetTagMethodParallelTest {

	@Rule
	public MultiThreadRule thread = new MultiThreadRule(100);

	private static MethodTestHelper helper = new MethodTestHelper();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		SetTagMethodParallelTest.helper.init();
		SetTagMethodParallelTest.helper.initTestData();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		OkuyamaClient client = SetTagMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			String key = SetTagMethodParallelTest.helper.createTestDataKey(false, i);
			client.removeTagFromKey(key, SetTagMethodParallelTest.helper.createTestDataTag(i));
			client.removeValue(key);
		}
		client.close();
	}

	@After
	public void tearDown() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = SetTagMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			try {
				String key = SetTagMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id;
				client.removeTagFromKey(key, SetTagMethodParallelTest.helper.createTestDataTag(i) + "_thread_" + id);
				client.removeValue(key);
			} catch (OkuyamaClientException e) {
			}
		}
		client.close();
	}

	@Test
	public void スレッドで違うタグのsetをスレッドごとに50回行う() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = SetTagMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			assertTrue(client.setValue(SetTagMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id,
									new String[]{SetTagMethodParallelTest.helper.createTestDataTag(i) + "_thread_" + id},
									SetTagMethodParallelTest.helper.createTestDataValue(false, i) + "_thread_" + id));
		}
		client.close();
	}

	@Test
	public void スレッドで同じタグのsetをスレッドごとに50回行う() throws Exception {
		OkuyamaClient client = SetTagMethodParallelTest.helper.getConnectedOkuyamaClient();
		long id = Thread.currentThread().getId();
		for (int i = 0;i < 50;i++) {
			assertTrue(client.setValue(SetTagMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id ,
										new String[]{SetTagMethodParallelTest.helper.createTestDataTag(i)},
										SetTagMethodParallelTest.helper.createTestDataValue(false, i) + "_thread_" + id));
		}
		client.close();
	}

}
