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
 * 並列でのgetテスト。
 * @author s-ito
 *
 */
public class GetMethodParallelTest {

	@Rule
	public MultiThreadRule thread = new MultiThreadRule(100);

	private static MethodTestHelper helper = new MethodTestHelper();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		GetMethodParallelTest.helper.init();
		GetMethodParallelTest.helper.initTestData();
		OkuyamaClient client = GetMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			client.setValue(GetMethodParallelTest.helper.createTestDataKey(false, i),
							GetMethodParallelTest.helper.createTestDataValue(false, i));
		}
		client.close();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		OkuyamaClient client = GetMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			try {
				client.removeValue(GetMethodParallelTest.helper.createTestDataKey(false, i));
			} catch (OkuyamaClientException e) {
			}
		}
		client.close();
	}

	@Before
	public void setUp() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = GetMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			String key = GetMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id;
			client.setValue(key, GetMethodParallelTest.helper.createTestDataValue(false, i) + "_thread_" + id);
			client.setObjectValue(key + "_Object",
							GetMethodParallelTest.helper.createTestDataValue(false, i) + "_thread_" + id);
		}
		client.close();
	}

	@After
	public void tearDown() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = GetMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			try {
				String key = GetMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id;
				client.removeValue(key);
				client.removeValue(key + "_Object");
			} catch (OkuyamaClientException e) {
			}
		}
		client.close();
	}

	@Test
	public void スレッドごとに違うキーに対応した値を50回取得する() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = GetMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			String testDataKey = GetMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id;
			String testDataValue = GetMethodParallelTest.helper.createTestDataValue(false, i) + "_thread_" + id;
			String[] result = client.getValue(testDataKey);
			if (result[0].equals("true")) {
				assertEquals(result[1], testDataValue);
			} else {
				fail("getメソッドエラー");
			}
		}
		client.close();
	}

	@Test
	public void スレッドごとに同じキーに対応した値を50回取得する() throws Exception {
		OkuyamaClient client = GetMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			String testDataKey = GetMethodParallelTest.helper.createTestDataKey(false, i);
			String testDataValue = GetMethodParallelTest.helper.createTestDataValue(false, i);
			String[] result = client.getValue(testDataKey);
			if (result[0].equals("true")) {
				assertEquals(result[1], testDataValue);
			} else {
				fail("getメソッドエラー");
			}
		}
		client.close();
	}

	@Test
	public void スレッドごとに違うキーに対応した値をObjectとして50回取得する() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = GetMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			String testDataKey = GetMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id + "_Object";
			String testDataValue = GetMethodParallelTest.helper.createTestDataValue(false, i) + "_thread_" + id;
			Object[] result = client.getObjectValue(testDataKey);
			if (result[0].equals("true")) {
				assertEquals(result[1], testDataValue);
			} else {
				fail("getメソッドエラー");
			}
		}
		client.close();
	}


}
