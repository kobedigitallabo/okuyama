package test.junit.parallel;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;
import okuyama.imdst.client.OkuyamaClientException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import test.junit.MethodTestHelper;
import test.junit.MultiThreadRule;

public class IncrAndDecrMethodParallelTest {


	@Rule
	public MultiThreadRule thread = new MultiThreadRule(100);

	private static MethodTestHelper helper = new MethodTestHelper();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		IncrAndDecrMethodParallelTest.helper.init();
		IncrAndDecrMethodParallelTest.helper.initTestData();
	}

	@Before
	public void setUp() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = IncrAndDecrMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			String key = IncrAndDecrMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id;
			client.setValue(key, String.valueOf(i + id));
		}
		client.close();
	}

	@After
	public void tearDown() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = IncrAndDecrMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			try {
				String key = IncrAndDecrMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id;
				client.removeValue(key);
				client.removeValue(key + "_Object");
			} catch (OkuyamaClientException e) {
			}
		}
		client.close();
	}
	
	@Test
	public void スレッドごとに違うキーに対応した値を50回加算する() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = IncrAndDecrMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			String testDataKey = IncrAndDecrMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id;
			Object[] ret = client.incrValue(testDataKey, 1);
			assertTrue((Boolean) ret[0]);
			assertEquals(ret[1], Long.valueOf(i + id + 1));
			String[] result = client.getValue(testDataKey);
			assertEquals(result[0], "true");
			assertEquals(result[1], String.valueOf(i + id + 1));
		}
		client.close();
	}
	
	@Test
	public void スレッドごとに違うキーに対応した値を50回減算する() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = IncrAndDecrMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			String testDataKey = IncrAndDecrMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id;
			Object[] ret = client.decrValue(testDataKey, 1);
			assertTrue((Boolean) ret[0]);
			assertEquals(ret[1], Long.valueOf(i + id - 1));
			String[] result = client.getValue(testDataKey);
			assertEquals(result[0], "true");
			assertEquals(result[1], String.valueOf(i + id - 1));
		}
		client.close();
	}
	
	@Test
	public void 別々のデータに対して加算と減算を同時に行う() throws Exception {
		OkuyamaClient client = IncrAndDecrMethodParallelTest.helper.getConnectedOkuyamaClient();
		long id = Thread.currentThread().getId();
		if ((id % 2) == 0) {
			for (int i = 0;i < 50;i++) {
				String testDataKey = IncrAndDecrMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id;
				Object[] ret = client.decrValue(testDataKey, 1);
				assertTrue((Boolean) ret[0]);
				assertEquals(ret[1], Long.valueOf(i + id - 1));
				String[] result = client.getValue(testDataKey);
				assertEquals(result[0], "true");
				assertEquals(result[1], String.valueOf(i + id - 1));
			}
		} else {
			for (int i = 0;i < 50;i++) {
				String testDataKey = IncrAndDecrMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id;
				Object[] ret = client.incrValue(testDataKey, 1);
				assertTrue((Boolean) ret[0]);
				assertEquals(ret[1], Long.valueOf(i + id + 1));
				String[] result = client.getValue(testDataKey);
				assertEquals(result[0], "true");
				assertEquals(result[1], String.valueOf(i + id + 1));
			}
		}
		client.close();
	}

}
