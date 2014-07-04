package test.junit.parallel;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import test.junit.MethodTestHelper;
import test.junit.MultiThreadRule;

/**
 * removeメソッドの並列処理テスト。
 * @author s-ito
 *
 */
public class RemoveMethodParallelTest {

	@Rule
	public MultiThreadRule thread = new MultiThreadRule(100);

	private static MethodTestHelper helper = new MethodTestHelper();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		RemoveMethodParallelTest.helper.init();
		RemoveMethodParallelTest.helper.initTestData();
		OkuyamaClient client = RemoveMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			client.setValue(RemoveMethodParallelTest.helper.createTestDataKey(false, i),
							RemoveMethodParallelTest.helper.createTestDataValue(false, i));
		}
		client.close();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		OkuyamaClient client = RemoveMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			client.removeValue(RemoveMethodParallelTest.helper.createTestDataKey(false, i));
		}
		client.close();
	}

	@Before
	public void setUp() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = RemoveMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			client.setValue(RemoveMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id,
							RemoveMethodParallelTest.helper.createTestDataValue(false, i) + "_thread_" + id);
		}
		client.close();
	}

	@After
	public void tearDown() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = RemoveMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			client.removeValue(RemoveMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id);
		}
		client.close();
	}

	@Test
	public void スレッドごとに違うキーに対応した値を50回削除する() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = RemoveMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			String testDataKey = RemoveMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id;
			String testDataValue = RemoveMethodParallelTest.helper.createTestDataValue(false, i) + "_thread_" + id;
			String[] result = client.removeValue(testDataKey);
			if (result[0].equals("true")) {
				assertEquals(result[1], testDataValue);
			} else {
				fail("getメソッドエラー");
			}
		}
		client.close();
	}

}
