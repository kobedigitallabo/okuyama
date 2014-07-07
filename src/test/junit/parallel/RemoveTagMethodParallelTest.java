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
 * removeTagメソッドの並列処理テスト。
 * @author s-ito
 *
 */
public class RemoveTagMethodParallelTest {

	@Rule
	public MultiThreadRule thread = new MultiThreadRule(100);

	private static MethodTestHelper helper = new MethodTestHelper();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		RemoveTagMethodParallelTest.helper.init();
		RemoveTagMethodParallelTest.helper.initTestData();
		OkuyamaClient client = RemoveTagMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			client.setValue(RemoveTagMethodParallelTest.helper.createTestDataKey(false, i),
							new String[]{RemoveTagMethodParallelTest.helper.createTestDataTag(i)},
							RemoveTagMethodParallelTest.helper.createTestDataValue(false, i));
		}
		client.close();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		OkuyamaClient client = RemoveTagMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			String key = RemoveTagMethodParallelTest.helper.createTestDataKey(false, i);
			client.removeTagFromKey(key, RemoveTagMethodParallelTest.helper.createTestDataTag(i));
			client.removeValue(key);
		}
		client.close();
	}

	@Before
	public void setUp() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = RemoveTagMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			client.setValue(RemoveTagMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id,
							new String[]{RemoveTagMethodParallelTest.helper.createTestDataTag(i) + "_thread_" + id},
							RemoveTagMethodParallelTest.helper.createTestDataValue(false, i) + "_thread_" + id);
		}
		client.close();
	}

	@After
	public void tearDown() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = RemoveTagMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			String key = RemoveTagMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id;
			client.removeTagFromKey(key, RemoveTagMethodParallelTest.helper.createTestDataTag(i));
			client.removeValue(RemoveTagMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id);
		}
		client.close();
	}

	@Test
	public void スレッドごとに違うタグに対応したキーを50回取得する() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = RemoveTagMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			String testDataKey = RemoveTagMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id;
			String testDataTag = RemoveTagMethodParallelTest.helper.createTestDataTag(i) + "_thread_" + id;
			assertTrue(client.removeTagFromKey(testDataKey, testDataTag));
			Object[] result = client.getTagKeys(testDataTag);
			assertEquals(result[0], "false");
		}
		client.close();
	}

}
