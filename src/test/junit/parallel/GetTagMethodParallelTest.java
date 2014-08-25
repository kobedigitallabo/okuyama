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
 * getTagメソッドの並列テスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class GetTagMethodParallelTest {

	@Rule
	public MultiThreadRule thread = new MultiThreadRule(100);

	private static MethodTestHelper helper = new MethodTestHelper();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		GetTagMethodParallelTest.helper.init();
		GetTagMethodParallelTest.helper.initTestData();
		OkuyamaClient client = GetTagMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			client.setValue(GetTagMethodParallelTest.helper.createTestDataKey(false, i),
							new String[]{GetTagMethodParallelTest.helper.createTestDataTag(i)},
							GetTagMethodParallelTest.helper.createTestDataValue(false, i));
		}
		client.close();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		GetTagMethodParallelTest.helper.deleteAllData();
	}

	@Before
	public void setUp() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = GetTagMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			client.setValue(GetTagMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id,
							new String[]{GetTagMethodParallelTest.helper.createTestDataTag(i) + "_thread_" + id},
							GetTagMethodParallelTest.helper.createTestDataValue(false, i) + "_thread_" + id);
		}
		client.close();
	}

	@After
	public void tearDown() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = GetTagMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			try {
				String key = GetTagMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id;
				client.removeTagFromKey(key, GetTagMethodParallelTest.helper.createTestDataTag(i) + "_thread_" + id);
				client.removeValue(key);
			} catch (OkuyamaClientException e) {
			}
		}
		client.close();
	}

	@Test
	public void スレッドごとに違うタグに対応したキーを50回取得する() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = GetTagMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			String testDataKey = GetTagMethodParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id;
			String testDataTag = GetTagMethodParallelTest.helper.createTestDataTag(i) + "_thread_" + id;
			Object[] result = client.getTagKeys(testDataTag);
			if (result[0].equals("true")) {
				String[] keys = (String[]) result[1];
				assertEquals(keys[0], testDataKey);
			} else {
				fail("getメソッドエラー");
			}
		}
		client.close();
	}

	@Test
	public void スレッドごとに同じタグに対応したキーを50回取得する() throws Exception {
		OkuyamaClient client = GetTagMethodParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			String testDataKey = GetTagMethodParallelTest.helper.createTestDataKey(false, i);
			String testDataTag = GetTagMethodParallelTest.helper.createTestDataTag(i);
			Object[] result = client.getTagKeys(testDataTag);
			if (result[0].equals("true")) {
				String[] keys = (String[]) result[1];
				assertEquals(keys[0], testDataKey);
			} else {
				fail("getメソッドエラー");
			}
		}
		client.close();
	}

}
