package test.junit.parallel;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import test.junit.MethodTestHelper;
import test.junit.MultiThreadRule;

/**
 * listLPushメソッドの並列処理テスト。
 * @author s-ito
 *
 */
public class LPushMethodParallelTest {

	@Rule
	public MultiThreadRule thread = new MultiThreadRule(50);

	private static MethodTestHelper helper = new MethodTestHelper();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		LPushMethodParallelTest.helper.init();
		LPushMethodParallelTest.helper.initTestData();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		LPushMethodParallelTest.helper.deleteAllData();
	}
	
	@Before
	public void setUp() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = LPushMethodParallelTest.helper.getConnectedOkuyamaClient();
		String[] ret = client.createListStruct(LPushMethodParallelTest.helper.createTestDataKey(false) + "_List_" + id);
		client.close();
		assertEquals(ret[0], "true");
	}
	
	@Test
	public void スレッドごとに別々なリストに先頭からデータを追加する() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = LPushMethodParallelTest.helper.getConnectedOkuyamaClient();
		String listName = LPushMethodParallelTest.helper.createTestDataKey(false) + "_List_" + id;
		for (int i = 0;i < 100;i++) {
			String[] ret = client.listLPush(listName,
										LPushMethodParallelTest.helper.createTestDataValue(false, i) + "_Data_" + id);
			assertEquals(ret[0], "true");
		}
		for (int i = 0;i < 100;i++) {
			String[] ret = client.listIndex(listName, 99 - i);
			assertEquals(ret[0], "true");
			assertEquals(ret[1], LPushMethodParallelTest.helper.createTestDataValue(false, i) + "_Data_" + id);
		}
		client.close();
	}

}
