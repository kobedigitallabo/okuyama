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
 * listRPopメソッドの並列処理テスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class RPopMethodParallelTest {

	@Rule
	public MultiThreadRule thread = new MultiThreadRule(50);

	private static MethodTestHelper helper = new MethodTestHelper();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		RPopMethodParallelTest.helper.init();
		RPopMethodParallelTest.helper.initTestData();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		RPopMethodParallelTest.helper.deleteAllData();
	}
	
	@Before
	public void setUp() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = RPopMethodParallelTest.helper.getConnectedOkuyamaClient();
		String listName = RPopMethodParallelTest.helper.createTestDataKey(false) + "_List_" + id;
		String[] ret = client.createListStruct(listName);
		assertEquals(ret[0], "true");
		for (int i = 0;i < 100;i++) {
			ret = client.listRPush(listName,
									RPopMethodParallelTest.helper.createTestDataValue(false, i) + "_Data_" + id);
			assertEquals(ret[0], "true");
		}
		client.close();
	}
	
	@Test
	public void スレッドごとに別々なリストの末尾からデータを取り出す() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = RPopMethodParallelTest.helper.getConnectedOkuyamaClient();
		String listName = RPopMethodParallelTest.helper.createTestDataKey(false) + "_List_" + id;
		for (int i = 0;i < 100;i++) {
			String[] ret = client.listRPop(listName);
			assertEquals(ret[0], "true");
			assertEquals(ret[1], RPopMethodParallelTest.helper.createTestDataValue(false, 99 - i) + "_Data_" + id);
		}
		for (int i = 0;i < 100;i++) {
			String[] ret = client.listIndex(listName, i);
			assertEquals(ret[0], "false");
		}
		client.close();
	}
}
