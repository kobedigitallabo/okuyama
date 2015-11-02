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
 * listLPopメソッドの並列処理テスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class LPopMethodParallelTest {

	@Rule
	public MultiThreadRule thread = new MultiThreadRule(50);

	private static MethodTestHelper helper = new MethodTestHelper();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		LPopMethodParallelTest.helper.init();
		LPopMethodParallelTest.helper.initTestData();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		LPopMethodParallelTest.helper.deleteAllData();
	}
	
	@Before
	public void setUp() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = LPopMethodParallelTest.helper.getConnectedOkuyamaClient();
		String listName = LPopMethodParallelTest.helper.createTestDataKey(false) + "_List_" + id;
		String[] ret = client.createListStruct(listName);
		assertEquals(ret[0], "true");
		for (int i = 0;i < 100;i++) {
			ret = client.listRPush(listName,
									LPopMethodParallelTest.helper.createTestDataValue(false, i) + "_Data_" + id);
			assertEquals(ret[0], "true");
		}
		client.close();
	}
	
	@Test
	public void スレッドごとに別々なリストの先頭からデータを取り出す() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = LPopMethodParallelTest.helper.getConnectedOkuyamaClient();
		String listName = LPopMethodParallelTest.helper.createTestDataKey(false) + "_List_" + id;
		for (int i = 0;i < 100;i++) {
			String[] ret = client.listLPop(listName);
			assertEquals(ret[0], "true");
			assertEquals(ret[1], LPopMethodParallelTest.helper.createTestDataValue(false, i) + "_Data_" + id);
		}
		for (int i = 0;i < 100;i++) {
			String[] ret = client.listIndex(listName, i);
			assertEquals(ret[0], "false");
		}
		client.close();
	}
}
