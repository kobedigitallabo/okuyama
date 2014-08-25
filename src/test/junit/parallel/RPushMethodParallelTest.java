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
 * listRPushメソッドの並列処理テスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class RPushMethodParallelTest {

	@Rule
	public MultiThreadRule thread = new MultiThreadRule(50);

	private static MethodTestHelper helper = new MethodTestHelper();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		RPushMethodParallelTest.helper.init();
		RPushMethodParallelTest.helper.initTestData();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		RPushMethodParallelTest.helper.deleteAllData();
	}
	
	@Before
	public void setUp() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = RPushMethodParallelTest.helper.getConnectedOkuyamaClient();
		String[] ret = client.createListStruct(RPushMethodParallelTest.helper.createTestDataKey(false) + "_List_" + id);
		client.close();
		assertEquals(ret[0], "true");
	}
	
	@Test
	public void スレッドごとに別々なリストに末尾からデータを追加する() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = RPushMethodParallelTest.helper.getConnectedOkuyamaClient();
		String listName = RPushMethodParallelTest.helper.createTestDataKey(false) + "_List_" + id;
		for (int i = 0;i < 100;i++) {
			String[] ret = client.listRPush(listName,
										RPushMethodParallelTest.helper.createTestDataValue(false, i) + "_Data_" + id);
			assertEquals(ret[0], "true");
		}
		for (int i = 0;i < 100;i++) {
			String[] ret = client.listIndex(listName, i);
			assertEquals(ret[0], "true");
			assertEquals(ret[1], RPushMethodParallelTest.helper.createTestDataValue(false, i) + "_Data_" + id);
		}
		client.close();
	}

}
