package test.junit.parallel;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import test.junit.MethodTestHelper;
import test.junit.MultiThreadRule;

/**
 * List構造作成メソッドの並列処理テスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class ListCreateMethodParallelTest {

	@Rule
	public MultiThreadRule thread = new MultiThreadRule(100);

	private static MethodTestHelper helper = new MethodTestHelper();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		ListCreateMethodParallelTest.helper.init();
		ListCreateMethodParallelTest.helper.initTestData();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		ListCreateMethodParallelTest.helper.deleteAllData();
	}
	
	@Test
	public void スレッドごとにList構造を作成する() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = ListCreateMethodParallelTest.helper.getConnectedOkuyamaClient();
		String[] ret = client.createListStruct(ListCreateMethodParallelTest.helper.createTestDataKey(false) + "_List_" + id);
		client.close();
		assertEquals(ret[0], "true");
	}

}
