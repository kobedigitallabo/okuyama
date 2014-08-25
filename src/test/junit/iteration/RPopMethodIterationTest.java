package test.junit.iteration;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import test.junit.MethodTestHelper;

/**
 * 巨大データに対するlistRPopメソッドの繰り返しテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class RPopMethodIterationTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;
	
	private String listName;
	
	private String[] listData = new String[1000];

	@Before
	public void setUp() throws Exception {
		RPopMethodIterationTest.helper.init();
		RPopMethodIterationTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient = RPopMethodIterationTest.helper.getConnectedOkuyamaClient();
		// List構造準備
		this.listName = RPopMethodIterationTest.helper.createTestDataKey(false);
		String[] ret = this.okuyamaClient.createListStruct(this.listName);
		assertEquals(ret[0], "true");
		for (int i = 0;i < this.listData.length;i++) {
			this.listData[i] = RPopMethodIterationTest.helper.createTestDataKey(false, i);
			ret = this.okuyamaClient.listRPush(this.listName, this.listData[i]);
			assertEquals(ret[0], "true");
		}
	}

	@After
	public void tearDown() throws Exception {
		RPopMethodIterationTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}
	
	@Test
	public void リストの末尾から1000個データを取り出す() throws Exception {
		String[] ret = null;
		for (int i = 0;i < 1000;i++) {
			ret = this.okuyamaClient.listRPop(this.listName);
			assertEquals(ret[0], "true");
			assertEquals(ret[1], this.listData[999 - i]);
		}
		for (int i = 0;i < 1000;i++) {
			ret = this.okuyamaClient.listIndex(this.listName, i);
			assertEquals(ret[0], "false");
		}
	}

}
