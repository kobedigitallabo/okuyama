package test.junit.iteration.huge;

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
 * @author s-ito
 *
 */
public class RPopMethodIterationHugeTest {
	
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;
	
	private String listName;

	@Before
	public void setUp() throws Exception {
		RPopMethodIterationHugeTest.helper.init();
		RPopMethodIterationHugeTest.helper.initBigTestData();
		// okuyamaに接続
		this.okuyamaClient = RPopMethodIterationHugeTest.helper.getConnectedOkuyamaClient();
		// List構造準備
		this.listName = RPopMethodIterationHugeTest.helper.createTestDataKey(false);
		String[] ret = this.okuyamaClient.createListStruct(this.listName);
		assertEquals(ret[0], "true");
		for (int i = 0;i < 1000;i++) {
			ret = this.okuyamaClient.listRPush(this.listName,
												RPopMethodIterationHugeTest.helper.createTestDataKey(true, i));
			assertEquals(ret[0], "true");
		}
	}

	@After
	public void tearDown() throws Exception {
		RPopMethodIterationHugeTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}
	
	@Test
	public void リストの末尾から1000個データを取り出す() throws Exception {
		String[] ret = null;
		for (int i = 0;i < 1000;i++) {
			ret = this.okuyamaClient.listRPop(this.listName);
			assertEquals(ret[0], "true");
			assertEquals(ret[1], RPopMethodIterationHugeTest.helper.createTestDataKey(true, 999 - i));
		}
		for (int i = 0;i < 1000;i++) {
			ret = this.okuyamaClient.listIndex(this.listName, i);
			assertEquals(ret[0], "false");
		}
	}

}
