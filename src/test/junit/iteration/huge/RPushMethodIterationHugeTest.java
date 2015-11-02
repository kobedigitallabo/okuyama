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
 * 巨大データに対するlistRPushメソッドの繰り返しテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class RPushMethodIterationHugeTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;
	
	private String listName;

	@Before
	public void setUp() throws Exception {
		RPushMethodIterationHugeTest.helper.init();
		RPushMethodIterationHugeTest.helper.initBigTestData();
		// okuyamaに接続
		this.okuyamaClient = RPushMethodIterationHugeTest.helper.getConnectedOkuyamaClient();
		// List構造準備
		this.listName = RPushMethodIterationHugeTest.helper.createTestDataKey(false);
		String[] ret = this.okuyamaClient.createListStruct(this.listName);
		assertEquals(ret[0], "true");
	}

	@After
	public void tearDown() throws Exception {
		RPushMethodIterationHugeTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}
	
	@Test
	public void リストに末尾に1000個データを追加する() throws Exception {
		String[] ret = null;
		for (int i = 0;i < 1000;i++) {
			ret = this.okuyamaClient.listRPush(this.listName, RPushMethodIterationHugeTest.helper.createTestDataKey(true, i));
			assertEquals(ret[0], "true");
		}
		for (int i = 0;i < 1000;i++) {
			ret = this.okuyamaClient.listIndex(this.listName, i);
			assertEquals(ret[0], "true");
			assertEquals(ret[1], RPushMethodIterationHugeTest.helper.createTestDataKey(true, i));
		}
	}

}
