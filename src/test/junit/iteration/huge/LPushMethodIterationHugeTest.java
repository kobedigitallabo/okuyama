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
 * 巨大データに対するlistLPushメソッドの繰り返しテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class LPushMethodIterationHugeTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;
	
	private String listName;

	@Before
	public void setUp() throws Exception {
		LPushMethodIterationHugeTest.helper.init();
		LPushMethodIterationHugeTest.helper.initBigTestData();
		// okuyamaに接続
		this.okuyamaClient = LPushMethodIterationHugeTest.helper.getConnectedOkuyamaClient();
		// List構造準備
		this.listName = LPushMethodIterationHugeTest.helper.createTestDataKey(false);
		String[] ret = this.okuyamaClient.createListStruct(this.listName);
		assertEquals(ret[0], "true");
	}

	@After
	public void tearDown() throws Exception {
		LPushMethodIterationHugeTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}
	
	@Test
	public void リストに先頭から1000個データを追加する() throws Exception {
		String[] ret = null;
		for (int i = 0;i < 1000;i++) {
			ret = this.okuyamaClient.listLPush(this.listName, LPushMethodIterationHugeTest.helper.createTestDataKey(true, i));
			assertEquals(ret[0], "true");
		}
		for (int i = 0;i < 1000;i++) {
			ret = this.okuyamaClient.listIndex(this.listName, 999 - i);
			assertEquals(ret[0], "true");
			assertEquals(ret[1], LPushMethodIterationHugeTest.helper.createTestDataKey(true, i));
		}
	}
}
