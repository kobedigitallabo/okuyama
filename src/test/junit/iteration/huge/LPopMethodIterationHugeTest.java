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
 * 巨大データに対するlistLPopメソッドの繰り返しテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class LPopMethodIterationHugeTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;
	
	private String listName;

	@Before
	public void setUp() throws Exception {
		LPopMethodIterationHugeTest.helper.init();
		LPopMethodIterationHugeTest.helper.initBigTestData();
		// okuyamaに接続
		this.okuyamaClient = LPopMethodIterationHugeTest.helper.getConnectedOkuyamaClient();
		// List構造準備
		this.listName = LPopMethodIterationHugeTest.helper.createTestDataKey(false);
		String[] ret = this.okuyamaClient.createListStruct(this.listName);
		assertEquals(ret[0], "true");
		for (int i = 0;i < 1000;i++) {
			String data = LPopMethodIterationHugeTest.helper.createTestDataKey(true, i);
			ret = this.okuyamaClient.listRPush(this.listName, data);
			assertEquals(ret[0], "true");
		}
	}

	@After
	public void tearDown() throws Exception {
		LPopMethodIterationHugeTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}
	
	@Test
	public void リストの先頭から1000個データを取り出す() throws Exception {
		String[] ret = null;
		for (int i = 0;i < 1000;i++) {
			ret = this.okuyamaClient.listLPop(this.listName);
			assertEquals(ret[0], "true");
			assertEquals(ret[1], LPopMethodIterationHugeTest.helper.createTestDataKey(true, i));
		}
		for (int i = 0;i < 1000;i++) {
			ret = this.okuyamaClient.listIndex(this.listName, i);
			assertEquals(ret[0], "false");
		}
	}

}
