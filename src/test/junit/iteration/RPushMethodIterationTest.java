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
 * listRPushメソッドの繰り返しテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class RPushMethodIterationTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;
	
	private String listName;
	
	private String[] listData = new String[1000];

	@Before
	public void setUp() throws Exception {
		RPushMethodIterationTest.helper.init();
		RPushMethodIterationTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient = RPushMethodIterationTest.helper.getConnectedOkuyamaClient();
		// List構造準備
		this.listName = RPushMethodIterationTest.helper.createTestDataKey(false);
		for (int i = 0;i < this.listData.length;i++) {
			this.listData[i] = RPushMethodIterationTest.helper.createTestDataKey(false, i);
		}
		String[] ret = this.okuyamaClient.createListStruct(this.listName);
		assertEquals(ret[0], "true");
	}

	@After
	public void tearDown() throws Exception {
		RPushMethodIterationTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}
	
	@Test
	public void リストに末尾に1000個データを追加する() throws Exception {
		String[] ret = null;
		for (int i = 0;i < 1000;i++) {
			ret = this.okuyamaClient.listRPush(this.listName, this.listData[i]);
			assertEquals(ret[0], "true");
		}
		for (int i = 0;i < 1000;i++) {
			ret = this.okuyamaClient.listIndex(this.listName, i);
			assertEquals(ret[0], "true");
			assertEquals(ret[1], this.listData[i]);
		}
	}

}
