package test.junit.huge;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import test.junit.MethodTestHelper;


/**
 * 巨大データに
 * @author s-ito
 *
 */
public class RPopMethodHugeTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;
	
	private String listName;
	
	private String listData;

	@Before
	public void setUp() throws Exception {
		RPopMethodHugeTest.helper.init();
		RPopMethodHugeTest.helper.initBigTestData();
		// okuyamaに接続
		this.okuyamaClient = RPopMethodHugeTest.helper.getConnectedOkuyamaClient();
		// List構造準備
		this.listName = RPopMethodHugeTest.helper.createTestDataKey(false, 0);
		this.listData = RPopMethodHugeTest.helper.createTestDataKey(true, 1);
		String[] ret = this.okuyamaClient.createListStruct(this.listName);
		assertEquals(ret[0], "true");
		ret = this.okuyamaClient.listLPush(this.listName, this.listData);
		assertEquals(ret[0], "true");
	}

	@After
	public void tearDown() throws Exception {
		RPopMethodHugeTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}
	
	@Test
	public void リストの末尾から巨大データを取り出す() throws Exception {
		String[] ret = okuyamaClient.listRPop(this.listName);
		assertEquals(ret[0], "true");
		assertEquals(ret[1], this.listData);
	}


}
