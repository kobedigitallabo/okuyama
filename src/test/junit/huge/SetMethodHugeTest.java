package test.junit.huge;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import test.junit.MethodTestHelper;

/**
 * 巨大データに対するsetメソッドテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class SetMethodHugeTest {

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;


	@Before
	public void setUp() throws Exception {
		SetMethodHugeTest.helper.init();
		SetMethodHugeTest.helper.initBigTestData();
		// okuyamaに接続
		this.okuyamaClient =  SetMethodHugeTest.helper.getConnectedOkuyamaClient();
	}

	@After
	public void tearDown() throws Exception {
		SetMethodHugeTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void 値のサイズが巨大なデータをsetする() throws Exception {
		assertTrue(this.okuyamaClient.setValue(SetMethodHugeTest.helper.createTestDataKey(true),
												SetMethodHugeTest.helper.createTestDataValue(true)));
	}

	@Test
	public void サイズが大きい値をObjectとしてsetする() throws Exception {
		assertTrue(this.okuyamaClient.setObjectValue(SetMethodHugeTest.helper.createTestDataKey(true),
														SetMethodHugeTest.helper.createTestDataValue(true)));
	}
}
