package test.junit.huge;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import test.junit.MethodTestHelper;

/**
 * 巨大データに対するsetNewValueメソッドの簡単なテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class SetNewValueMethodHugeTest {

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	@Before
	public void setUp() throws Exception {
		SetNewValueMethodHugeTest.helper.init();
		SetNewValueMethodHugeTest.helper.initBigTestData();
		// okuyamaに接続
		this.okuyamaClient =  SetNewValueMethodHugeTest.helper.getConnectedOkuyamaClient();
	}

	@After
	public void tearDown() throws Exception {
		SetNewValueMethodHugeTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void 値のサイズが巨大なデータを新規登録する() throws Exception {
		String[] result = this.okuyamaClient.setNewValue(SetNewValueMethodHugeTest.helper.createTestDataKey(true),
														SetNewValueMethodHugeTest.helper.createTestDataValue(true));
		assertEquals(result[0], "true");
	}
	
	@Test
	public void サイズが大きい値をObjectとして新規登録する() throws Exception {
		String[] result = this.okuyamaClient.setNewObjectValue(SetNewValueMethodHugeTest.helper.createTestDataKey(true),
															SetNewValueMethodHugeTest.helper.createTestDataValue(true));
		assertEquals(result[0], "true");
	}
}
