package test.junit.huge;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import test.junit.MethodTestHelper;

/**
 * 巨大データに対するsetメソッドテスト。
 * @author s-ito
 *
 */
public class SetMethodHugeTest {

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		SetMethodHugeTest.helper.init();
		SetMethodHugeTest.helper.initBigTestData();
		// okuyamaに接続
		this.okuyamaClient =  SetMethodHugeTest.helper.getConnectedOkuyamaClient();
	}

	@After
	public void tearDown() throws Exception {
		this.okuyamaClient.close();
	}

	@Test
	public void setに成功してtrueを返す() throws Exception {
		assertTrue(this.okuyamaClient.setValue(SetMethodHugeTest.helper.createTestDataKey(true),
												SetMethodHugeTest.helper.createTestDataValue(true)));
	}
}
