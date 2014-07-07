package test.junit.iteration;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import test.junit.MethodTestHelper;

/**
 * setTagメソッドの繰り返しテスト。
 * @author s-ito
 *
 */
public class SetTagMethodIterationTest {

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
		SetTagMethodIterationTest.helper.init();
		SetTagMethodIterationTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  SetTagMethodIterationTest.helper.getConnectedOkuyamaClient();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void set5000回で別々のタグを設定する() throws Exception {
		for (int i = 0;i < 5000;i++) {
			assertTrue(this.okuyamaClient.setValue(SetTagMethodIterationTest.helper.createTestDataKey(false, i),
												new String[]{SetTagMethodIterationTest.helper.createTestDataTag(i)},
												SetTagMethodIterationTest.helper.createTestDataValue(false, i)));
		}
	}

	@Test
	public void set5000回で同じタグを設定する() throws Exception {
		for (int i = 0;i < 5000;i++) {
			assertTrue(this.okuyamaClient.setValue(SetTagMethodIterationTest.helper.createTestDataKey(false, i),
												new String[]{SetTagMethodIterationTest.helper.createTestDataTag()},
												SetTagMethodIterationTest.helper.createTestDataValue(false, i)));
		}
	}

}
