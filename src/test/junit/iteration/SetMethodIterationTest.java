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
 * setメソッドの繰り返しテスト。
 * @author s-ito
 *
 */
public class SetMethodIterationTest {

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
		SetMethodIterationTest.helper.init();
		SetMethodIterationTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  SetMethodIterationTest.helper.getConnectedOkuyamaClient();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void setに5000回成功して全てtrueを返す() throws Exception {
		for (int i = 0;i < 5000;i++) {
			assertTrue(this.okuyamaClient.setValue(SetMethodIterationTest.helper.createTestDataKey(false, i),
												SetMethodIterationTest.helper.createTestDataValue(false, i)));
		}
	}
}
