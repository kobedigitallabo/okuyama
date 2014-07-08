package test.junit.iteration;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;
import okuyama.imdst.client.OkuyamaClientException;

import org.junit.After;
import org.junit.Before;
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

	@Before
	public void setUp() throws Exception {
		SetMethodIterationTest.helper.init();
		SetMethodIterationTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  SetMethodIterationTest.helper.getConnectedOkuyamaClient();
	}

	@After
	public void tearDown() throws Exception {
		try {
			for (int i = 0;i < 5000;i++) {
				this.okuyamaClient.removeValue(SetMethodIterationTest.helper.createTestDataKey(false, i));
			}
		} catch (OkuyamaClientException e) {
		}
		this.okuyamaClient.close();
	}

	@Test
	public void データ5000個をsetする() throws Exception {
		for (int i = 0;i < 5000;i++) {
			assertTrue(this.okuyamaClient.setValue(SetMethodIterationTest.helper.createTestDataKey(false, i),
												SetMethodIterationTest.helper.createTestDataValue(false, i)));
		}
	}
}
