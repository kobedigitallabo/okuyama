package test.junit.iteration.huge;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;
import okuyama.imdst.client.OkuyamaClientException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import test.junit.MethodTestHelper;

/**
 * 巨大なデータに対するsetメソッドの繰り返しテスト。
 * @author s-ito
 *
 */
public class SetMethodIterationHugeTest {

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	@Before
	public void setUp() throws Exception {
		SetMethodIterationHugeTest.helper.init();
		SetMethodIterationHugeTest.helper.initBigTestData();
		// okuyamaに接続
		this.okuyamaClient =  SetMethodIterationHugeTest.helper.getConnectedOkuyamaClient();
	}

	@After
	public void tearDown() throws Exception {
		try {
			for (int i = 0;i < 5000;i++) {
				this.okuyamaClient.removeValue(SetMethodIterationHugeTest.helper.createTestDataKey(false, i));
			}
		} catch (OkuyamaClientException e) {
		}
		this.okuyamaClient.close();
	}

	@Test
	public void 巨大なデータのsetに500回成功して全てtrueを返す() throws Exception {
		for (int i = 0;i < 500;i++) {
			assertTrue(this.okuyamaClient.setValue(SetMethodIterationHugeTest.helper.createTestDataKey(true, i),
												SetMethodIterationHugeTest.helper.createTestDataValue(true, i)));
		}
	}
}
