package test.junit.iteration;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import test.junit.MethodTestHelper;

/**
 * IncrメソッドとDecrメソッドの簡単なテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class IncrAndDecrMethodIterationTest {

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	@Before
	public void setUp() throws Exception {
		IncrAndDecrMethodIterationTest.helper.init();
		IncrAndDecrMethodIterationTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  IncrAndDecrMethodIterationTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		for (int i = 0;i < 5000;i++) {
			String key = IncrAndDecrMethodIterationTest.helper.createTestDataKey(false, i);
			this.okuyamaClient.setValue(key, Long.valueOf(5000 + i).toString());
		}
	}

	@After
	public void tearDown() throws Exception {
		IncrAndDecrMethodIterationTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void 別々のキーに対応した値を5000個に加算する() throws Exception{
		String key = null;
		for (int i = 0;i < 5000;i++) {
			key = IncrAndDecrMethodIterationTest.helper.createTestDataKey(false, i);
			Object[] ret = this.okuyamaClient.incrValue(key, 5);
			assertTrue((Boolean) ret[0]);
			assertEquals(ret[1], Long.valueOf(5000 + i + 5));
			String[] value = this.okuyamaClient.getValue(key);
			assertEquals(value[0], "true");
			assertEquals(value[1], String.valueOf(5000 + i + 5));
		}
	}

	@Test
	public void 値1つに対して5000回加算する() throws Exception {
		String key = IncrAndDecrMethodIterationTest.helper.createTestDataKey(false, 0);
		for (int i = 1;i <= 5000;i++) {
			Object[] ret = this.okuyamaClient.incrValue(key, 1);
			assertTrue((Boolean) ret[0]);
			assertEquals(ret[1], Long.valueOf(5000 + i));
			String[] value = this.okuyamaClient.getValue(key);
			assertEquals(value[0], "true");
			assertEquals(value[1], String.valueOf(5000 + i));
		}
	}

	@Test
	public void 別々のキーに対応した値を5000個を減算する() throws Exception {
		String key = null;
		for (int i = 0;i < 5000;i++) {
			key = IncrAndDecrMethodIterationTest.helper.createTestDataKey(false, i);
			Object[] ret = this.okuyamaClient.decrValue(key, 5);
			assertTrue((Boolean) ret[0]);
			assertEquals(ret[1], Long.valueOf(5000 + i - 5));
			String[] value = this.okuyamaClient.getValue(key);
			assertEquals(value[0], "true");
			assertEquals(value[1], String.valueOf(5000 + i - 5));
		}
	}

	@Test
	public void 値1つに対して5000回減算する() throws Exception {
		String key = IncrAndDecrMethodIterationTest.helper.createTestDataKey(false, 0);
		for (int i = 1;i <= 5000;i++) {
			Object[] ret = this.okuyamaClient.decrValue(key, 1);
			assertTrue((Boolean) ret[0]);
			assertEquals(ret[1], Long.valueOf(5000 - i));
			String[] value = this.okuyamaClient.getValue(key);
			assertEquals(value[0], "true");
			assertEquals(value[1], String.valueOf(5000 - i));
		}
	}
}
