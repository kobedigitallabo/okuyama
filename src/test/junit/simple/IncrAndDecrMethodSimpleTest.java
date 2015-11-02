package test.junit.simple;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import test.junit.MethodTestHelper;

/**
 * IncrメソッドとDecrメソッドの簡単なテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class IncrAndDecrMethodSimpleTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	private String testDataKey;

	private Long testDataValue;

	@Before
	public void setUp() throws Exception {
		IncrAndDecrMethodSimpleTest.helper.init();
		IncrAndDecrMethodSimpleTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  IncrAndDecrMethodSimpleTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		this.testDataKey = IncrAndDecrMethodSimpleTest.helper.createTestDataKey(false);
		this.testDataValue = Long.valueOf(10);
		this.okuyamaClient.setValue(this.testDataKey, this.testDataValue.toString());
		Long value = Long.valueOf(-10);
		this.okuyamaClient.setValue(this.testDataKey + "_0未満", value.toString());
		this.okuyamaClient.setObjectValue(this.testDataKey + "_Object", this.testDataValue);
	}

	@After
	public void tearDown() throws Exception {
		IncrAndDecrMethodSimpleTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void 値に加算する() throws Exception {
		Object[] ret = this.okuyamaClient.incrValue(this.testDataKey, 5);
		assertTrue((Boolean) ret[0]);
		assertEquals(ret[1], Long.valueOf(15));
		String[] value = this.okuyamaClient.getValue(this.testDataKey);
		assertEquals(value[0], "true");
		assertEquals(value[1], "15");
	}

	@Test
	public void 存在しない値に加算しようとして失敗する() throws Exception {
		Object[] ret = this.okuyamaClient.incrValue(this.testDataKey + "_Nothing", 15);
		assertFalse((Boolean) ret[0]);
	}

	@Test
	public void Objectとしてsetされた値に対しては0として加算する() throws Exception {
		Object[] ret = this.okuyamaClient.incrValue(this.testDataKey + "_Object", 15);
		assertTrue((Boolean) ret[0]);
		assertEquals(ret[1], Long.valueOf(15));
		String[] value = this.okuyamaClient.getValue(this.testDataKey + "_Object");
		assertEquals(value[0], "true");
		assertEquals(value[1], "15");
	}

	@Test
	public void 存在しない値に対して初期値を0として加算する() throws Exception {
		Object[] ret = this.okuyamaClient.incrValue(this.testDataKey + "_Nothing", 15, true);
		assertTrue((Boolean) ret[0]);
		assertEquals(ret[1], Long.valueOf(15));
		String[] value = this.okuyamaClient.getValue(this.testDataKey + "_Nothing");
		assertEquals(value[0], "true");
		assertEquals(value[1], "15");
	}

	@Test
	public void 値が0未満の場合はその絶対値に対して加算する() throws Exception {
		Object[] ret = this.okuyamaClient.incrValue(this.testDataKey + "_0未満", 15);
		assertTrue((Boolean) ret[0]);
		assertEquals(ret[1], Long.valueOf(5));
		String[] value = this.okuyamaClient.getValue(this.testDataKey + "_0未満");
		assertEquals(value[0], "true");
		assertEquals(value[1], "5");
	}

	@Test
	public void 加算結果が0未満のとき0とする() throws Exception {
		Object[] ret = this.okuyamaClient.incrValue(this.testDataKey + "_0未満", 5);
		assertTrue((Boolean) ret[0]);
		assertEquals(ret[1], Long.valueOf(0));
		String[] value = this.okuyamaClient.getValue(this.testDataKey + "_0未満");
		assertEquals(value[0], "true");
		assertEquals(value[1], "0");
	}

	@Test
	public void 引数が0未満の場合その絶対値で減算する() throws Exception {
		Object[] ret = this.okuyamaClient.decrValue(this.testDataKey, -5);
		assertTrue((Boolean) ret[0]);
		assertEquals(ret[1], Long.valueOf(5));
	}

	@Test
	public void 値を減算する() throws Exception {
		Object[] ret = this.okuyamaClient.decrValue(this.testDataKey, 5);
		assertTrue((Boolean) ret[0]);
		assertEquals(ret[1], Long.valueOf(5));
	}

	@Test
	public void 存在しない値を減算しようとして失敗する() throws Exception {
		Object[] ret = this.okuyamaClient.decrValue(this.testDataKey + "_Nothing", 15);
		assertFalse((Boolean) ret[0]);
	}

	@Test
	public void 値は0未満にならない() throws Exception {
		Object[] ret = this.okuyamaClient.decrValue(this.testDataKey, 100000);
		assertTrue((Boolean) ret[0]);
		assertEquals(ret[1], Long.valueOf(0));
	}

	@Test
	public void Objectとしてsetされた値に対しては0として減算する() throws Exception {
		Object[] ret = this.okuyamaClient.decrValue(this.testDataKey + "_Object", 15);
		assertTrue((Boolean) ret[0]);
		assertEquals(ret[1], Long.valueOf(0));
	}

	@Test
	public void 存在しない値に対して初期値を0として減算する() throws Exception {
		Object[] ret = this.okuyamaClient.decrValue(this.testDataKey + "_Object", -15, true);
		assertTrue((Boolean) ret[0]);
		assertEquals(ret[1], Long.valueOf(0));
	}
}
