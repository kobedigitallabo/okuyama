package test.junit;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * okuyamaのメソッドを1回テストするためのクラス。
 * @author s-ito
 *
 */
public class MethodTest {

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	private int start;

	private int count;

	private String bigCharacter;

	private int nowCount;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		MethodTest.helper.init();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		// okuyamaに接続
		this.okuyamaClient =  MethodTest.helper.getConnectedOkuyamaClient();
		// 変数初期化
		this.start = MethodTest.helper.getStart();
		this.count = MethodTest.helper.getCount();
		this.bigCharacter = MethodTest.helper.getBigCharacter();
		this.nowCount = MethodTest.helper.getTestCount();
	}

	@After
	public void tearDown() throws Exception {
		// okuyamaとの通信を切断
		this.okuyamaClient.close();
	}

	@Test
	public void testSet() throws Exception {
		for (int i = start; i < count; i++) {
			// データ登録
			assertTrue("Set - Error=[" + this.nowCount + "datasavekey_" + new Integer(i).toString() + ", " + this.nowCount + "testdata1234567891011121314151617181920212223242526272829_savedatavaluestr_" + new Integer(i).toString(),
					okuyamaClient.setValue(this.nowCount + "datasavekey_" + new Integer(i).toString(), this.nowCount + "testdata1234567891011121314151617181920212223242526272829_savedatavaluestr_" + new Integer(i).toString()));
		}
		for (int i = start; i < start+500; i++) {
			// データ登録
			assertTrue("Set - Error=[" + this.nowCount + "datasavekey_bigdata_" + new Integer(i).toString(),
					okuyamaClient.setValue(this.nowCount + "datasavekey_bigdata_" + new Integer(i).toString(), this.nowCount + "savetestbigdata_" + new Integer(i).toString() + "_" + bigCharacter + "_" + new Integer(i).toString()));
		}
	}

}
