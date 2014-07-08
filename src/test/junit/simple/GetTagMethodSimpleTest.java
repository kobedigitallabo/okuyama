package test.junit.simple;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;
import okuyama.imdst.client.OkuyamaClientException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import test.junit.MethodTestHelper;

/**
 * getTagの簡単なテスト。
 * @author s-ito
 *
 */
public class GetTagMethodSimpleTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	private String[] testTags = new String[2];

	private String nothingTag;

	private String[] testKeys = new String[4];

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		GetTagMethodSimpleTest.helper.init();
		GetTagMethodSimpleTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  GetTagMethodSimpleTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		testTags[0] = GetTagMethodSimpleTest.helper.createTestDataTag(0);
		testTags[1] = GetTagMethodSimpleTest.helper.createTestDataTag(1);
		nothingTag = GetTagMethodSimpleTest.helper.createTestDataTag(2);
		testKeys[0] = GetTagMethodSimpleTest.helper.createTestDataKey(false, 0);
		testKeys[1] = GetTagMethodSimpleTest.helper.createTestDataKey(false, 1);
		testKeys[2] = GetTagMethodSimpleTest.helper.createTestDataKey(false, 2);
		testKeys[3] = GetTagMethodSimpleTest.helper.createTestDataKey(false, 3);
		this.okuyamaClient.setValue(testKeys[0], new String[]{testTags[0]},
									GetTagMethodSimpleTest.helper.createTestDataValue(false, 0));
		this.okuyamaClient.setValue(testKeys[1], new String[]{testTags[1]},
									GetTagMethodSimpleTest.helper.createTestDataValue(false, 1));
		this.okuyamaClient.setValue(testKeys[2], new String[]{testTags[1]},
									GetTagMethodSimpleTest.helper.createTestDataValue(false, 2));
		this.okuyamaClient.setValue(testKeys[3], new String[]{testTags[1]},
									GetTagMethodSimpleTest.helper.createTestDataValue(false, 3));
	}

	@After
	public void tearDown() throws Exception {
		try {
			this.okuyamaClient.getOkuyamaVersion();
		} catch (OkuyamaClientException e) {
			this.okuyamaClient = GetTagMethodSimpleTest.helper.getConnectedOkuyamaClient();
		}
		// テストデータを破棄
		try {
			this.okuyamaClient.removeValue(testKeys[0]);
			this.okuyamaClient.removeTagFromKey(testKeys[0], testTags[0]);
			this.okuyamaClient.removeValue(testKeys[1]);
			this.okuyamaClient.removeTagFromKey(testKeys[1], testTags[1]);
			this.okuyamaClient.removeValue(testKeys[2]);
			this.okuyamaClient.removeTagFromKey(testKeys[2], testTags[1]);
			this.okuyamaClient.removeValue(testKeys[3]);
			this.okuyamaClient.removeTagFromKey(testKeys[2], testTags[1]);
		} catch (OkuyamaClientException e) {
		}

		this.okuyamaClient.close();
	}

	@Test
	public void タグからキーを1つ取得する() throws Exception {
		Object[] result = this.okuyamaClient.getTagKeys(testTags[0]);
		if (result[0].equals("true")) {
			String[] keys = (String[]) result[1];
			assertEquals(keys[0], GetTagMethodSimpleTest.helper.createTestDataKey(false, 0));
		} else {
			fail("getメソッドエラー");
		}
	}

	@Test
	public void タグからキーを複数取得する() throws Exception {
		Object[] result = this.okuyamaClient.getTagKeys(testTags[1]);
		if (result[0].equals("true")) {
			String[] keys = (String[]) result[1];
			for (String key : keys) {
				boolean assertFlag = false;
				for (String answerKey : this.testKeys) {
					if (answerKey.equals(key)) {
						assertFlag = true;
					}
				}
				assertTrue(assertFlag);
			}
		} else {
			fail("getメソッドエラー");
		}
	}

	@Test
	public void 存在しないタグからキーを取得しようとして失敗する() throws Exception {
		Object[] result = this.okuyamaClient.getTagKeys(nothingTag);
		assertEquals(result[0], "false");
	}

	@Test
	public void サーバとのセッションが無い状態でgetすることで例外を発生させる() throws Exception {
		thrown.expect(OkuyamaClientException.class);
		thrown.expectMessage("No ServerConnect!!");
		this.okuyamaClient.close();
		this.okuyamaClient.getTagKeys(testTags[0]);
	}

}
