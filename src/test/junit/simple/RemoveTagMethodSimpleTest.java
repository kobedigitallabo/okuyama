package test.junit.simple;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import okuyama.imdst.client.OkuyamaClient;
import okuyama.imdst.client.OkuyamaClientException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import test.junit.MethodTestHelper;

/**
 * removeTagの簡単なテスト。
 * @author s-ito
 *
 */
public class RemoveTagMethodSimpleTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	private String[] testTags = new String[2];

	private String[] testKeys = new String[4];

	@Before
	public void setUp() throws Exception {
		RemoveTagMethodSimpleTest.helper.init();
		RemoveTagMethodSimpleTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  RemoveTagMethodSimpleTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		testTags[0] = RemoveTagMethodSimpleTest.helper.createTestDataTag(0);
		testTags[1] = RemoveTagMethodSimpleTest.helper.createTestDataTag(1);
		testKeys[0] = RemoveTagMethodSimpleTest.helper.createTestDataKey(false, 0);
		testKeys[1] = RemoveTagMethodSimpleTest.helper.createTestDataKey(false, 1);
		testKeys[2] = RemoveTagMethodSimpleTest.helper.createTestDataKey(false, 2);
		testKeys[3] = RemoveTagMethodSimpleTest.helper.createTestDataKey(false, 3);
		this.okuyamaClient.setValue(testKeys[0], new String[]{testTags[0]},
									RemoveTagMethodSimpleTest.helper.createTestDataValue(false, 0));
		this.okuyamaClient.setValue(testKeys[1], new String[]{testTags[1]},
									RemoveTagMethodSimpleTest.helper.createTestDataValue(false, 1));
		this.okuyamaClient.setValue(testKeys[2], new String[]{testTags[1]},
									RemoveTagMethodSimpleTest.helper.createTestDataValue(false, 2));
		this.okuyamaClient.setValue(testKeys[3], new String[]{testTags[1]},
									RemoveTagMethodSimpleTest.helper.createTestDataValue(false, 3));
	}

	@After
	public void tearDown() throws Exception {
		RemoveTagMethodSimpleTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void キーからタグを削除する() throws Exception {
		assertTrue(this.okuyamaClient.removeTagFromKey(testKeys[0], testTags[0]));
		Object[] result = this.okuyamaClient.getTagKeys(testTags[0]);
		assertEquals(result[0], "false");
		// タグを削除してもキーが生き残っていることを確認する
		String[] result2 = this.okuyamaClient.getValue(testKeys[0]);
		assertEquals(result2[0], "true");
	}

	@Test
	public void キーから複数のキーに紐付けられたタグを削除する() throws Exception {
		assertTrue(this.okuyamaClient.removeTagFromKey(testKeys[1], testTags[1]));
		Object[] result = this.okuyamaClient.getTagKeys(testTags[1]);
		if (result[0].equals("true")) {
			String[] keys = (String[]) result[1];
			Map<String, String> tagKey = new HashMap<String, String>();
			for (int i = 0;i < keys.length;i++) {
				tagKey.put(keys[i], testTags[1]);
			}
			assertNull(tagKey.get(testKeys[1]));
			assertNotNull(tagKey.get(testKeys[2]));
			assertNotNull(tagKey.get(testKeys[3]));
		} else {
			fail("getメソッドエラー");
		}
	}

	@Test
	public void 存在しないタグとキーの紐付けを削除しようとして失敗する() throws Exception {
		assertFalse(this.okuyamaClient.removeTagFromKey(testKeys[0], testTags[1]));
	}

	@Test
	public void サーバとのセッションが無い状態でgetすることで例外を発生させる() throws Exception {
		thrown.expect(OkuyamaClientException.class);
		thrown.expectMessage("No ServerConnect!!");
		this.okuyamaClient.close();
		this.okuyamaClient.removeTagFromKey(testKeys[0], testTags[0]);
	}

}
