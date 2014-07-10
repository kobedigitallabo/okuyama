package test.junit.simple;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import okuyama.imdst.client.OkuyamaClient;
import okuyama.imdst.client.OkuyamaClientException;
import okuyama.imdst.client.OkuyamaResultSet;

import org.junit.After;
import org.junit.Before;
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

	private String nothingTag;

	private Map<String, String[]> keyTags = new HashMap<String, String[]>();

	private Map<String, String> keyValue = new HashMap<String, String>();

	private String[] testTags = new String[2];

	@Before
	public void setUp() throws Exception {
		GetTagMethodSimpleTest.helper.init();
		GetTagMethodSimpleTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  GetTagMethodSimpleTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		this.testTags[0] = GetTagMethodSimpleTest.helper.createTestDataTag(0);
		this.testTags[1] = GetTagMethodSimpleTest.helper.createTestDataTag(1);

		String key = GetTagMethodSimpleTest.helper.createTestDataKey(false, 0);
		String[] tags = new String[]{this.testTags[0]};
		String value = GetTagMethodSimpleTest.helper.createTestDataValue(false, 0);
		this.okuyamaClient.setValue(key, tags, value);
		this.keyTags.put(key, tags);
		this.keyValue.put(key, value);

		key = GetTagMethodSimpleTest.helper.createTestDataKey(false, 1);
		tags = new String[]{this.testTags[1]};
		value = GetTagMethodSimpleTest.helper.createTestDataValue(false, 1);
		this.okuyamaClient.setValue(key, tags, value);
		this.keyTags.put(key, tags);
		this.keyValue.put(key, value);

		key = GetTagMethodSimpleTest.helper.createTestDataKey(false, 2);
		value = GetTagMethodSimpleTest.helper.createTestDataValue(false, 2);
		this.okuyamaClient.setValue(key, tags, value);
		this.keyTags.put(key, tags);
		this.keyValue.put(key, value);

		key = GetTagMethodSimpleTest.helper.createTestDataKey(false, 3);
		value = GetTagMethodSimpleTest.helper.createTestDataValue(false, 3);
		this.okuyamaClient.setValue(key, tags, value);
		this.keyTags.put(key, tags);
		this.keyValue.put(key, value);

		nothingTag = GetTagMethodSimpleTest.helper.createTestDataTag(2);
	}

	@After
	public void tearDown() throws Exception {
		try {
			this.okuyamaClient.getOkuyamaVersion();
		} catch (OkuyamaClientException e) {
			this.okuyamaClient = GetTagMethodSimpleTest.helper.getConnectedOkuyamaClient();
		}
		for (String key : this.keyTags.keySet()) {
			try {
				this.okuyamaClient.removeValue(key);
				String[] tags = this.keyTags.get(key);
				this.okuyamaClient.removeTagFromKey(key, tags[0]);
			} catch (OkuyamaClientException e) {
			}
		}
		this.okuyamaClient.close();
	}

	@Test
	public void タグからキーを1つ取得する() throws Exception {
		Object[] result = this.okuyamaClient.getTagKeys(testTags[0]);
		assertEquals(result[0], "true");
		String[] keys = (String[]) result[1];
		assertTrue(MethodTestHelper.checkTagResult(this.keyTags, new String[]{this.testTags[0]}, keys, true));
	}

	@Test
	public void タグからキーを複数取得する() throws Exception {
		Object[] result = this.okuyamaClient.getTagKeys(testTags[1]);
		assertEquals(result[0], "true");
		String[] keys = (String[]) result[1];
		assertTrue(MethodTestHelper.checkTagResult(this.keyTags, new String[]{this.testTags[1]}, keys, true));
	}

	@Test
	public void OkuyamaResultSetを使ってタグからデータを複数取得する() throws Exception {
		OkuyamaResultSet resultSet = this.okuyamaClient.getTagKeysResult(this.testTags[1]);
		assertNotNull(resultSet);
		ArrayList<String> keys = new ArrayList<String>();
		while (resultSet.next()) {
			String key = (String) resultSet.getKey();
			keys.add(key);
			assertEquals(resultSet.getValue(), this.keyValue.get(key));
		}
		assertTrue(MethodTestHelper.checkTagResult(this.keyTags, new String[]{this.testTags[1]},
														keys.toArray(new String[keys.size()]), true));
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
