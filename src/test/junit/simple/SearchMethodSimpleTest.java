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
 * Index作成と検索メソッドの簡単なテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class SearchMethodSimpleTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();
	
	private static final String SEARCH_TARGET_VALUE1 = "123abcあいう絵尾";
	
	private static final String SEARCH_TARGET_VALUE2 = "123defかきく毛個";
	
	private static final String SEARCH_TARGET_VALUE_IN_GROUP1 = "456ghiさしす背祖";
	
	private static final String SEARCH_TRAGET_VALUE_IN_GROUP2 = "456jklたちつ手戸";
	
	private static final String SEARCH_TARGET_GROUP1 = "group1";
	
	private static final String SEARCH_TARGET_GROUP2 = "group2";
	
	private String searchTargetKey1;
	
	private String searchTargetKey2;
	
	private String searchTargetKeyInGroup1;
	
	private String searchTargetKeyInGroup2;
	
	private OkuyamaClient okuyamaClient;
	
	@Before
	public void setUp() throws Exception {
		SearchMethodSimpleTest.helper.init();
		SearchMethodSimpleTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  SearchMethodSimpleTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		this.searchTargetKey1 = SearchMethodSimpleTest.helper.createTestDataKey(false, 0);
		this.searchTargetKey2 = SearchMethodSimpleTest.helper.createTestDataKey(false, 1);
		this.searchTargetKeyInGroup1 = SearchMethodSimpleTest.helper.createTestDataKey(false, 2);
		this.searchTargetKeyInGroup2 = SearchMethodSimpleTest.helper.createTestDataKey(false, 3);
		this.okuyamaClient.setValueAndCreateIndex(this.searchTargetKey1, SearchMethodSimpleTest.SEARCH_TARGET_VALUE1);
		this.okuyamaClient.setValueAndCreateIndex(this.searchTargetKey2, SearchMethodSimpleTest.SEARCH_TARGET_VALUE2);
		this.okuyamaClient.setValueAndCreateIndex(this.searchTargetKeyInGroup1,
													SearchMethodSimpleTest.SEARCH_TARGET_VALUE_IN_GROUP1,
													SearchMethodSimpleTest.SEARCH_TARGET_GROUP1);
		this.okuyamaClient.setValueAndCreateIndex(this.searchTargetKeyInGroup2,
													SearchMethodSimpleTest.SEARCH_TRAGET_VALUE_IN_GROUP2,
													SearchMethodSimpleTest.SEARCH_TARGET_GROUP2);
	}

	@After
	public void tearDown() throws Exception {
		SearchMethodSimpleTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void キーワード1つで値を検索する() throws Exception {
		String[] keyword = new String[]{"abc"};
		Object[] searchResult = this.okuyamaClient.searchValue(keyword, "1");
		assertEquals(searchResult[0], "true");
		String[] searchResultKeys = (String[]) searchResult[1];
		assertEquals(searchResultKeys.length, 1);
		String[] getResult = this.okuyamaClient.getValue(searchResultKeys[0]);
		assertEquals(getResult[0], "true");
		assertEquals(getResult[1], SearchMethodSimpleTest.SEARCH_TARGET_VALUE1);
	}
	
	@Test
	public void 日本語のキーワード1つで値を検索する() throws Exception {
		String[] keyword = new String[]{"あいう"};
		Object[] searchResult = this.okuyamaClient.searchValue(keyword, "1");
		assertEquals(searchResult[0], "true");
		String[] searchResultKeys = (String[]) searchResult[1];
		assertEquals(searchResultKeys.length, 1);
		String[] getResult = this.okuyamaClient.getValue(searchResultKeys[0]);
		assertEquals(getResult[0], "true");
		assertEquals(getResult[1], SearchMethodSimpleTest.SEARCH_TARGET_VALUE1);
	}
	
	@Test
	public void 検索により複数の値を得る() throws Exception {
		String[] keyword = new String[]{"123"};
		Object[] searchResult = this.okuyamaClient.searchValue(keyword, "1");
		assertEquals(searchResult[0], "true");
		String[] searchResultKeys = (String[]) searchResult[1];
		assertEquals(searchResultKeys.length, 2);
		for (String key : searchResultKeys) {
			String[] getResult = this.okuyamaClient.getValue(key);
			assertEquals(getResult[0], "true");
			if (key.equals(this.searchTargetKey1)) {
				assertEquals(getResult[1], SearchMethodSimpleTest.SEARCH_TARGET_VALUE1);
			} else if (key.equals(this.searchTargetKey2)) {
				assertEquals(getResult[1], SearchMethodSimpleTest.SEARCH_TARGET_VALUE2);
			} else {
				fail();
			}
		}
	}
	
	@Test
	public void 複数のキーワードによるAND検索をする() throws Exception {
		String[] keyword = new String[]{"あいう", "123"};
		Object[] searchResult = this.okuyamaClient.searchValue(keyword, "1");
		assertEquals(searchResult[0], "true");
		String[] searchResultKeys = (String[]) searchResult[1];
		assertEquals(searchResultKeys.length, 1);
		String[] getResult = this.okuyamaClient.getValue(searchResultKeys[0]);
		assertEquals(getResult[0], "true");
		assertEquals(getResult[1], SearchMethodSimpleTest.SEARCH_TARGET_VALUE1);
	}
	
	@Test
	public void 複数のキーワードによるOR検索をする() throws Exception {
		String[] keyword = new String[]{"abc", "かきく"};
		Object[] searchResult = this.okuyamaClient.searchValue(keyword, "0");
		assertEquals(searchResult[0], "true");
		String[] searchResultKeys = (String[]) searchResult[1];
		assertEquals(searchResultKeys.length, 2);
		for (String key : searchResultKeys) {
			String[] getResult = this.okuyamaClient.getValue(key);
			assertEquals(getResult[0], "true");
			if (key.equals(this.searchTargetKey1)) {
				assertEquals(getResult[1], SearchMethodSimpleTest.SEARCH_TARGET_VALUE1);
			} else if (key.equals(this.searchTargetKey2)) {
				assertEquals(getResult[1], SearchMethodSimpleTest.SEARCH_TARGET_VALUE2);
			} else {
				fail();
			}
		}
	}
	
	@Test
	public void 存在しない値を検索して失敗する() throws Exception {
		String[] keyword = new String[]{"なにぬ"};
		Object[] searchResult = this.okuyamaClient.searchValue(keyword, "1");
		assertEquals(searchResult[0], "false");
	}
	
	@Test
	public void 削除済みの値を検索して失敗する() throws Exception {
		this.okuyamaClient.removeValue(this.searchTargetKey1);
		String[] keyword = new String[]{"あいう"};
		Object[] searchResult = this.okuyamaClient.searchValue(keyword, "1");
		assertEquals(searchResult[0], "true");
		String[] searchResultKeys = (String[]) searchResult[1];
		assertEquals(searchResultKeys.length, 1);
		String[] getResult = this.okuyamaClient.getValue(searchResultKeys[0]);
		assertEquals(getResult[0], "false");
	}
	
	@Test
	public void Index削除済みの値を検索して失敗する() throws Exception {
		this.okuyamaClient.removeSearchIndex(this.searchTargetKey1);
		String[] keyword = new String[]{"あいう"};
		Object[] searchResult = this.okuyamaClient.searchValue(keyword, "1");
		assertEquals(searchResult[0], "false");
	}
	
	@Test
	public void グループ内検索をする() throws Exception {
		String[] keyword = new String[]{"さしす"};
		Object[] searchResult = this.okuyamaClient.searchValue(keyword,
																"1", SearchMethodSimpleTest.SEARCH_TARGET_GROUP1);
		assertEquals(searchResult[0], "true");
		String[] searchResultKeys = (String[]) searchResult[1];
		assertEquals(searchResultKeys.length, 1);
		String[] getResult = this.okuyamaClient.getValue(searchResultKeys[0]);
		assertEquals(getResult[0], "true");
		assertEquals(getResult[1], SearchMethodSimpleTest.SEARCH_TARGET_VALUE_IN_GROUP1);
	}
	
	@Test
	public void 指定グループに存在しない値を検索して失敗する() throws Exception {
		String[] keyword = new String[]{"さしす"};
		Object[] searchResult = this.okuyamaClient.searchValue(keyword,
																"1", SearchMethodSimpleTest.SEARCH_TARGET_GROUP2);
		assertEquals(searchResult[0], "false");
	}

}
