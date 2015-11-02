package test.junit.util;

import static org.junit.Assert.*;

import java.util.Map;

import okuyama.imdst.util.StatusUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * StatusUtilクラスのテスト。
 * @author s-ito
 *
 */
public class StatusUtilTest {
	
	@Before
	public void setUp() throws Exception {
		StatusUtil.clearNodeDataSize();
	}

	@After
	public void tearDown() throws Exception {
		StatusUtil.clearNodeDataSize();
	}

	@Test
	public void DataNode3がもつデータのサイズを取得する() {
		StatusUtil.setNodeDataSize(Integer.valueOf(0), new String[]{"all=400"});
		StatusUtil.setNodeDataSize(Integer.valueOf(1), new String[]{"all=300"});
		StatusUtil.setNodeDataSize(Integer.valueOf(2), new String[]{"all=600"});
		
		Map<?, ?> result = StatusUtil.getNodeDataSize();
		
		assertNotNull(result);
		assertEquals(result.get("all"), 1300L);
	}
	
	@Test
	public void DataNode3つのうち1つデータサイズ計算しない場合はデータサイズの計算を行わない() {
		StatusUtil.setNodeDataSize(Integer.valueOf(0), new String[]{"all=400"});
		StatusUtil.setNodeDataSize(Integer.valueOf(1), new String[]{""});
		StatusUtil.setNodeDataSize(Integer.valueOf(2), new String[]{"all=600"});
		
		Map<?, ?> result = StatusUtil.getNodeDataSize();
		
		assertNotNull(result);
		assertEquals(result.get("all"), -1L);
	}
	
	@Test
	public void DataNode3つ全てデータサイズ計算しない場合はデータサイズの計算を行わない() {
		StatusUtil.setNodeDataSize(Integer.valueOf(0), new String[]{""});
		StatusUtil.setNodeDataSize(Integer.valueOf(1), new String[]{""});
		StatusUtil.setNodeDataSize(Integer.valueOf(2), new String[]{""});
		
		Map<?, ?> result = StatusUtil.getNodeDataSize();
		
		assertNotNull(result);
		assertEquals(result.get("all"), -1L);
	}

}
