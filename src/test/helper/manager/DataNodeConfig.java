package test.helper.manager;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DataNodeのpropertiesの管理クラス。
 * @author s-ito
 *
 */
public class DataNodeConfig extends NodeConfig {

	private Logger logger = Logger.getLogger(DataNodeConfig.class.getName());
	/**
	 * ストレージモード。
	 * @author s-ito
	 *
	 */
	public enum StorageMode {
		/**
		 * 完全メモリ。
		 */
		ALL_MEMORY,
		/**
		 * 永続型メモリ。
		 */
		MEMORY,
		/**
		 * Key=メモリ/Value=ファイル。
		 */
		VALUE_FILE,
		/**
		 * 完全ファイル。
		 */
		ALL_FILE,
		/**
		 * 不正な組み合わせ。
		 */
		ERROR
	}

	/**
	 * 圧縮モード。
	 * @author s-ito
	 *
	 */
	public enum CompressionMode {
		/**
		 * 圧縮無し。
		 */
		NOTHING,
		/**
		 * 高速低圧縮。
		 */
		LOW,
		/**
		 * 低速高圧縮。
		 */
		HIGHLY
	}

	/**
	 * コンストラクタ。
	 * @param prop - Nodeのproperties本体。
	 * @param jobName - NodeのJob名。
	 */
	public DataNodeConfig(Properties prop, String jobName) {
		super(prop, jobName);
	}

	@Override
	public void validate() throws NodeConfigValidationException {
		String key = null;
		try {
			key = this.getNodeJobName() + ".JobClass";
			// Nodeのジョブをチェック
			String jobClassName = this.prop.getProperty(key);
			if (jobClassName == null || jobClassName.isEmpty()) {
				throw new NodeConfigValidationException(key, "NodeのJobが指定されていません");
			}
			// 通常ポートが設定されているか確認
			key = this.getNodeJobName() + ".Init";
			Integer.valueOf(this.prop.getProperty(key));
			// その他のJobチェック
			key = "joblist";
			String[] jobs = NodeConfig.getJob("okuyama.imdst.job.ServerManagedJob", this.prop);
			if (jobs.length <= 0) {
				throw new NodeConfigValidationException(key, "okuyama.imdst.job.ServerManagedJobのJobが存在しません");
			}
			// 必要なHelperが設定されているか確認
			key = "helperlist";
			String[] helpers = NodeConfig.getHelper("okuyama.imdst.helper.KeyManagerHelper", this.prop);
			if (helpers.length <= 0) {
				throw new NodeConfigValidationException(key, "okuyama.imdst.helper.KeyManagerHelperのHelperが存在しません");
			}
			helpers = NodeConfig.getHelper("okuyama.imdst.helper.KeyManagerHelper", this.prop);
			if (helpers.length <= 0) {
				throw new NodeConfigValidationException(key, "okuyama.imdst.helper.KeyManagerHelperのHelperが存在しません");
			}
			// 管理ポートが設定されているか確認
			// ストレージモードの組み合わせをチェック
			// データ保存先の設定を必要に応じてチェック
		} catch (NodeConfigValidationException e) {
			throw e;
		} catch (Throwable e) {
			if (key != null) {
				throw new NodeConfigValidationException(key, e);
			}
		}
	}

	/**
	 * ジャーナルファイルを取得する。
	 * @see <p>NodeのpropertiesファイルのNodeジョブ名.Option</p>
	 * @return ジャーナルファイルのパス。設定されていない場合はnullを返す。
	 */
	public String getJournalFile() {
		String optionStr = this.prop.getProperty(this.getNodeJobName() + ".Option");
		if (optionStr == null || optionStr.isEmpty()) {
			this.logger.warning("ジャーナルファイルが指定されていません");
			return null;
		}
		String[] option = optionStr.split(",");
		if (option.length < 2) {
			this.logger.warning("ジャーナルファイルが指定されていません");
			return null;
		}
		return option[1];
	}

	/**
	 * スナップショットファイルを取得する。
	 * @see <p>NodeのpropertiesファイルのNodeジョブ名.Option</p>
	 * @return スナップショットファイルのパス。
	 */
	public String getSnapshotFile() {
		String optionStr = this.prop.getProperty(this.getNodeJobName() + ".Option");
		if (optionStr == null || optionStr.isEmpty()) {
			this.logger.warning("スナップショットファイルが指定されていません");
			return null;
		}
		String[] option = optionStr.split(",");
		return option[0] + ".obj";
	}

	/**
	 * ストレージモードを取得する。
	 * @see <p>NodeのpropertiesファイルのNodeジョブ名.memoryMode、Nodeジョブ名.dataMemory、Nodeジョブ名.keyMemory</p>
	 * @return DataNodeのストレージモード。
	 */
	public StorageMode getStorageMode() {
		Properties prop = this.prop;
		String memoryModeStr = prop.getProperty(this.getNodeJobName() + ".memoryMode");
		String dataMemoryStr = prop.getProperty(this.getNodeJobName() + ".dataMemory");
		String keyMemoryStr = prop.getProperty(this.getNodeJobName() + ".keyMemory");
		try {
			boolean memoryMode = Boolean.valueOf(memoryModeStr);
			boolean dataMemory = Boolean.valueOf(dataMemoryStr);
			boolean keyMemory = Boolean.valueOf(keyMemoryStr);
			if (memoryMode && dataMemory && keyMemory) {
				return StorageMode.ALL_MEMORY;
			} else if (!memoryMode && dataMemory && keyMemory) {
				return StorageMode.MEMORY;
			} else if (!memoryMode && !dataMemory && keyMemory) {
				return StorageMode.VALUE_FILE;
			} else if (!(memoryMode || dataMemory || keyMemory)) {
				return StorageMode.ALL_FILE;
			}
			this.logger.warning("ストレージモードの設定が不正です");
		} catch (Exception e) {
			this.logger.log(Level.WARNING, "ストレージモードの設定が不正です", e);
		}
		return StorageMode.ERROR;
	}

	/**
	 * DataNodeに保存される予想データサイズを取得する。
	 * @see <p>NodeのpropertiesファイルのNodeジョブ名.keySize</p>
	 * @return DataNodeに保存される予想データサイズ。
	 */
	public int getAnticipatedSize() {
		String keySizeStr = this.prop.getProperty(this.getNodeJobName() + ".keySize");
		if (keySizeStr == null || keySizeStr.isEmpty()) {
			return 0;
		}
		try {
			return Integer.valueOf(keySizeStr);
		} catch (Exception e) {
			this.logger.log(Level.WARNING, "予想データサイズの設定が不正です", e);
		}
		return 0;
	}

	/**
	 * 仮想メモリを利用するタイミングを取得する。
	 * @see <p>NodeのpropertiesファイルのNodeジョブ名.memoryLimitSize</p>
	 * @return 仮想メモリの利用を開始するNodeのメモリ使用率。-1なら仮想メモリ機能を利用しない。単位は%。
	 */
	public int getMemoryLimit() {
		Properties prop = this.prop;
		String memoryLimitSizeStr = prop.getProperty(this.getNodeJobName() + ".memoryLimitSize");
		String dataMemoryStr = prop.getProperty(this.getNodeJobName() + ".dataMemory");
		String keyMemoryStr = prop.getProperty(this.getNodeJobName() + ".keyMemory");
		if (memoryLimitSizeStr == null || memoryLimitSizeStr.isEmpty()) {
			return -1;
		}
		try {
			boolean dataMemory = Boolean.valueOf(dataMemoryStr);
			boolean keyMemory = Boolean.valueOf(keyMemoryStr);
			if (dataMemory && keyMemory) {
				return Integer.valueOf(memoryLimitSizeStr);
			}
		} catch (Exception e) {
			this.logger.log(Level.WARNING, "仮想メモリを利用するタイミングの設定が不正です", e);
		}
		return -1;
	}

	/**
	 * 仮想メモリとして使うディレクトリのパスを取得する。
	 * @see <p>NodeのpropertiesファイルのNodeジョブ名.virtualStoreDirs</p>
	 * @return 仮想メモリとして使うディレクトリのパス。仮想メモリを使わない場合はnullを返す。
	 */
	public String[] getVirtualMemory() {
		if (this.getMemoryLimit() < 0) {
			return null;
		}
		String dirsStr = this.prop.getProperty(this.getNodeJobName() + ".virtualStoreDirs");
		if (dirsStr == null || dirsStr.isEmpty()) {
			return null;
		}
		return dirsStr.split(",");
	}

	/**
	 * 完全ファイルモード時にKeyが保存されるディレクトリのパスを取得する。
	 * @see <p>NodeのpropertiesファイルのNodeジョブ名.keyStoreDirs</p>
	 * @return 完全ファイルモード時にKeyが保存されるディレクトリのパス。使用しない場合はnullを返す。
	 */
	public String[] getKeyDirectory() {
		if (!(StorageMode.ALL_FILE.equals(this.getStorageMode()))) {
			return null;
		}
		String dirsStr = this.prop.getProperty(this.getNodeJobName() + ".keyStoreDirs");
		if (dirsStr == null || dirsStr.isEmpty()) {
			return null;
		}
		return dirsStr.split(",");
	}

	/**
	 * Valueをファイルに保存する場合に使うキャッシュのパスを取得する。
	 * @see <p>NodeのpropertiesファイルのNodeジョブ名.cacheFilePath</p>
	 * @return Valueをファイルに保存する場合に使うキャッシュのパス。使用しない場合はnullを返す。
	 */
	public String[] getCache() {
		StorageMode mode = this.getStorageMode();
		if (!(StorageMode.ALL_FILE.equals(mode) || StorageMode.VALUE_FILE.equals(mode))) {
			return null;
		}
		String cacheStr = this.prop.getProperty(this.getNodeJobName() + ".keyStoreDirs");
		if (cacheStr == null || cacheStr.isEmpty()) {
			return null;
		}
		return cacheStr.split(",");
	}

	/**
	 * データ保存時にシリアライズするか確認する。
	 * @see <p>NodeのpropertiesファイルのDataSaveMapType</p>
	 * @return シリアライズする場合はtrueを返す。
	 */
	public boolean isSerialize() {
		String str = this.prop.getProperty("DataSaveMapType");
		if (str == null || str.isEmpty() || !("serialize".equals(str))) {
			return false;
		}
		return true;
	}

	/**
	 * データの(デ)シリアライズに使用するクラスの名前を取得する。
	 * @see <p>NodeのpropertiesファイルのSerializerClassName</p>
	 * @return データの(デ)シリアライズに使用するクラスの名前。使用しない場合はnullを返す。
	 */
	public String getSerializeClassName() {
		if (!(this.isSerialize())) {
			return null;
		}
		String str = this.prop.getProperty("SerializerClassName");
		if (str == null || str.isEmpty()) {
			return null;
		}
		return str;
	}

	/**
	 * Value1つの制限サイズを取得する。
	 * @see <p>NodeのpropertiesファイルのSaveDataMemoryStoreLimitSize</p>
	 * @return Value1つの制限サイズ。制限しない場合は-1を返す。
	 */
	public int getValueLimitSize() {
		StorageMode mode = this.getStorageMode();
		if (StorageMode.ALL_FILE.equals(mode) || StorageMode.VALUE_FILE.equals(mode)) {
			return -1;
		}
		String str = this.prop.getProperty("SaveDataMemoryStoreLimitSize");
		try {
			int result = Integer.valueOf(str);
			return result <= 0 ? -1 : result;
		} catch (Exception e) {
			this.logger.log(Level.WARNING, "Value1つの制限サイズの設定が不正です", e);
		}
		return -1;
	}

	/**
	 * ログを毎回出力するか確認する。
	 * @see <p>NodeのpropertiesファイルのDataSaveTransactionFileEveryCommit</p>
	 * @return ログを毎回出力する場合はtrueを返す。
	 */
	public boolean isDataSaveTransactionFileEveryCommit() {
		if (StorageMode.ALL_MEMORY.equals(this.getStorageMode())) {
			return false;
		}
		String str = this.prop.getProperty("DataSaveTransactionFileEveryCommit");
		try {
			return Boolean.valueOf(str);
		} catch (Exception e) {
			this.logger.log(Level.WARNING, "DataSaveTransactionFileEveryCommitの設定が不正です", e);
		}
		return false;
	}

	/**
	 * 共有データファイルへの変更の最大書き込みプール数(データ数)を取得する。
	 * @see <p>NodeのpropertiesファイルのShareDataFileWriteDelayFlg、ShareDataFileMaxDelayCount</p>
	 * @return 共有データファイルへの変更の最大書き込みプール数(データ数)。使用しない場合は0以下の値を返す。
	 */
	public int getShareDataFileMaxDelayCount() {
		Properties prop = this.prop;
		String flgStr = prop.getProperty("ShareDataFileWriteDelayFlg");
		if (flgStr == null || flgStr.isEmpty()) {
			return 0;
		}
		try {
			String str = prop.getProperty("ShareDataFileMaxDelayCount");
			if (str == null) {
				return 0;
			}
			return Integer.valueOf(str);
		} catch (Exception e) {
			this.logger.log(Level.WARNING, "共有データファイルへの変更の最大書き込みプール数の設定が不正です", e);
		}
		return 0;
	}

	/**
	 * 圧縮モードを取得する。
	 * @see <p>NodeのpropertiesファイルのSaveDataCompress、SaveDataCompressType</p>
	 * @return 圧縮モード。
	 */
	public CompressionMode getCompresstionMode() {
		Properties prop = this.prop;
		String str = prop.getProperty("SaveDataCompress");
		if (str == null || str.isEmpty()) {
			return CompressionMode.NOTHING;
		}
		try {
			boolean isCompression = Boolean.valueOf(str);
			if (isCompression) {
				int mode = Integer.valueOf(prop.getProperty("SaveDataCompressType"));
				switch (mode) {
				case 1:return CompressionMode.LOW;
				case 9:return CompressionMode.HIGHLY;
				}
				this.logger.warning("圧縮の設定が不正です");
			}
		} catch (Exception e) {
			this.logger.log(Level.WARNING, "圧縮の設定が不正です", e);
		}
		return CompressionMode.NOTHING;
	}


}
