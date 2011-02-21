package okuyama.imdst.util;

import java.io.ByteArrayOutputStream;

import com.sun.mail.util.BASE64DecoderStream;
import com.sun.mail.util.BASE64EncoderStream;

/**
 * 最終保存媒体がMemory時のConverter.<br>
 * Encode仕様:Key=BASE64でデコード後、バイト配列で返す
 *            Value=バイト配列で返す
 *
 * Decode仕様:Key=BASE64でエンコード後、Stringで返す
 *            Value=Stringで返す
 *
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class MemoryModeCoreValueCnv implements ICoreValueConverter {


    /**
     * 引数のObjectはBase64でエンコード後のString
     * 返却値はCoreMapKey
     *
     */
    public Object convertEncodeKey(Object key) {
        if (key == null) return null;
        //System.out.println("Encode Key[" + key + "]");
        return new CoreMapKey(((String)key).getBytes());
    }


    /**
     * 引数のObjectはBase64でエンコードされた文字とメタ情報の連結文字列
     * 返却値はbyte配列
     *
     */
    public Object convertEncodeValue(Object value) {
        if (value == null) return null;
        boolean tagMatch = false;

        byte[] registerData = null;

        String[] timeSepSplit = ((String)value).split("!");

        String timeVal = timeSepSplit[1];

        byte[] timeValBytes = timeVal.getBytes();

        String[] metaSep = timeSepSplit[0].split(",");

        String metaVal = null;
        byte[] metaValBytes = null;

        String realVal = metaSep[0];
        if (metaSep.length > 1) {
            metaVal = metaSep[1];
            metaValBytes = metaVal.getBytes();
        }

        // 真のValueの部分だけバイト化
        byte[] realValBytes = realVal.getBytes();

        // タグチェック
        int checkSize = realValBytes.length;
        if (checkSize > ImdstDefine.saveKeyMaxSize) {
            checkSize = ImdstDefine.saveKeyMaxSize;
        }

        // Tagの場合は":"が含まれるのでそれを調べる
        for (int realValBytesIdx = 0; realValBytesIdx < checkSize; realValBytesIdx++) {

            // ":"(58)チェック
            if (realValBytes[realValBytesIdx] == 58) {
                tagMatch = true;
                break;
            }
        }

        // タグの場合はここで終了
        if (tagMatch) {

            byte[] tagBytes = ((String)value).getBytes();
            
            byte[] returnTagBytes = new byte[tagBytes.length + 1];
            for (int i = 0; i < tagBytes.length; i++) {
                returnTagBytes[i+1] = tagBytes[i];
            }

            return new MemoryDataEntry(SystemUtil.valueCompress(tagBytes), null, null, true, true);
        }

        byte[] realDecodeValBytes = BASE64DecoderStream.decode(realValBytes);

        // 圧縮の場合
        if (ImdstDefine.saveValueCompress) {
            byte[] compressData = SystemUtil.valueCompress(realDecodeValBytes);

            // 圧縮したがデータが大きくなった場合は圧縮していない元データを保存する
            if (compressData.length < realDecodeValBytes.length) {

                // 圧縮データ
                return new MemoryDataEntry(compressData, timeValBytes, metaValBytes, false, true);
            }
        }

        // 非圧縮データ
        return new MemoryDataEntry (realDecodeValBytes, timeValBytes, metaValBytes, false, false);

        //System.out.println("-------------------");
        //System.out.println(((byte[])",".getBytes())[0]); 44 
        //System.out.println(((byte[])"!".getBytes())[0]); 33
        //System.out.println(registerData.length + " => " + SystemUtil.valueCompress(registerData).length);
    }
    

    /**
     * 引数のObjectはCoreMapKey
     * 返却値は文字列
     */
    public Object convertDecodeKey(Object key) {
        if (key == null) return null;
        return new String(((CoreMapKey)key).getDatas());
    }



    /**
     * 引数のObjectはBase64でエンコードされた文字とメタ情報の連結文字列のbyte配列
     * 返却値はbyte配列を文字列に変更したString
     *
     */
    public Object convertDecodeValue(Object value) {
        if (value == null) return null;

        MemoryDataEntry memoryDataEntry = (MemoryDataEntry)value;

        // タグの場合はそのまま返却
        if (memoryDataEntry.isTag) {

            return new String(SystemUtil.valueDecompress(memoryDataEntry.value));
        }

        // Tagではない
        StringBuilder workBuf = new StringBuilder(memoryDataEntry.value.length + 30);

        // valueを復元
        if (memoryDataEntry.value != null) {
            if (memoryDataEntry.compress) {
                workBuf.append(new String(BASE64EncoderStream.encode(SystemUtil.valueDecompress(memoryDataEntry.value))));
            } else {
                workBuf.append(new String(BASE64EncoderStream.encode(memoryDataEntry.value)));
            }
        }

        // 期限(Flags)を復元
        if (memoryDataEntry.metaData != null) {
            workBuf.append(",");
            workBuf.append(new String(memoryDataEntry.metaData));
        }

        // 登録日時を復元
        if (memoryDataEntry.registerDate != null) {
            workBuf.append("!");
            workBuf.append(new String(memoryDataEntry.registerDate));
        }

        return workBuf.toString();
    }


    // メモリ上に保存するインナークラス
    class MemoryDataEntry {

        protected byte[] value = null;

        protected byte[] registerDate = null;

        protected byte[] metaData = null;

        protected boolean isTag = false;

        protected boolean compress = false;


        protected MemoryDataEntry (byte[] value, byte[] registerDate, byte[] metaData, boolean isTag, boolean compress) {
            this.value = value;
            this.registerDate = registerDate;
            this.metaData = metaData;
            this.isTag = isTag;
            this.compress = compress;
        }
    }
}
