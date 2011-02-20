package okuyama.imdst.util;

import java.io.*;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.zip.*;

import okuyama.base.util.ILogger;
import okuyama.base.util.LoggerFactory;

/**
 * okuyamaが使用する共通的なApiに対してアクセスする.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class SystemUtil {
    
    private static ConcurrentLinkedQueue valueCompresserPool = null;    
    
    private static ConcurrentLinkedQueue valueDecompresserPool = null;
    
    
    public static PrintWriter netDebugPrinter = null;
    
    
    static {
        if (ImdstDefine.saveValueCompress) {
            
            valueCompresserPool = new ConcurrentLinkedQueue();
            valueDecompresserPool = new ConcurrentLinkedQueue();
            
            for (int i = 0; i < ImdstDefine.valueCompresserPoolSize; i++) {
                Deflater compresser = new Deflater();
                compresser.setLevel(ImdstDefine.valueCompresserLevel);
                valueCompresserPool.add(compresser);
                
                Inflater decompresser = new Inflater();
                valueDecompresserPool.add(decompresser);
            }
        } 
    }
    
    
    /**
     * 指定の文字を指定の桁数で特定文字列で埋める.<br>
     *
     * @param data
     * @param fixSize
     */
    public static String fillCharacter(String data, int fixSize) {
        return fillCharacter(data, fixSize, 38);
    }


    /**
     * 指定の文字を指定の桁数で特定文字列で埋める.<br>
     *
     * @param data
     * @param fixSize
     * @param fixByte
     */
    public static String fillCharacter(String data, int fixSize, int fillByte) {
        StringBuilder writeBuf = new StringBuilder(data);

        int valueSize = data.length();

        // 渡されたデータが固定の長さ分ない場合は足りない部分を補う
        byte[] appendDatas = new byte[fixSize - valueSize];

        for (int i = 0; i < appendDatas.length; i++) {
            appendDatas[i] = new Integer(fillByte).byteValue();
        }

        writeBuf.append(new String(appendDatas));
        return writeBuf.toString();
    }


    /**
     * 指定された値を時間に置き換えた場合に現在時間を過ぎているかをチェックする.<br>
     *
     * @param expirTimeStr
     * @return boolean
     */
    public static boolean expireCheck(String expirTimeStr) {
        boolean ret = true;

        try {
            // 数値変換出来ない場合はエラー
            if (!expirTimeStr.trim().equals("0")) {

                long expireTime = Long.parseLong(expirTimeStr);

                if (expireTime <= JavaSystemApi.currentTimeMillis) ret = false;
            }
        } catch (NumberFormatException e) {
            ret = false;
        }

        return ret;
    }


    /**
     * 指定された値を時間に置き換えた場合に現在時間を過ぎているかをチェックする.<br>
     * 引数のoverTimeで指定したミリ秒を過ぎている場合のみfalseを返す.<br>
     *
     * @param expirTimeStr
     * @param overTime
     * @return boolean
     */
    public static boolean expireCheck(String expirTimeStr, long overTime) {
        boolean ret = true;

        try {
            // 数値変換出来ない場合はエラー
            if (!expirTimeStr.trim().equals("0")) {

                long expireTime = Long.parseLong(expirTimeStr);

                if ((expireTime + overTime) <= JavaSystemApi.currentTimeMillis) ret = false;
            }
        } catch (NumberFormatException e) {
            ret = false;
        }

        return ret;
    }

    
    /**
     * 圧縮処理.<br>
     *
     * @param src
     * @return byte[]
     */
    public static byte[] valueCompress(byte[] src) {
        if (!ImdstDefine.saveValueCompress) return src;


        Deflater compresser = (Deflater)valueCompresserPool.poll();
        if (compresser == null) {
            compresser = new Deflater();
            compresser.setLevel(ImdstDefine.valueCompresserLevel);
        }

        ByteArrayOutputStream compos = new ByteArrayOutputStream(src.length);

        try {
            compresser.setInput(src);
            compresser.finish();

            byte[] buf = new byte[ImdstDefine.valueCompresserCompressSize];
            int count = 0;
            while (!compresser.finished()) {
                count = compresser.deflate(buf);
                compos.write(buf, 0, count);
            }
            return compos.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
            compresser = null;
        } finally {
            if (compresser != null) { 
                compresser.reset();
                valueCompresserPool.add(compresser);
            }
        }
        return null;
    }
    
    
    /**
     * 圧縮解除処理.<br>
     *
     * @param src
     * @return byte[]
     */
    public static byte[] valueDecompress(byte[] src) {
        if (!ImdstDefine.saveValueCompress) return src;
        Inflater decompresser = (Inflater)valueDecompresserPool.poll();
        if (decompresser == null) {
            decompresser = new Inflater();
        }
        
        ByteArrayOutputStream decompos = new ByteArrayOutputStream();
        try {
            
            decompresser.setInput(src);

            byte[] buf = new byte[ImdstDefine.valueCompresserCompressSize];
            int count = 0;
            while (!decompresser.finished()) {
                count = decompresser.inflate(buf);
                decompos.write(buf, 0, count);
            }
        } catch (Exception e) {
            e.printStackTrace();
            decompresser = null;
        } finally {
            if (decompresser != null) {
                decompresser.reset();
                valueDecompresserPool.add(decompresser);
            }
        }
        return decompos.toByteArray();
    }
    
    
    /**
     * -debugオプションを利用した際に、標準出力への出力を行う.<br>
     *
     * @param String outputStr
     */
    public static void debugLine(String outputStr) {
        if (StatusUtil.getDebugOption()) {
            StringBuilder strBuf = new StringBuilder(100);
            strBuf.append(new Date().toString());
            strBuf.append(" DebugLine \"");
            strBuf.append(outputStr);
            strBuf.append("\"");
            if (SystemUtil.netDebugPrinter == null) { 
                System.out.println(strBuf.toString());
            } else {
                try {
                    SystemUtil.netDebugPrinter.println(strBuf.toString());
                    SystemUtil.netDebugPrinter.flush();
                } catch (Exception e) {
                    
                    StatusUtil.setDebugOption(false);
                    SystemUtil.netDebugPrinter = null;
                }
            }
            
            strBuf = null;
        }
    }
}
