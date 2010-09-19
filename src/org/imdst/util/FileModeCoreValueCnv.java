package org.imdst.util;


import com.sun.mail.util.BASE64DecoderStream;
import com.sun.mail.util.BASE64EncoderStream;

/**
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class FileModeCoreValueCnv implements ICoreValueConverter {


    /**
     *
     */
    public Object convertPutKey(Object key) {
        return decode(((String)key).getBytes());
    }

    /**
     *
     */
    public Object convertPutValue(Object value) {
        return value;
    }



    /**
     *
     */
    public Object convertGetKey(Object key) {
        return new String(encode((byte[])key));
    }

    /**
     *
     */
    public Object convertPutValue(Object value) {
        return value;
    }



    private byte[] encode(byte[] datas) {
        return BASE64EncoderStream.encode(datas)
    }

    private byte[] decode(byte[] datas) {
        return BASE64DecoderStream.decode(datas)
    }


}
