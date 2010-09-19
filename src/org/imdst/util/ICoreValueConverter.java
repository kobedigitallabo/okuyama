package org.imdst.util;

import org.batch.lang.BatchException;


/**
 *
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public interface ICoreValueConverter {

    /**
     *
     */
    public Object convertEncodeKey(Object key);

    /**
     *
     */
    public Object convertEncodeValue(Object value);



    /**
     *
     */
    public Object convertDecodeKey(Object key);

    /**
     *
     */
    public Object convertDecodeValue(Object value);

}   