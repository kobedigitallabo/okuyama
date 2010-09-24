package okuyama.imdst.util;


import java.util.*;

/**
 * CoreValueMap専用のKeyコンテナ.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class CoreMapKey  {

    byte[] datas = null;

    public CoreMapKey(byte[] datas) {
        this.datas = datas;
    }

    public CoreMapKey(String data) {
        this.datas = data.getBytes();
    }

    public boolean equals(Object tg) {

        if (tg instanceof CoreMapKey) {
            byte[] tgDatas = ((CoreMapKey)tg).getDatas();
            if (tgDatas.length == datas.length) {
                if (datas.length > 0) {
                    if (tgDatas[0]  == datas[0] && tgDatas[(datas.length - 1)]  == datas[(datas.length - 1)]) {
                        for (int i = 1; i < datas.length; i++) {
                            if (tgDatas[i] != datas[i]) return false;
                        }
                    } else {
                        return false;
                    }
                }
            } else {
                return false;
            }
        } else {
            return false;
        }
        return true;
    }

    public byte[] getDatas() {
        return datas;
    }

    public int hashCode() {
        int ret = 1;
        for (int i = 0; i <  datas.length; i++) {
            ret = ret * 31 + datas[i];
        }
        return ret;
    }
}