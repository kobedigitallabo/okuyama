package okuyama.imdst.util.io;

import java.io.*;
import java.net.*;
import java.util.*;

import okuyama.imdst.util.*;

/**
 * IOのRead系のラッパー.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class CustomReader {

    private InputStream is = null;

    private BufferedInputStream bis = null;

    private ByteArrayOutputStream bos = null;

    private byte[] b = new byte[1];


    public CustomReader(InputStream is) {

        this.is = is;
        if (ImdstDefine.bigDataTransfer == false) {
            this.bos = new ByteArrayOutputStream(1024*8);
            this.bis = new BufferedInputStream(this.is, 1024*8);
        } else {
            this.bos = new ByteArrayOutputStream(1024*1024);
            this.bis = new BufferedInputStream(this.is, 1024*256);
        }
    }


    private String readLine4Big() throws Exception {
        byte[] b = new byte[1024*256];

        int i = 0;
        int readLen =0;
        while ((readLen = bis.read(b, 0, 1024*256)) != -1) {

            if (readLen == 1) {
                if (b[0] == 10) {
                    break;
                } else if (b[0] != 13) {
                    this.bos.write(b, 0, readLen);
                }
            } else if (b[readLen - 2] != 13 && b[readLen - 1] != 10) {
                this.bos.write(b, 0, readLen);
            } else if (b[readLen - 1] == 10) {
                if (b[readLen - 2] == 13) {
                    this.bos.write(b, 0, (readLen - 2));
                } else {
                    this.bos.write(b, 0, (readLen - 1));
                }
                break;
            }
        }

        String ret = this.bos.toString();
        this.bos.reset();
        return ret;
    }

    private String readLine4Small() throws Exception {
        this.b[0] = (byte)0;

        int i = 0;
        while (bis.read(this.b, 0, 1) != -1) {

            if (this.b[0] != 13 && this.b[0] != 10) {
                this.bos.write(this.b, 0, 1);
            } else if (this.b[0] == 10) {
                break;
            }
        }

        String ret = this.bos.toString();
        this.bos.reset();
        return ret;
    }


    public String readLine() throws Exception {
        if (ImdstDefine.bigDataTransfer) {
            return readLine4Big();
        } else {
            return readLine4Small();
        }
    }

    public boolean ready() throws Exception {
        if(this.bis.available() > 0) return true;
        return false;
    }


    public int read(byte[] buf) throws Exception {

        for (int i= 0; i < buf.length; i++) {
            bis.read(buf, i, 1);
        }

        return buf.length;

    }


    public void mark(int point) throws Exception {
        this.bis.mark(point);
    }


    public int read() throws Exception {
        return this.bis.read();
    }


    public void reset() throws Exception {
        this.bis.reset();
    }


    public void close() {

        try {
            if (this.bis != null) {
                this.bis.close();
                this.bis = null;
            }

            if (this.is != null) {
                this.is.close();
                this.is = null;
            }
        } catch (Exception e) {
            // 無視
        }
    }
}

