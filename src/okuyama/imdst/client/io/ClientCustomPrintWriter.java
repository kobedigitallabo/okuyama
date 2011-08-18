package okuyama.imdst.client.io;


import java.io.*;
import java.util.*;

/**
 * OkuyamaClientが利用するPrintWriterのラッパー<br>
 * デバッグなどの機能を持つ<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class ClientCustomPrintWriter extends PrintWriter {


    private boolean debug = false;


    public ClientCustomPrintWriter(File file) throws FileNotFoundException {
        super(file);
    }

    public ClientCustomPrintWriter(File file, String csn) throws FileNotFoundException, UnsupportedEncodingException {
        super(file, csn);
    }

    public ClientCustomPrintWriter(OutputStream out) {
        super(out);
    }

    public ClientCustomPrintWriter(OutputStream out, boolean autoFlush) {
        super(out, autoFlush);
    }

    public ClientCustomPrintWriter(String fileName) throws FileNotFoundException {
        super(fileName);
    }

    public ClientCustomPrintWriter(String fileName, String csn) throws FileNotFoundException, UnsupportedEncodingException {
        super(fileName, csn);
    }

    public ClientCustomPrintWriter(Writer out) throws FileNotFoundException {
        super(out);
    }

    public ClientCustomPrintWriter(Writer out, boolean autoFlush) throws FileNotFoundException, UnsupportedEncodingException{
        super(out, autoFlush);
    }


    public void setDebugLine(boolean debug) {
        this.debug = debug;
    }


    public void print(boolean b) {
        if (debug) System.out.println(b);
        super.print(b);
    }

    public void print(char c) {
        if (debug) System.out.println(c);
        super.print(c);
    }

    public void print(char[] s) {
        if (debug) System.out.println(s);
        super.print(s);
    }

    public void print(double d) {
        if (debug) System.out.println(d);
        super.print(d);
    }

    public void print(float f) {
        if (debug) System.out.println(f);
        super.print(f);
    }

    public void print(int i) {
        if (debug) System.out.println(i);
        super.print(i);
    }

    public void print(long l) {
        if (debug) System.out.println(l);
        super.print(l);
    }

    public void print(Object obj) {
        if (debug) System.out.println(obj);
        super.print(obj);
    }

    public void print(String s) {
        if (debug) System.out.println(s);
        super.print(s);
    }


    public void println(boolean x) {
        if (debug) System.out.println(x);
        super.println(x);
    }

    public void println(char x) {
        if (debug) System.out.println(x);
        super.println(x);
    }

    public void println(char[] x) {
        if (debug) System.out.println(x);
        super.println(x);
    }

    public void println(double x) {
        if (debug) System.out.println(x);
        super.println(x);
    }

    public void println(float x) {
        if (debug) System.out.println(x);
        super.println(x);
    }

    public  void println(int x) {
        if (debug) System.out.println(x);
        super.println(x);
    }

    public void println(long x) {
        if (debug) System.out.println(x);
        super.println(x);
    }

    public void println(Object x) {
        if (debug) System.out.println(x);
        super.println(x);
    }

    public void println(String x) {
        if (debug) System.out.println(x);
        super.println(x);
    }
}
