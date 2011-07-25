package okuyama.imdst.util.io;

import java.io.*;
import java.net.*;
import java.util.*;

import okuyama.imdst.util.*;

public class CustomBufferedWriter extends BufferedWriter {

    private FileOutputStream fos = null;

    public CustomBufferedWriter(Writer out, FileOutputStream fos) {
        super(out);
        this.fos = fos;
    }

    public CustomBufferedWriter(Writer out, int sz, FileOutputStream fos) {
        super(out, sz);
        this.fos = fos;
    }

    public FileDescriptor getFD() throws Exception {
        try {
            return this.fos.getFD();
        } catch (Exception e) {

            throw e;
        }
    }
}

