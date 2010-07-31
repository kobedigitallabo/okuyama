package org.imdst.util.io;

import java.io.*;
import java.net.*;
import java.nio.channels.*;
import java.nio.*;

public class ChannelInput extends InputStream {
  
    private SocketChannel targetChannel = null;

    ChannelInput(SocketChannel channel){
        this.targetChannel = channel;
    }

    public int read() throws IOException {

        byte [] buf = new byte[1];

        try {

            int len = read(buf);

            if (len > 0) {

                return buf[0];
            }

        } catch (IOException ie) {
            throw ie;
        }
        return -1;
    }

    public int read(byte tBuf[], int off, int len) throws IOException {
        int rLen = 0;
        ByteBuffer buf = ByteBuffer.allocate(len);
        try {

            rLen = targetChannel.read(buf);

            if (rLen <= 0) return -1;

            buf.flip();
            buf.get(tBuf, off, rLen);
        } catch (IOException ie) {
            throw ie;
        }
        return rLen;
    }
}