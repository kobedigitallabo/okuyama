package okuyama.imdst.util.io;

import java.io.*;
import java.net.*;
import java.nio.channels.*;
import java.nio.*;

/**
 * NewIOのOutputStreamのラッパー.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
class ChannelOutput extends OutputStream {

    private SocketChannel targetChannel;

    ChannelOutput(SocketChannel channel){
        this.targetChannel = channel;
    }


    public void write(int data) throws IOException {

        byte [] buf = new byte[1];
        buf[0] = (byte)(data & 0xff);

        try {

            write(buf, 0, 1);
        } catch(IOException ie ) {
            throw ie;
        }
    }


    public void write(byte data[], int off, int len) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(len);
        buf.put(data, off, len);
        buf.flip();

        try {

            targetChannel.write(buf);
        } catch (IOException ie) {
            throw ie;
        }
    }
}