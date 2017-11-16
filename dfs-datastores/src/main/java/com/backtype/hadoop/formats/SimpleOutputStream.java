package com.backtype.hadoop.formats;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;


public class SimpleOutputStream implements RecordOutputStream {

    private OutputStream _raw;
    private DataOutputStream _os;

    public SimpleOutputStream(OutputStream os) {
        _raw = os;
        _os = new DataOutputStream(os);
    }

    public OutputStream getWrappedStream() {
        return _raw;
    }

    public long writeRaw(byte[] record) throws IOException {
        return writeRaw(record, 0, record.length);
    }

    public long writeRaw(byte[] record, int start, int length) throws IOException {
        _os.writeInt(length);
        _os.write(record, start, length);
        return _os.size();
    }

    public void close() throws IOException {
        _os.close();
    }

    @Override
    public void flush() throws IOException {
        // NOT DOING ANYTHING TO LEAVE IT AT STATUS-QUO
    }
}
