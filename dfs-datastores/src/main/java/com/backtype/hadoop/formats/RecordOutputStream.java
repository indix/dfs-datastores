package com.backtype.hadoop.formats;

import java.io.IOException;


public interface RecordOutputStream {
    long writeRaw(byte[] record) throws IOException;
    long writeRaw(byte[] record, int start, int length) throws IOException;
    void close() throws IOException;
    void flush() throws IOException;
}
