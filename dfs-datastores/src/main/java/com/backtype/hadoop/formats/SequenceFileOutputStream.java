package com.backtype.hadoop.formats;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;

import static org.apache.hadoop.io.SequenceFile.Writer.file;
import static org.apache.hadoop.io.SequenceFile.Writer.compression;
import static org.apache.hadoop.io.SequenceFile.Writer.keyClass;
import static org.apache.hadoop.io.SequenceFile.Writer.valueClass;

public class SequenceFileOutputStream implements RecordOutputStream {

    private SequenceFile.Writer _writer;
    private BytesWritable writable = new BytesWritable();

    public SequenceFileOutputStream(FileSystem fs, Path path) throws IOException {
        _writer = SequenceFile.createWriter(fs.getConf(), file(path), keyClass(BytesWritable.class), valueClass(NullWritable.class), compression(CompressionType.NONE));
    }

    public SequenceFileOutputStream(FileSystem fs, Path path, CompressionType type, CompressionCodec codec) throws IOException {
        _writer = SequenceFile.createWriter(fs.getConf(), file(path), keyClass(BytesWritable.class), valueClass(NullWritable.class), compression(type, codec));
    }

    public long writeRaw(byte[] record) throws IOException {
        return writeRaw(record, 0, record.length);
    }

    public long writeRaw(byte[] record, int start, int length) throws IOException {
        writable.set(record, start, length);
        _writer.append(writable, NullWritable.get());
        return _writer.getLength();
    }


    public void close() throws IOException {
        _writer.close();
    }

    @Override
    public void flush() throws IOException {
        _writer.hflush();
    }
}
