package com.backtype.hadoop.pail;

import com.backtype.hadoop.formats.RecordInputStream;
import com.backtype.hadoop.formats.RecordOutputStream;
import com.backtype.hadoop.formats.SequenceFileInputStream;
import com.backtype.hadoop.formats.SequenceFileOutputStream;
import com.backtype.support.KeywordArgParser;
import com.backtype.support.Utils;
import com.hadoop.compression.lzo.LzoCodec;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.*;

public class SequenceFileFormat implements PailFormat {
    public static final String TYPE_ARG = "compressionType";
    public static final String CODEC_ARG = "compressionCodec";

    public static final String TYPE_ARG_NONE = "none";
    public static final String TYPE_ARG_RECORD = "record";
    public static final String TYPE_ARG_BLOCK = "block";

    public static final String CODEC_ARG_DEFAULT = "default";
    public static final String CODEC_ARG_GZIP = "gzip";
    public static final String CODEC_ARG_BZIP2 = "bzip2";
    public static final String CODEC_ARG_LZO = "lzo";

    private static final Map<String, CompressionType> TYPES = new HashMap<String, CompressionType>() {{
        put(TYPE_ARG_RECORD, CompressionType.RECORD);
        put(TYPE_ARG_BLOCK, CompressionType.BLOCK);
    }};

    private static final Map<String, CompressionCodec> CODECS = new HashMap<String, CompressionCodec>() {{
        put(CODEC_ARG_DEFAULT, new DefaultCodec());
        put(CODEC_ARG_GZIP, new GzipCodec());
        put(CODEC_ARG_BZIP2, new BZip2Codec());
        put(CODEC_ARG_LZO, new LzoCodec());
    }};

    private String _typeArg;
    private String _codecArg;

    public SequenceFileFormat(Map<String, Object> args) {
        args = new KeywordArgParser()
                .add(TYPE_ARG, null, true, TYPE_ARG_RECORD, TYPE_ARG_BLOCK)
                .add(CODEC_ARG, CODEC_ARG_DEFAULT, false, CODEC_ARG_DEFAULT, CODEC_ARG_GZIP, CODEC_ARG_BZIP2, CODEC_ARG_LZO)
                .parse(args);
        _typeArg = (String) args.get(TYPE_ARG);
        _codecArg = (String) args.get(CODEC_ARG);
    }

    public RecordInputStream getInputStream(FileSystem fs, Path path) throws IOException {
        return new SequenceFileInputStream(fs, path);
    }

    public RecordOutputStream getOutputStream(FileSystem fs, Path path) throws IOException {
        CompressionType type = TYPES.get(_typeArg);
        CompressionCodec codec = CODECS.get(_codecArg);

        if(type==null)
            return new SequenceFileOutputStream(fs, path);
        else
            return new SequenceFileOutputStream(fs, path, type, codec);
    }

    public Class<? extends InputFormat> getInputFormatClass() {
        return SequenceFilePailInputFormat.class;
    }


    public static class SequenceFilePailRecordReader implements RecordReader<PailRecordInfo, BytesWritable> {
        private static Logger LOG = LoggerFactory.getLogger(SequenceFilePailRecordReader.class);
        public static final int NUM_TRIES = 10;

        JobConf conf;
        PailInputSplit split;
        int recordsRead;
        long currentStartOffset = 0;
        Reporter reporter;

        SequenceFileRecordReader<BytesWritable, NullWritable> delegate;
        boolean isBlockCompressed = false;


        public SequenceFilePailRecordReader(JobConf conf, PailInputSplit split, Reporter reporter) throws IOException {
           this.split = split;
           this.conf = conf;
           this.recordsRead = 0;
           this.reporter = reporter;
           LOG.info("Processing pail file " + split.getPath().toString());
            ifBlockCompressed();
           resetDelegate();
        }

        private void resetDelegate() throws IOException {
            this.delegate = new SequenceFileRecordReader<BytesWritable, NullWritable>(conf, split);
           BytesWritable dummyValue = new BytesWritable();
           for(int i=0; i<recordsRead; i++) {
               long posBeforeNext = delegate.getPos();
               delegate.next(dummyValue, NullWritable.get());
               long posAfterNext = delegate.getPos();
               checkForOffsetDuringBlockCompression(posBeforeNext, posAfterNext);
           }
        }

        private void ifBlockCompressed() throws IOException {
            SequenceFile.Reader reader = new SequenceFile.Reader(split.getPath().getFileSystem(conf), split.getPath(), conf);
            this.isBlockCompressed = reader.isBlockCompressed();
            reader.close();
        }

        private void progress() {
            if(reporter!=null) {
                reporter.progress();
            }
        }

        private void checkForOffsetDuringBlockCompression(long posBeforeReading, long posAfterReading) {
            if(isBlockCompressed) {
                if (posAfterReading != posBeforeReading) {
                    recordsRead = 0;
                    currentStartOffset = posBeforeReading;
                } else {
                    recordsRead++;
                }
            } else {
                recordsRead++;
            }
        }

        public boolean next(PailRecordInfo k, BytesWritable v) throws IOException {
            /**
             * There's 2 bugs that happen here, both resulting in indistinguishable EOFExceptions.
             *
             * 1. Random EOFExceptions when reading data off of S3. Usually succeeds on the 2nd try.
             * 2. Corrupted files most likely due to network corruption (which isn't handled by Hadoop/S3 integration).
             *    These always result in error.
             *
             * The strategy is to retry a few times. If it fails every time then we're in case #2, and the best thing we can do
             * is continue on and accept the data loss. If we're in case #1, it'll just succeed.
             */
            for(int i=0; i<NUM_TRIES; i++) {
                try {
                    long posBeforeNext = delegate.getPos();
                    boolean ret = delegate.next(v, NullWritable.get());
                    long posAfterNext = delegate.getPos();
                    checkForOffsetDuringBlockCompression(posBeforeNext, posAfterNext);

                    k.setFullPath(split.getPath().toString());
                    k.setPailRelativePath(split.getPailRelPath());
                    k.setSplitStartOffset(currentStartOffset);
                    k.setRecordsToSkip(recordsRead);
                    return ret;
                } catch(EOFException e) {
                    progress();
                    Utils.sleep(10000); //in case it takes time for S3 to recover
                    progress();
                    //this happens due to some sort of S3 corruption bug.
                    LOG.error("Hit an EOF exception while processing file " + split.getPath().toString() +
                              " with records read = " + recordsRead);
                    resetDelegate();
                }
            }
            //stop trying to read the file at this point and discard the rest of the file
            return false;
        }

        public PailRecordInfo createKey() {
            return new PailRecordInfo();
        }

        public BytesWritable createValue() {
            return new BytesWritable();
        }

        public long getPos() throws IOException {
            return delegate.getPos();
        }

        public void close() throws IOException {
            delegate.close();
        }

        public float getProgress() throws IOException {
            return delegate.getProgress();
        }

    }

    public static class SequenceFilePailInputFormat extends SequenceFileInputFormat<PailRecordInfo, BytesWritable> {
        private Pail _currPail;

        public List<PailInputSplit> withLastSplitInfo(List<PailInputSplit> pailSplits) {
            PailInputSplit lastSplit = pailSplits.get(0);
            lastSplit.setIsLastSplit(true);
            long highestOffset = lastSplit.getStart();
            for (PailInputSplit pailSplit : pailSplits) {
                long offset = pailSplit.getStart();
                if ((offset > highestOffset) && (pailSplit.getPath() == lastSplit.getPath())) {
                    highestOffset = offset;
                    lastSplit.setIsLastSplit(false);
                    lastSplit = pailSplit;
                    lastSplit.setIsLastSplit(true);
                }
            }
            return pailSplits;
        }

        @Override
        public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
            List<PailInputSplit> returnList = new ArrayList<PailInputSplit>();
            Path[] roots = FileInputFormat.getInputPaths(job);
            for(int i=0; i < roots.length; i++) {
                _currPail = new Pail(roots[i].toString());
                InputSplit[] splits = super.getSplits(job, numSplits);
                List<PailInputSplit> ret = new ArrayList<PailInputSplit>();
                Path splitPath = ((FileSplit)splits[0]).getPath();
                PailInputSplit pl;
                for(InputSplit split: splits) {
                    pl = new PailInputSplit(_currPail.getFileSystem(), _currPail.getInstanceRoot(), _currPail.getSpec(), job, (FileSplit) split);
                    if (pl.getPath() != splitPath)
                    {
                        splitPath=pl.getPath();
                        withLastSplitInfo(ret);
                        returnList.addAll(ret);
                        ret = new ArrayList<PailInputSplit>();
                    }
                    ret.add(pl);
                }
                withLastSplitInfo(ret);
                returnList.addAll(ret);
            }

            return returnList.toArray(new InputSplit[returnList.size()]);
        }

        @Override
        protected FileStatus[] listStatus(JobConf job) throws IOException {
            List<Path> paths = PailFormatFactory.getPailPaths(_currPail, job);
            FileSystem fs = _currPail.getFileSystem();
            FileStatus[] ret = new FileStatus[paths.size()];
            for(int i=0; i<paths.size(); i++) {
                ret[i] = fs.getFileStatus(paths.get(i).makeQualified(fs));
            }
            return ret;
        }

        @Override
        public RecordReader<PailRecordInfo, BytesWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
            return new SequenceFilePailRecordReader(job, (PailInputSplit) split, reporter);
        }
    }
}
