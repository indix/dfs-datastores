package com.backtype.hadoop.pail;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
/**
 * Contributed by rajatv on 9/26/14.
 */
public class PailSequenceFileFormat extends SequenceFileFormat {
    public PailSequenceFileFormat(Map<String, Object> args) {
        super(args);
    }
    //Supporting class for listLocatedStatus
    private static class MultiPathFilter implements PathFilter {
        private List<PathFilter> filters;

        public MultiPathFilter(List<PathFilter> filters) {
            this.filters = filters;
        }

        public boolean accept(Path path) {
            for (PathFilter filter : filters) {
                if (!filter.accept(path)) {
                    return false;
                }
            }
            return true;
        }
    }

    // I have decided to replace the entire class. It was too hard to derive and just add
    // locatedliststatus. The dealbreaker was the private member variable.
    // So all the code is the same except for the new function

    public static class PailSequenceFileInputFormat extends SequenceFileInputFormat<Text, BytesWritable> {
        public static final Logger LOG = LoggerFactory.getLogger(PailSequenceFileInputFormat.class);

        private Pail _currPail;

        @Override
        public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
            List<InputSplit> ret = new ArrayList<InputSplit>();
            Path[] roots = FileInputFormat.getInputPaths(job);
            for(int i=0; i < roots.length; i++) {
                _currPail = new Pail(roots[i].toString());
                InputSplit[] splits = super.getSplits(job, numSplits);
                for(InputSplit split: splits) {
                    ret.add(new PailInputSplit(_currPail.getFileSystem(), _currPail.getInstanceRoot(), _currPail.getSpec(), job, (FileSplit) split));
                }
            }
            return ret.toArray(new InputSplit[ret.size()]);
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

        // The killer line is the first one in the function. It replaces
        // getInputPaths in the FileInputFormat implementation.
        // I wish I could override getInputPath and just call the
        // base class function. Unfortunately thats a static function.
        // There is no way to fool FileInputFormat.

        protected LocatedFileStatus[] listLocatedStatus(JobConf job)
                throws IOException {
            List<Path> inputPaths = PailFormatFactory.getPailPaths(_currPail, job);
            if (inputPaths.size() == 0) {
                throw new IOException("No input paths specified in job");
            }


            // creates a MultiPathFilter with the hiddenFileFilter and the
            // user provided one (if any).
            List<PathFilter> filters = new ArrayList<PathFilter>();
            filters.add(hiddenFileFilter);
            PathFilter jobFilter = getInputPathFilter(job);
            if (jobFilter != null) {
                filters.add(jobFilter);
            }
            final PathFilter inputFilter = new MultiPathFilter(filters);

            InputPathProcessor ipp = new InputPathProcessor(job, inputFilter, inputPaths);
            LOG.info("Input paths to process:" + inputPaths.size());
            long t1 = System.nanoTime();
            ipp.compute();
            LOG.info("computeLocatedFileStatus took " + (System.nanoTime() - t1)/Math.pow(10, 9));
            List<LocatedFileStatus> result = ipp.getLocatedFileStatus();
            LOG.info("Total result paths to process : " + result.size());

            return result.toArray(new LocatedFileStatus[result.size()]);
        }

        @Override
        public RecordReader<Text, BytesWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
            return new SequenceFilePailRecordReader(job, (PailInputSplit) split, reporter);
        }
    }
}