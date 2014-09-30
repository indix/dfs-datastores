package com.backtype.hadoop.pail;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.util.List;

/**
* Created by rajatv on 9/29/14.
*/ //Supporting class for listLocatedStatus
class MultiPathFilter implements PathFilter {

  protected static final PathFilter hiddenFileFilter = new PathFilter(){
    public boolean accept(Path p){
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

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

  public static PathFilter getHiddenFileFilter() {
    return hiddenFileFilter;
  }
}
