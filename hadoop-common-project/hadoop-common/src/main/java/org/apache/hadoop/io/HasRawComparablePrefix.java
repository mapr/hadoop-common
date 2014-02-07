package org.apache.hadoop.io;

import org.apache.hadoop.classification.MapRModified;

@MapRModified(summary = "Improve map task performance - bug 5942")
public interface HasRawComparablePrefix {
  public void getPrefix(byte[] dst, int off, int prefixLen);
}
