package org.apache.hadoop.io;

public interface HasRawComparablePrefix {
  public void getPrefix(byte[] dst, int off, int prefixLen);
}
