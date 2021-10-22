package org.apache.hadoop.security.scram;

public class ScramCredential {

  private final byte[] salt;
  private final byte[] serverKey;
  private final byte[] storedKey;
  private final int iterations;

  public ScramCredential(byte[] salt, byte[] storedKey, byte[] serverKey, int iterations) {
    this.salt = salt;
    this.serverKey = serverKey;
    this.storedKey = storedKey;
    this.iterations = iterations;
  }

  public byte[] salt() {
    return salt;
  }

  public byte[] serverKey() {
    return serverKey;
  }

  public byte[] storedKey() {
    return storedKey;
  }

  public int iterations() {
    return iterations;
  }
}
