package org.apache.hadoop.security.scram;

import javax.security.auth.callback.Callback;

public class ScramCredentialCallback implements Callback {
  private ScramCredential scramCredential;

  public ScramCredential scramCredential() {
    return scramCredential;
  }

  public void scramCredential(ScramCredential scramCredential) {
    this.scramCredential = scramCredential;
  }
}
