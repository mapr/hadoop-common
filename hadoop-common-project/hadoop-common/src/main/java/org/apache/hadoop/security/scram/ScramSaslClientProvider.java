package org.apache.hadoop.security.scram;

import java.security.Provider;
import java.security.Security;

import org.apache.hadoop.security.scram.ScramSaslClient.ScramSaslClientFactory;

public class ScramSaslClientProvider extends Provider {

  private static final long serialVersionUID = 1L;

  protected ScramSaslClientProvider() {
    super("SASL/SCRAM Client Provider", 1.0, "SASL/SCRAM Client Provider");
    for (ScramMechanism mechanism : ScramMechanism.values())
      super.put("SaslClientFactory." + mechanism.mechanismName(), ScramSaslClientFactory.class.getName());
  }

  public static void initialize() {
    Security.addProvider(new ScramSaslClientProvider());
  }
}