package org.apache.hadoop.security.scram;

import java.security.Provider;
import java.security.Security;

import org.apache.hadoop.security.scram.ScramSaslServer.ScramSaslServerFactory;

public class ScramSaslServerProvider extends Provider {

  private static final long serialVersionUID = 1L;

  protected ScramSaslServerProvider() {
    super("SASL/SCRAM Server Provider", 1.0, "SASL/SCRAM Server Provider");
    for (ScramMechanism mechanism : ScramMechanism.values())
      super.put("SaslServerFactory." + mechanism.mechanismName(), ScramSaslServerFactory.class.getName());
  }

  public static void initialize() {
    Security.addProvider(new ScramSaslServerProvider());
  }
}