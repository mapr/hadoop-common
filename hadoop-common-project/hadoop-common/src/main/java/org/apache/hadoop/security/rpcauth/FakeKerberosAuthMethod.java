package org.apache.hadoop.security.rpcauth;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;

import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;

public class FakeKerberosAuthMethod extends RpcAuthMethod {
  private static final String FAKE_SASL_MECH_NAME = "FAKE-SASL-MECHANISM";
  private static final byte KERBEROS_AUTH_CODE = 81;

  static {
    java.security.Security.addProvider(new FakeSaslProvider());
  }

  static final RpcAuthMethod INSTANCE = new FakeKerberosAuthMethod();
  private FakeKerberosAuthMethod() {
    super((byte) 0xff, "fake", FAKE_SASL_MECH_NAME, AuthenticationMethod.SIMPLE);
  }


  private static final String[] LOGIN_MODULES = {
    "org.apache.hadoop.security.login.PermissiveLoginModule"
  };
  @Override
  public String[] loginModules() {
    return LOGIN_MODULES;
  }

  /** Write to out. */
  @Override
  public void write(DataOutput out) throws IOException {
    out.write(KERBEROS_AUTH_CODE);
  }

  @Override
  public boolean isSasl() {
    return true;
  }

  @Override
  public SaslClient createSaslClient(final Map<String, Object> saslProperties)
      throws IOException {
    return Sasl.createSaslClient(new String[] {mechanismName},
      null, null, null, saslProperties, null);
 }

  @SuppressWarnings("serial")
  public static class FakeSaslProvider extends java.security.Provider {
    public FakeSaslProvider() {
      super("FakeSasl", 1.0, "Fake SASL provider");
      put("SaslClientFactory." + FAKE_SASL_MECH_NAME, FakeSaslClientFactory.class.getName());
    }
  }

  public static class FakeSaslClientFactory implements SaslClientFactory {

    @Override
    public String[] getMechanismNames(Map<String, ?> props) {
      return new String[] { FAKE_SASL_MECH_NAME };
    }

    @Override
    public SaslClient createSaslClient(String[] mechanisms,
        String authorizationId, String protocol, String serverName,
        Map<String, ?> props, CallbackHandler cbh) throws SaslException {
      if ( mechanisms != null ) {
        for ( String mechanism : mechanisms ) {
          if (FAKE_SASL_MECH_NAME.equals(mechanism)) {
            return new FakeSaslClient();
          }
        }
      }
      return null;
    }
  }

  public static class FakeSaslClient implements SaslClient {
    private boolean firstPass = true;

    @Override
    public String getMechanismName() {
      return FAKE_SASL_MECH_NAME;
    }

    @Override
    public boolean hasInitialResponse() {
      return true;
    }

    private static final byte[] FAKE_TOKEN = {'F', 'A', 'K', 'E', 'T', 'O', 'K', 'E', 'N'};
    @Override
    public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
      return FAKE_TOKEN;
    }

    @Override
    public boolean isComplete() {
      if (firstPass) {
        firstPass = false;
        return false;
      }
      return true;
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
      return null;
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
      return null;
    }

    @Override
    public Object getNegotiatedProperty(String propName) {
      return null;
    }

    @Override
    public void dispose() throws SaslException {
    }
  }
}
