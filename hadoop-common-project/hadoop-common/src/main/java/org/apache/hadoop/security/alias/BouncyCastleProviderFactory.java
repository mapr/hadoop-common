package org.apache.hadoop.security.alias;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Provider;
import java.util.Optional;

/**
 * Factory to find and initialize BouncyCastleProvider provider
 * based on the libraries that are available in the classpath
 * */
public class BouncyCastleProviderFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(BouncyCastleProviderFactory.class);

  private static final String FIPS_PROVIDER =
      "org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider";
  private static final String GENERAL_PROVIDER =
      "org.bouncycastle.jce.provider.BouncyCastleProvider";

  public static Provider getBouncyCastleProvider() {
    for (String providerClass : new String[]{FIPS_PROVIDER, GENERAL_PROVIDER}) {
      Optional<Class<?>> provider = findProvider(providerClass);
      if (provider.isPresent()) {
        LOG.debug("Using BouncyCastle provider: {}", providerClass);
        return initializeProvider(provider.get());
      }
      LOG.debug("Provider not found: {}", providerClass);
    }
    throw new IllegalStateException(
        "Neither BouncyCastleFipsProvider nor BouncyCastleProvider was found on the classpath.");
  }

  private static Provider initializeProvider(Class<?> providerClass) {
    try {
      return (Provider) providerClass.getDeclaredConstructor().newInstance();
    } catch (Exception ex) {
      throw new IllegalStateException(providerClass.getName() + " cannot be instantiated.", ex);
    }
  }

  private static Optional<Class<?>> findProvider(String className) {
    try {
      return Optional.of(Class.forName(className));
    } catch (ClassNotFoundException e) {
      return Optional.empty();
    }
  }
}
