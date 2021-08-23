package org.apache.hadoop.security.scram;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CredentialCache {

  private final Map<String, Cache<? extends Object>> cacheMap = new HashMap<>();

  public <C> Cache<C> createCache(String mechanism, Class<C> credentialClass) {
    Cache<C> cache = new Cache<C>(credentialClass);
    cacheMap.put(mechanism, cache);
    return cache;
  }

  @SuppressWarnings("unchecked")
  public <C> Cache<C> cache(String mechanism, Class<C> credentialClass) {
    Cache<?> cache = cacheMap.get(mechanism);
    if (cache != null) {
      if (cache.credentialClass() != credentialClass)
        throw new IllegalArgumentException("Invalid credential class " + credentialClass + ", expected " + cache.credentialClass());
      return (Cache<C>) cache;
    } else
      return null;
  }

  public static class Cache<C> {
    private final Class<C> credentialClass;
    private final ConcurrentHashMap<String, C> credentials;

    public Cache(Class<C> credentialClass) {
      this.credentialClass = credentialClass;
      this.credentials = new ConcurrentHashMap<>();
    }

    public C get(String username) {
      return credentials.get(username);
    }

    public C put(String username, C credential) {
      return credentials.put(username, credential);
    }

    public C remove(String username) {
      return credentials.remove(username);
    }

    public Class<C> credentialClass() {
      return credentialClass;
    }
  }
}