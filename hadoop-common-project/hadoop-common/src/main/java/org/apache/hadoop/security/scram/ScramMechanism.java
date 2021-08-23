package org.apache.hadoop.security.scram;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum ScramMechanism {

  SCRAM_SHA_256("SHA-256", "HmacSHA256", 4096),
  SCRAM_SHA_512("SHA-512", "HmacSHA512", 4096);

  private final String mechanismName;
  private final String hashAlgorithm;
  private final String macAlgorithm;
  private final int minIterations;

  private static final Map<String, ScramMechanism> MECHANISMS_MAP;

  static {
    Map<String, ScramMechanism> map = new HashMap<>();
    for (ScramMechanism mech : values())
      map.put(mech.mechanismName, mech);
    MECHANISMS_MAP = Collections.unmodifiableMap(map);
  }

  ScramMechanism(String hashAlgorithm, String macAlgorithm, int minIterations) {
    this.mechanismName = "SCRAM-" + hashAlgorithm;
    this.hashAlgorithm = hashAlgorithm;
    this.macAlgorithm = macAlgorithm;
    this.minIterations = minIterations;
  }

  public final String mechanismName() {
    return mechanismName;
  }

  public String hashAlgorithm() {
    return hashAlgorithm;
  }

  public String macAlgorithm() {
    return macAlgorithm;
  }

  public int minIterations() {
    return minIterations;
  }

  public static ScramMechanism forMechanismName(String mechanismName) {
    return MECHANISMS_MAP.get(mechanismName);
  }

  public static Collection<String> mechanismNames() {
    return MECHANISMS_MAP.keySet();
  }

  public static boolean isScram(String mechanismName) {
    return MECHANISMS_MAP.containsKey(mechanismName);
  }
}
