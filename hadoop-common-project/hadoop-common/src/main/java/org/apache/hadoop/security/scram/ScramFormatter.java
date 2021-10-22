package org.apache.hadoop.security.scram;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;


import org.apache.hadoop.security.scram.ScramMessages.ClientFirstMessage;
import org.apache.hadoop.security.scram.ScramMessages.ServerFirstMessage;
import org.apache.hadoop.security.scram.ScramMessages.ClientFinalMessage;



/**
 * Scram message salt and hash functions defined in <a href="https://tools.ietf.org/html/rfc5802">RFC 5802</a>.
 */
public class ScramFormatter {

  private final MessageDigest messageDigest;
  private final Mac mac;
  private final SecureRandom random;

  public ScramFormatter(ScramMechanism mechanism) throws NoSuchAlgorithmException {
    this.messageDigest = MessageDigest.getInstance(mechanism.hashAlgorithm());
    this.mac = Mac.getInstance(mechanism.macAlgorithm());
    this.random = new SecureRandom();
  }

  public byte[] hmac(byte[] key, byte[] bytes) throws InvalidKeyException {
    mac.init(new SecretKeySpec(key, mac.getAlgorithm()));
    return mac.doFinal(bytes);
  }

  public byte[] hash(byte[] str) {
    return messageDigest.digest(str);
  }

  public byte[] xor(byte[] first, byte[] second) {
    if (first.length != second.length)
      throw new IllegalArgumentException("Argument arrays must be of the same length");
    byte[] result = new byte[first.length];
    for (int i = 0; i < result.length; i++)
      result[i] = (byte) (first[i] ^ second[i]);
    return result;
  }

  public byte[] hi(byte[] str, byte[] salt, int iterations) throws InvalidKeyException {
    mac.init(new SecretKeySpec(str, mac.getAlgorithm()));
    mac.update(salt);
    byte[] u1 = mac.doFinal(new byte[]{0, 0, 0, 1});
    byte[] prev = u1;
    byte[] result = u1;
    for (int i = 2; i <= iterations; i++) {
      byte[] ui = hmac(str, prev);
      result = xor(result, ui);
      prev = ui;
    }
    return result;
  }

  public byte[] normalize(String str) {
    return toBytes(str);
  }

  public byte[] saltedPassword(String password, byte[] salt, int iterations) throws InvalidKeyException {
    return hi(normalize(password), salt, iterations);
  }

  public byte[] clientKey(byte[] saltedPassword) throws InvalidKeyException {
    return hmac(saltedPassword, toBytes("Client Key"));
  }

  public byte[] storedKey(byte[] clientKey) {
    return hash(clientKey);
  }

  public String saslName(String username) {
    return username.replace("=", "=3D").replace(",", "=2C");
  }

  public String username(String saslName) {
    String username = saslName.replace("=2C", ",");
    if (username.replace("=3D", "").indexOf('=') >= 0)
      throw new IllegalArgumentException("Invalid username: " + saslName);
    return username.replace("=3D", "=");
  }

  public String authMessage(String clientFirstMessageBare, String serverFirstMessage, String clientFinalMessageWithoutProof) {
    return clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;
  }

  public byte[] clientSignature(byte[] storedKey, ClientFirstMessage clientFirstMessage, ServerFirstMessage serverFirstMessage, ClientFinalMessage clientFinalMessage) throws InvalidKeyException {
    byte[] authMessage = authMessage(clientFirstMessage, serverFirstMessage, clientFinalMessage);
    return hmac(storedKey, authMessage);
  }

  public byte[] clientProof(byte[] saltedPassword, ClientFirstMessage clientFirstMessage, ServerFirstMessage serverFirstMessage, ClientFinalMessage clientFinalMessage) throws InvalidKeyException {
    byte[] clientKey = clientKey(saltedPassword);
    byte[] storedKey = hash(clientKey);
    byte[] clientSignature = hmac(storedKey, authMessage(clientFirstMessage, serverFirstMessage, clientFinalMessage));
    return xor(clientKey, clientSignature);
  }

  private byte[] authMessage(ClientFirstMessage clientFirstMessage, ServerFirstMessage serverFirstMessage, ClientFinalMessage clientFinalMessage) {
    return toBytes(authMessage(clientFirstMessage.clientFirstMessageBare(),
        serverFirstMessage.toMessage(),
        clientFinalMessage.clientFinalMessageWithoutProof()));
  }

  public byte[] storedKey(byte[] clientSignature, byte[] clientProof) {
    return hash(xor(clientSignature, clientProof));
  }

  public byte[] serverKey(byte[] saltedPassword) throws InvalidKeyException {
    return hmac(saltedPassword, toBytes("Server Key"));
  }

  public byte[] serverSignature(byte[] serverKey, ClientFirstMessage clientFirstMessage, ServerFirstMessage serverFirstMessage, ClientFinalMessage clientFinalMessage) throws InvalidKeyException {
    byte[] authMessage = authMessage(clientFirstMessage, serverFirstMessage, clientFinalMessage);
    return hmac(serverKey, authMessage);
  }

  public String secureRandomString() {
    return new BigInteger(130, random).toString(Character.MAX_RADIX);
  }

  public byte[] secureRandomBytes() {
    return toBytes(secureRandomString());
  }

  public byte[] toBytes(String str) {
    return str.getBytes(StandardCharsets.UTF_8);
  }

  public ScramCredential generateCredential(String password, int iterations) {
    try {
      byte[] salt = secureRandomBytes();
      byte[] saltedPassword = saltedPassword(password, salt, iterations);
      byte[] clientKey = clientKey(saltedPassword);
      byte[] storedKey = storedKey(clientKey);
      byte[] serverKey = serverKey(saltedPassword);
      return new ScramCredential(salt, storedKey, serverKey, iterations);
    } catch (InvalidKeyException e) {
      throw new RuntimeException("Could not create credential", e);
    }
  }
}
