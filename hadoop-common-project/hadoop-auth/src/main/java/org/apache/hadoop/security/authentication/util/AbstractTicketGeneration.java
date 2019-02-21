package org.apache.hadoop.security.authentication.util;

import java.io.IOException;

public abstract class AbstractTicketGeneration {

  public abstract void generateTicketAndSetServerKey() throws IOException;

  public abstract void generateTicketAndSetServerKey(String clusterName) throws IOException;
}
