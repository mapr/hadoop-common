package org.apache.hadoop.security.authentication.util;

public abstract class AbstractTicketGeneration {

    public abstract void generateTicketAndSetServerKey();

    public abstract void generateTicketAndSetServerKey(String clusterName);
}
