package org.apache.hadoop.security.token.delegation.web;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.authentication.server.MultiMechsAuthenticationHandler;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MaprDelegationTokenAuthenticationHandler extends DelegationTokenAuthenticationHandler {
    public MaprDelegationTokenAuthenticationHandler() {
        super(new MultiMechsAuthenticationHandler());
    }
}