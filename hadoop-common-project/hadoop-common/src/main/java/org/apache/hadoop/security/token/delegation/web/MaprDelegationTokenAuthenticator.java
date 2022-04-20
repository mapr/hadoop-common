package org.apache.hadoop.security.token.delegation.web;

import org.apache.hadoop.security.authentication.client.AbstractMaprAuthenticator;

import java.util.ServiceLoader;

public class MaprDelegationTokenAuthenticator extends DelegationTokenAuthenticator {

    private static final ServiceLoader<AbstractMaprAuthenticator> loader =
            ServiceLoader.load(AbstractMaprAuthenticator.class);

    public MaprDelegationTokenAuthenticator() throws IllegalAccessException, InstantiationException {
        // Lazy loading
        super(loader.iterator().next());
    }
}