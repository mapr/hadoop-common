package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashSet;
import java.util.Set;

public class DebugController {

    public static final Log LOG = LogFactory.getLog(DebugController.class);

    private Set<String> apps   = new HashSet<>();
    private Set<String> queues = new HashSet<>();

    private static DebugController debugController = new DebugController();

    private  DebugController() {
    }

    public static DebugController getInstance() {
        return debugController;
    }

    public boolean isUseDebugController(String queueName, String appId) {

        if (LOG.isDebugEnabled()) {
            if (apps.isEmpty() && queues.isEmpty()) {
                return true;
            } else {
                if (containsQueue(queueName) || containsApp(appId)) {
                    return true;
                }
            }
        }
        return false;
    }

    public synchronized void addApp(String app) {
        apps.add(app);
    }

    public synchronized void removeApp(String app) {
        apps.remove(app);
    }

    public synchronized boolean containsApp(String app) {
        return apps.contains(app);
    }

    public Set<String> getApps() {
        return apps;
    }

    public synchronized void addQueue(String queue) {
        queues.add(queue);
    }

    public synchronized void removeQueue(String queue) {
        queues.remove(queue);
    }

    public synchronized boolean containsQueue(String queue) {
        return queues.contains(queue);
    }

    public Set<String> getQueues() {
        return queues;
    }
}
