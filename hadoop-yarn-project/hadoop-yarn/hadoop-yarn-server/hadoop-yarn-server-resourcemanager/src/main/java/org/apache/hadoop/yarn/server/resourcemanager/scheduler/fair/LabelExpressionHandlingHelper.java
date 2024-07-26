package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LabelExpressionHandlingHelper {
  private static final Logger LOG = LoggerFactory.getLogger(LabelExpressionHandlingHelper.class);
  private static Map <String, Set<String>> nodeLabelsMap = null;
  private static LabelExpressionHandlingHelper labelHelper = null;
  private static RMNodeLabelsManager localLabelsManager = null;

  private LabelExpressionHandlingHelper(){}

  public static LabelExpressionHandlingHelper getInstance(RMNodeLabelsManager labelsManager){
    if(labelHelper == null){
      LOG.debug("Initializing LabelExpressionHandlingHelper for Scheduler");
      labelHelper = new LabelExpressionHandlingHelper();
      localLabelsManager = labelsManager;
      nodeLabelsMap = labelsManager.getNodeLabels().entrySet().stream().collect(Collectors.toMap(
              entry -> entry.getKey().getHost(),
              entry -> entry.getValue()));
    }
    return labelHelper;
  }

  public void reloadLabels(){
    LOG.info("Labels information are reloaded for Scheduler");
    nodeLabelsMap = localLabelsManager.getNodeLabels().entrySet().stream().collect(Collectors.toMap(
            entry -> entry.getKey().getHost(),
            entry -> entry.getValue()));
  }

  public LabelApplicabilityStatus isNodeApplicableForApp(String node, String appLabel) {
    if (appLabel == null) {
      return LabelApplicabilityStatus.NOT_APPLICABLE;
    }
    Set<String> nodeLabels = nodeLabelsMap.get(node);
    if (nodeLabels == null || nodeLabels.isEmpty()) {
      return LabelApplicabilityStatus.NODE_DOES_NOT_HAVE_LABEL;
    }
    if(nodeLabels.contains(appLabel)) {
      return LabelApplicabilityStatus.NODE_HAS_LABEL;
    } else {
      return LabelApplicabilityStatus.NODE_DOES_NOT_HAVE_LABEL;
    }
  }

  public enum LabelApplicabilityStatus {
    NOT_APPLICABLE,
    NODE_HAS_LABEL,
    NODE_DOES_NOT_HAVE_LABEL;
  }
}
