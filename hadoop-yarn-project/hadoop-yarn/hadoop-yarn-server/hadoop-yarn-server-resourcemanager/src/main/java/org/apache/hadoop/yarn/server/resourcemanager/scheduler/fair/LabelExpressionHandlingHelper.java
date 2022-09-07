package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class LabelExpressionHandlingHelper {
  private static final Logger LOG = LoggerFactory.getLogger(LabelExpressionHandlingHelper.class);

  public LabelExpressionHandlingHelper() {
  }

  public static LabelApplicabilityStatus isNodeApplicableForApp(String node, String appLabel, RMNodeLabelsManager labelsManager) {
    if (appLabel == null) {
      return LabelApplicabilityStatus.NOT_APPLICABLE;
    }
    Optional<Map.Entry<NodeId, Set<String>>> nodeLabelsOpt = labelsManager.getNodeLabels().entrySet().stream().filter(e -> e.getKey().getHost().equals(node)).findFirst();
    if(!nodeLabelsOpt.isPresent()) {
      return LabelApplicabilityStatus.NODE_DOES_NOT_HAVE_LABEL;
    }
    Set<String> nodeLabels = nodeLabelsOpt.get().getValue();
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
