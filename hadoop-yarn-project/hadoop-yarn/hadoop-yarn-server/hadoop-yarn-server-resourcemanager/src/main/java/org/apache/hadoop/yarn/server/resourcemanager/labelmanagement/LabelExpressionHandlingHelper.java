/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.labelmanagement;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.NodeToLabelsList;
import org.apache.hadoop.yarn.server.resourcemanager.labelmanagement.LabelManager.LabelApplicabilityStatus;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;

import net.java.dev.eval.Expression;

/**
 * Helper class responsible for Logical Expression Evaluation
 * and helping figuring out resource applicability based on label expression
 * and labels per node
 *
 */
public class LabelExpressionHandlingHelper {

  private static final Log LOG = LogFactory.getLog(LabelExpressionHandlingHelper.class);

  public LabelExpressionHandlingHelper() {
  }

  static Expression getEffectiveLabelExpr(String label) throws IOException {
    if (label == null) {
      return null;
    }
    Expression labelExpression;
    
    String labelExpressionString = label.replaceAll("^\"|\"$", "");
    // check if app could be scheduled anywhere in cluster.
    if ("*".equals(labelExpressionString) || 
        "all".equals(labelExpressionString)) {
      labelExpression = null;
    } else {
      labelExpressionString = parseAndWrapLabel(labelExpressionString);
      try {
        // create an expression, set fillEmptyValues to true and default value 0
        labelExpression = new Expression(labelExpressionString);
      } catch (Throwable t) {
        LOG.warn("Invalid label format " + labelExpressionString + 
                 " Error " + t);
        return null;
      }
    }
    return labelExpression;
  }

  private static String parseAndWrapLabel(String label) {
    String[] labels = label.split("\\(|\\)|!|&&|\\|\\|");
    List<String> cleanLabels = new ArrayList<>();
    for (int i = 0; i < labels.length; i++) {
      if (!labels[i].matches(" ||")) {
        cleanLabels.add(labels[i].trim());
      }
    }
    int startInd;
    int endInd = 0;
    for (String tmpLabel : cleanLabels) {
      startInd = label.indexOf(tmpLabel, endInd);
      endInd = startInd + tmpLabel.length();
      String wrappedLabel = wrapIfNeeded(tmpLabel);
      label = label.substring(0, startInd) + wrappedLabel + label.substring(endInd);
      if (!wrappedLabel.equals(tmpLabel)) {
        endInd += 2;
      }
    }
    LOG.debug("Final wrapped label expression: " + label);
    return label;
  }

  /**
   * If a label contains two or more words (such as "High Memory"),
   * we should enclose the name in single or double quotation marks
   * so the whitespace will not be interpreted as a delimiter between two labels.
   * @param label The label name
   * @return If the label contains whitespace and doesn't enclose in single or double quotation marks
   * returns label enclosed in single quotation marks, otherwise returns the same label.
   */
  private static String wrapIfNeeded(String label) {
    final String SINGLE_QUOTATION_MARK = "'";
    final String DOUBLE_QUOTATION_MARK = "\"";
    boolean shouldBeWrapped = label.contains(" ") &&
        !(StringUtils.startsWith(label, SINGLE_QUOTATION_MARK) &&
            StringUtils.endsWith(label, SINGLE_QUOTATION_MARK) ||
            StringUtils.startsWith(label, DOUBLE_QUOTATION_MARK) &&
                StringUtils.endsWith(label, DOUBLE_QUOTATION_MARK));
    if (shouldBeWrapped) {
      label = SINGLE_QUOTATION_MARK + label + SINGLE_QUOTATION_MARK;
    }
    return label;
  }

  static Expression constructAppLabel(Queue.QueueLabelPolicy policy,
      Expression appLabelExpression,
      Expression queueLabelExpression) {
    if (Queue.QueueLabelPolicy.AND.equals(policy)) {
      if (queueLabelExpression != null && appLabelExpression != null) {
        return new Expression("(" + queueLabelExpression.toString() + 
            ") && (" + appLabelExpression.toString() + ")");
      }
      if (queueLabelExpression == null) {
        return appLabelExpression;
      } else {
        return queueLabelExpression;
      }
    } else if (Queue.QueueLabelPolicy.OR.equals(policy)) {
      if (queueLabelExpression != null && appLabelExpression != null) {
        return new Expression("(" + queueLabelExpression.toString() + 
            ") || (" + appLabelExpression.toString() + ")");
      }
      if (queueLabelExpression == null) {
        return appLabelExpression;
      } else {
        return queueLabelExpression;
      }
    } else if (Queue.QueueLabelPolicy.PREFER_QUEUE.equals(policy)) {
      return queueLabelExpression;
    } else if (Queue.QueueLabelPolicy.PREFER_APP.equals(policy)) {
      return appLabelExpression;
    }
    // wrong/no policy? return back app label
    return appLabelExpression;
  }
  
  static LabelApplicabilityStatus isNodeApplicableForApp(String node, Expression finalAppLabelExp) 
      throws IOException {
      if ( finalAppLabelExp == null ) {
        return LabelApplicabilityStatus.NOT_APPLICABLE;
      }
      Set<String> nodeLabels = LabelStorage.getInstance().getLabelsForNode(node);
      if ( nodeLabels == null || nodeLabels.isEmpty() ) {
        return LabelApplicabilityStatus.NODE_DOES_NOT_HAVE_LABEL;
      }
      Map<String, BigDecimal> labelEvalFillersTmp = 
          LabelStorage.getInstance().getFillers();
      
      for ( String label : nodeLabels) {
        labelEvalFillersTmp.put(label, BigDecimal.valueOf(1l));
      }
      try {
        BigDecimal retValue = finalAppLabelExp.eval(labelEvalFillersTmp);
        return (retValue.intValue() == 0 ) ? 
            LabelApplicabilityStatus.NODE_DOES_NOT_HAVE_LABEL : 
            LabelApplicabilityStatus.NODE_HAS_LABEL;
      } catch (Throwable t ) {
        LOG.warn("Exception while evaluating: " + finalAppLabelExp , t);
        throw new IOException("Exception while evaluating: " + finalAppLabelExp);
      }
    }

  static List<String> getNodesForLabel(Expression label) throws IOException {

    List<String> nodesForLabel = new ArrayList<>();
    List<NodeToLabelsList> labelsForAllNodes = LabelStorage.getInstance().getLabelsForAllNodes(true);
    LabelManager lb = LabelManager.getInstance();
    LabelApplicabilityStatus blackListStatus;

    for (NodeToLabelsList nodeToLabels : labelsForAllNodes) {
      blackListStatus = lb.isNodeApplicableForApp(nodeToLabels.getNode(), label);
      if (blackListStatus == LabelApplicabilityStatus.NOT_APPLICABLE ||
          blackListStatus == LabelApplicabilityStatus.NODE_HAS_LABEL) {
        nodesForLabel.add(nodeToLabels.getNode());
      }
    }
    return nodesForLabel;
  }
}
