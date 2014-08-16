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
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

  static Expression getEffectiveLabelExpr(String appLabelStr) throws IOException {
    if ( appLabelStr == null ) {
      return null;
    }
    Expression appLabelExpression;
    
    String appLabelExpressionString = appLabelStr.replaceAll("^\"|\"$", "");
    // check if app could be scheduled anywhere in cluster.
    if ("*".equals(appLabelExpressionString) || 
        "all".equals(appLabelExpressionString)) {
      appLabelExpression = null;
    } else {
      try {
        // create an expression, set fillEmptyValues to true and default value 0
        appLabelExpression = new Expression(appLabelExpressionString);
      } catch (Throwable t) {
        LOG.warn("Invalid label format " + appLabelExpressionString + 
                 " Error " + t);
        return null;
      }
    }
    return appLabelExpression;
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
        // if node does not have any label associated with it
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
}
