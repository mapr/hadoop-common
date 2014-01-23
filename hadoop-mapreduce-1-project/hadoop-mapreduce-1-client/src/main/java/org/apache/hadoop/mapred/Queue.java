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
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringUtils;

import net.java.dev.eval.Expression;
import java.math.BigDecimal;

/**
 * A class for storing the properties of a job queue.
 */
class Queue {

  private static final Log LOG = LogFactory.getLog(Queue.class);

  private String name;
  private Map<String,AccessControlList> acls;
  private QueueState state = QueueState.RUNNING;
  private Expression queueLabelExpression = null;
  private QueueLabelPolicy labelPolicy = QueueLabelPolicy.AND;

  /**
   * An Object that can be used by schedulers to fill in
   * arbitrary scheduling information. The toString method
   * of these objects will be called by the framework to
   * get a String that can be displayed on UI.
   */
  private Object schedulingInfo;

  /**
   * Enum representing queue state
   */
  static enum QueueState {
    STOPPED("stopped"), RUNNING("running");

    private final String stateName;

    QueueState(String stateName) {
      this.stateName = stateName;
    }

    public String getStateName() {
      return stateName;
    }
  }

  /**
   * Enum representing queue label policies
   */
  static enum QueueLabelPolicy {
    PREFER_QUEUE("PREFER_QUEUE"), PREFER_JOB("PREFER_JOB"), AND("AND"), OR("OR");

    private final String policyName;

    QueueLabelPolicy(String policyName) {
      this.policyName = policyName;
    }

    public String getLabelPolicyName() {
      return policyName;
    }
  }

  /**
   * Create a job queue
  /**
   * Create a job queue
   * @param name name of the queue
   * @param acls ACLs for the queue
   * @param state state of the queue
   */
  Queue(String name, Map<String, AccessControlList> acls, QueueState state, 
        String queueLabel, QueueLabelPolicy labelPolicy) {
    this.name = name;
    this.acls = acls;
    this.state = state;
    setLabel(queueLabel);
    this.labelPolicy = labelPolicy;
  }

  /**
   * Return the name of the queue
   *
   * @return name of the queue
   */
  String getName() {
    return name;
  }

  /**
   * Set the name of the queue
   * @param name name of the queue
   */
  void setName(String name) {
    assert name != null;
    this.name = name;
  }

  /**
   * Return the ACLs for the queue
   *
   * The keys in the map indicate the operations that can be performed,
   * and the values indicate the list of users/groups who can perform
   * the operation.
   *
   * @return Map containing the operations that can be performed and
   *          who can perform the operations.
   */
  Map<String, AccessControlList> getAcls() {
    return acls;
  }

  /**
   * Set the ACLs for the queue
   * @param acls Map containing the operations that can be performed and
   *          who can perform the operations.
   */
  void setAcls(Map<String, AccessControlList> acls) {
    assert acls != null;
    this.acls = acls;
  }

  /**
   * Return the state of the queue.
   * @return state of the queue
   */
  QueueState getState() {
    return state;
  }

  /**
   * Set the state of the queue.
   * @param state state of the queue.
   */
  void setState(QueueState state) {
    assert state != null;
    this.state = state;
  }

  Expression getLabel() {
    return this.queueLabelExpression;
  }

  /** 
    * Set node labels on a queue. Jobs Inherit queue label based on policy set.
    * In case user sends incorrect expression retain old label
    */
  void setLabel(String queueLabelExpressionString) {
    if (queueLabelExpressionString == null || 
        queueLabelExpressionString.isEmpty()) {
      this.queueLabelExpression = null;
      LOG.info("Queue " + this.name + " is allowed to be scheduled on all nodes.");  
      return;
    }
    Expression oldLabel = this.queueLabelExpression;
    // remove leading and trailing quotes
    queueLabelExpressionString = 
      queueLabelExpressionString.replaceAll("^\"|\"$", "");
    if ("*".equals(queueLabelExpressionString) || 
        "all".equals(queueLabelExpressionString)) {
      this.queueLabelExpression = null;
      LOG.info("Queue " + this.name + " is allowed to be scheduled on all nodes.");  
    } else {
      try {
        this.queueLabelExpression = new Expression(queueLabelExpressionString,
                                                   true, new BigDecimal(0));
        // evaluate this expression to make sure format is correct and supported.
        this.queueLabelExpression.eval(new HashMap<String, BigDecimal>());
        LOG.info("Queue " + this.name + 
                 " is allowed to be scheduled on nodes which will match '" 
                 + queueLabelExpressionString + "'");
      } catch (Throwable t) {
        this.queueLabelExpression = oldLabel;
        LOG.info("Queue " + this.name + " has Invalid label format " 
                 + queueLabelExpressionString + ". Error " + t);
      }
    }
  }

  QueueLabelPolicy getLabelPolicy() {
    return labelPolicy;
  }

  void setLabelPolicy(QueueLabelPolicy policy) {
    if (policy != null) {
      this.labelPolicy = policy;
    } else {
      this.labelPolicy = QueueLabelPolicy.AND;
    }
  }

  Expression constructJobLabel(Expression jobLabel) {
    if (QueueLabelPolicy.AND.equals(labelPolicy)) {
      if (queueLabelExpression != null && jobLabel != null) {
        return new Expression("(" + queueLabelExpression.toString() + 
                              ") && (" + jobLabel.toString() + ")",
                              true, new BigDecimal(0));
      }
      if (queueLabelExpression == null) {
        return jobLabel;
      } else {
        return queueLabelExpression;
      }
    } else if (QueueLabelPolicy.OR.equals(labelPolicy)) {
      if (queueLabelExpression != null && jobLabel != null) {
        return new Expression("(" + queueLabelExpression.toString() + 
                              ") || (" + jobLabel.toString() + ")",
                              true, new BigDecimal(0));
      }
      if (queueLabelExpression == null) {
        return jobLabel;
      } else {
        return queueLabelExpression;
      }
    } else if (QueueLabelPolicy.PREFER_QUEUE.equals(labelPolicy)) {
      return queueLabelExpression;
    } else if (QueueLabelPolicy.PREFER_JOB.equals(labelPolicy)) {
      return jobLabel;
    }
    // wrong/no policy? return back job label
    return jobLabel;
  }

  /**
   * Return the scheduling information for the queue
   * @return scheduling information for the queue.
   */
  Object getSchedulingInfo() {
    String labelInfo;
    if (queueLabelExpression != null) {
      labelInfo = " Queue label = '" + queueLabelExpression.toString() 
            + "', Queue label policy = " + labelPolicy.getLabelPolicyName();
    } else {
      labelInfo = " Queue label = 'all'" 
            + ", Queue label policy = " + labelPolicy.getLabelPolicyName();
    }
    if (schedulingInfo != null) {
      return schedulingInfo + labelInfo;
    }
    return labelInfo;
  }

  /**
   * Set the scheduling information from the queue.
   * @param schedulingInfo scheduling information for the queue.
   */
  void setSchedulingInfo(Object schedulingInfo) {
    this.schedulingInfo = schedulingInfo;
  }
}
