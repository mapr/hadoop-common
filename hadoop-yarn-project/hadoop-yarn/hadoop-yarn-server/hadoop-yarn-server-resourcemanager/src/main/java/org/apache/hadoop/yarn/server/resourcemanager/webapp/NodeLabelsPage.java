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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;

import org.apache.hadoop.yarn.api.records.NodeToLabelsList;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.labelmanagement.LabelManager;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodeLabelsPage extends RmView {
  static class NodeLabelsBlock extends HtmlBlock {
    final ResourceManager rm;

    @Inject
    NodeLabelsBlock(ResourceManager rm, ViewContext ctx) {
      super(ctx);
      this.rm = rm;
    }

    @Override
    protected void render(Block html) {
      List<NodeToLabelsList> labelsForAllNodes = LabelManager.getInstance().getLabelsForAllNodes(true);

      Map<String, Integer> labelNumNodesMap = getLabelNumNodesMap(labelsForAllNodes);

      html.div().$class("labelnumnodes").h3("Label and num nodes")._();

      TBODY<TABLE<Hamlet>> labelNodes = html.table("#labelnodes").
          thead().$class("ui-widget-header").
          tr().
          th().$class("ui-state-default")._("Label")._().
          th().$class("ui-state-default")._("Num nodes")._().
          _()._().
          tbody().$class("ui-widget-content");

      for (Map.Entry<String, Integer> e : labelNumNodesMap.entrySet()) {
        TR<TBODY<TABLE<Hamlet>>> row =
            labelNodes.tr().td(e.getKey());

        if (e.getValue() == null) {
          row.td("0")._();
        } else {
          row.td(String.valueOf(e.getValue()))._();
        }
      }
      labelNodes._()._();


      html.div().$class("nodelabelsdata").h3("Node labels")._();

      TBODY<TABLE<Hamlet>> nodeLabels = html.table("#nodelabels").
          thead().
          tr().
          th(".node", "Node").
          th(".labels", "Node Labels").
          _()._().
          tbody();

      for (NodeToLabelsList ntl : labelsForAllNodes) {
        TR<TBODY<TABLE<Hamlet>>> row =
            nodeLabels.tr().td(ntl.getNode());

        if (ntl.getNodeLabel() == null || ntl.getNodeLabel().isEmpty()) {
          row.td("No labels")._();
        } else {
          row.td(ntl.getNodeLabel().toString())._();
        }
      }
      nodeLabels._()._();
    }

    private Map<String, Integer> getLabelNumNodesMap(List<NodeToLabelsList> labelsForAllNodes) {
      Map<String, Integer> labelNodeQuantityMap = new HashMap<>();
      for (NodeToLabelsList ntl : labelsForAllNodes) {
        String node = ntl.getNode();
        List<String> nodeLabels = ntl.getNodeLabel();

        for (String label : nodeLabels) {
          if (labelNodeQuantityMap.containsKey(label)) {
            Integer num = labelNodeQuantityMap.get(label);
            labelNodeQuantityMap.put(label, ++num);
          } else {
            labelNodeQuantityMap.put(label, 1);
          }
        }
      }
      return labelNodeQuantityMap;
    }
  }

  @Override protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);
    String title = "Node labels of the cluster";
    setTitle(title);
    set(DATATABLES_ID, "nodelabels");
    setTableStyles(html, "nodelabels", ".healthStatus {width:10em}",
                   ".healthReport {width:10em}");
  }

  @Override protected Class<? extends SubView> content() {
    return NodeLabelsBlock.class;
  }
}
