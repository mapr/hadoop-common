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

import Ember from 'ember';

export default Ember.Controller.extend({
  loading: true,
  isSsoDisabled: Ember.computed('checkSssoButton', function() {
    fetch(window.location.origin + "?action=ssoEnable", {}).then((response) => {
      if (!response.ok) {
        return true;
      } else{
        return false;
      }
    });
  }),

  actions: {
    signIn: function () {
      var reqURL = window.location.origin;
      $.ajax({
        type: 'GET',
        async: false,
        context: this,
        headers: {
          "Authorization": "Basic " + btoa(this.get('login') + ":" + this.get('password'))
        },
        url: reqURL,

      }).always(function (data, textStatus, jqXHR) {
        switch (jqXHR.status) {
          case 200:
          case 302:
          case 307:
            Ember.Logger.log("Login with PAM successful.");
            window.localStorage.setItem("logToUI", "1");
            window.location.reload();
            break;
          default:
            Ember.Logger.log("Login with PAM error.");
            break;
        }

      });

    },
    signSSO: function () {
      Ember.Logger.log("SSO log initialized.");
      fetch(window.location.origin + "?ui2=true&action=initSSO", {
        headers: {
          'Content-Type': 'application/json'
        },
      }).then((response) => {
        if (!response.ok) {
          throw new Error(`Response status: ${response.status}`);
        }
        response.json().then(data => {
          window.location.href = data.loginURL;
        });
      });
    }
  }

});