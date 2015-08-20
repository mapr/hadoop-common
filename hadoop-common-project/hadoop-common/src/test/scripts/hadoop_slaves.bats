# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

load hadoop-functions_test_helper

@test "hadoop_populate_slaves_file (specific file)" {
  touch "${TMP}/file"
  hadoop_populate_slaves_file "${TMP}/file"
  [ "${HADOOP_SLAVES}" = "${TMP}/file" ]
}

@test "hadoop_populate_slaves_file (specific conf dir file)" {
  HADOOP_CONF_DIR=${TMP}/1
  mkdir -p "${HADOOP_CONF_DIR}"
  touch "${HADOOP_CONF_DIR}/file"
  hadoop_populate_slaves_file "file"
  echo "${HADOOP_SLAVES}"
  [ "${HADOOP_SLAVES}" = "${HADOOP_CONF_DIR}/file" ]
}

@test "hadoop_populate_slaves_file (no file)" {
  HADOOP_CONF_DIR=${TMP}
  run hadoop_populate_slaves_file "foo"
  [ "${status}" -eq 1 ]
}