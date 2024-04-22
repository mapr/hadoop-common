#!/bin/bash

function createSymlinks() {
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/hadoop-azure-datalake*.jar
  ln -sf __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/tools/lib/hadoop-azure-datalake*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  ln -sf __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/tools/lib/hadoop-azure-__VERSION_3DIGIT__*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/azure-data-lake-store-sdk*.jar
  ln -sf __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/tools/lib/azure-data-lake-store-sdk*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/

  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/hdfs/lib/failureaccess-*
  ln -sf __PREFIX__/lib/failureaccess-* __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/hdfs/lib/
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/hdfs/lib/guava-*
  ln -sf __PREFIX__/lib/guava-* __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/hdfs/lib/

  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/aws*.jar
  ln -sf __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/tools/lib/aws*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/azure*.jar
  ln -sf __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/tools/lib/azure*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/
  rm -f __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/hadoop-aws*.jar
  ln -sf __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/tools/lib/hadoop-aws*.jar __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/common/lib/

  rm -f __PREFIX__/lib/hadoop-yarn-client-__VERSION_3DIGIT__*.jar
  ln -sf __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/yarn/hadoop-yarn-client-__VERSION_3DIGIT__*.jar __PREFIX__/lib/
  rm -f __PREFIX__/lib/hadoop-yarn-common-*.jar
  ln -sf __PREFIX__/hadoop/hadoop-__VERSION_3DIGIT__/share/hadoop/yarn/hadoop-yarn-common-__VERSION_3DIGIT__*.jar __PREFIX__/lib/

}

createSymlinks

