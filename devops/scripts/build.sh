#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
WORK_DIR=${SCRIPT_DIR}/../..
. "${SCRIPT_DIR}/_initialize_package_variables.sh"
. "${SCRIPT_DIR}/_utils.sh"

cecho() {
  echo "--> $1"
}

build_hadoop() {
  cecho "Hadoop build start"
  cecho "GIT_COMMIT          : '$GIT_COMMIT'"
  cecho "INSTALLATION_PREFIX : '$INSTALLATION_PREFIX'"
  cecho "PKG_NAME            : '$PKG_NAME'"
  cecho "PKG_VERSION         : '$PKG_VERSION'"
  cecho "PKG_3DIGIT_VERSION  : '$PKG_3DIGIT_VERSION'"
  cecho "TIMESTAMP           : '$TIMESTAMP'"
  cecho "PKG_INSTALL_ROOT    : '$PKG_INSTALL_ROOT'"
  cecho "DIST_DIR            : '$DIST_DIR'"
  cecho "BUILD_ROOT          : '$BUILD_ROOT'"
  cecho "OS                  : '$OS'"
	mkdir -pv "${BUILD_ROOT}"
	mkdir -pv "${BUILD_ROOT}/distribution"
  cd "${WORK_DIR}"
	if [ "${OS}" = "mac" ]; then
		mvn -e -U install -Pdist -DskipTests -Dtar -Djava.awt.headless=true -Dmaven.javadoc.skip=true
		cd hadoop-hdfs-project/hadoop-hdfs-native-client && mvn -e -U install -Pnative -DskipTests -Dmaven.javadoc.skip=true || true
	elif [ "${OS}" = "ubuntu" ]; then \
		mvn install -Pdist -Pnative -Pyarn-ui -Drequire.snappy=true -Dbundle.snappy=true -Dsnappy.lib=/usr/local/lib/ -Drequire.zstd=true -Dbundle.zstd=true -Dzstd.lib=/usr/lib/x86_64-linux-gnu/ -Dmaven.javadoc.skip=true -DskipTests -Dtar
	else \
		mvn install -Pdist -Pnative -Pyarn-ui -Drequire.snappy=true -Dbundle.snappy=true -Dsnappy.lib=/usr/local/lib/ -Drequire.zstd=true -Dbundle.zstd=true -Dzstd.lib=/usr/lib64/ -Dmaven.javadoc.skip=true -DskipTests -Dtar
	fi

	tar -xvzf ${WORK_DIR}/${HADOOP_IMAGE_DIST}/${HADOOP_IMAGE_NAME} --directory ${BUILD_ROOT}/distribution
}

prepare_hadoop_util() {
  HADOOP_UTIL_INSTALL_3DIGIT_DIR="${BUILD_ROOT}/root/hadoop-util${PKG_INSTALL_ROOT}"
	mkdir -p ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/bin
	mkdir -p ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/libexec
	mkdir -p ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/include
	mkdir -p ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/share/hadoop/common
	mkdir -p ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/share/hadoop/yarn
	mkdir -p ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/share/hadoop/yarn/sources
	mkdir -p ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/share/hadoop/yarn/lib
	mkdir -p ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/etc/hadoop
	mkdir -p ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/lib/native

	mkdir -pv "${BUILD_ROOT}/root/hadoop-util${INSTALLATION_PREFIX}/roles"
	echo -e "PKG_HOME_DIR=${PKG_INSTALL_ROOT}\nPKG_CONFIG_COMMAND=${PKG_INSTALL_ROOT}/bin/hadoop_util_conf.sh\n" > "${BUILD_ROOT}/root/hadoop-util${INSTALLATION_PREFIX}/roles/hadoop-util"

	cat ${WORK_DIR}/ext-conf/hadoop_symlinks.sh | sed -e "s|__INSTALL__|${PKG_INSTALL_ROOT}|g;s|__VERSION_3DIGIT__|${PKG_3DIGIT_VERSION}|g;s|__PREFIX__|${INSTALLATION_PREFIX}|g" > ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/bin/hadoop_symlinks.sh
	chmod 755 ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/bin/hadoop_symlinks.sh
	cat ${WORK_DIR}/ext-conf/hadoop_util_conf.sh | sed -e "s|__INSTALL__|${PKG_INSTALL_ROOT}|g;s|__VERSION_3DIGIT__|${PKG_3DIGIT_VERSION}|g;s|__PREFIX__|${INSTALLATION_PREFIX}|g" > ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/bin/hadoop_util_conf.sh
	chmod 755 ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/bin/hadoop_util_conf.sh
	cat ${WORK_DIR}/ext-conf/createRMVolume.sh | sed -e "s|__INSTALL__|${PKG_INSTALL_ROOT}|g;s|__VERSION_3DIGIT__|${PKG_3DIGIT_VERSION}|g;s|__PREFIX__|${INSTALLATION_PREFIX}|g" > ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/bin/createRMVolume.sh
	chmod 755 ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/bin/createRMVolume.sh
	cat ${WORK_DIR}/ext-conf/createLocalVolumes.sh | sed -e "s|__INSTALL__|${PKG_INSTALL_ROOT}|g;s|__VERSION_3DIGIT__|${PKG_3DIGIT_VERSION}|g;s|__PREFIX__|${INSTALLATION_PREFIX}|g" > ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/bin/createLocalVolumes.sh
	chmod 755 ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/bin/createLocalVolumes.sh
	cat ${WORK_DIR}/ext-conf/verify_service | sed -e "s|__INSTALL__|${PKG_INSTALL_ROOT}|g;s|__VERSION_3DIGIT__|${PKG_3DIGIT_VERSION}|g;s|__PREFIX__|${INSTALLATION_PREFIX}|g" > ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/bin/verify_service
	chmod 755 ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/bin/verify_service
	pushd ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/bin ; \
	    ln -s verify_service verify_service-nodemanager ; \
	    ln -s verify_service verify_service-resourcemanager ; \
	    ln -s verify_service verify_service-timelineserver ; \
	    ln -s verify_service verify_service-historyserver ; \
	    ln -s verify_service verify_service-RMHA ; \
	popd ;
	cp ${WORK_DIR}/ext-conf/mapr-eco-config.sh ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/bin/
	chmod 755 ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/bin/mapr-eco-config.sh

	cp ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/bin/hadoop ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/bin
	cp -r ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/libexec/* ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/libexec/.
	cp ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/include/* ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/include/.
	cp -r ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/lib/native/* ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/lib/native/.
	cp ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/etc/hadoop/core-site* ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/etc/hadoop
	cp ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/etc/hadoop/log4j* ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/etc/hadoop
	cp ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/etc/hadoop/hadoop-env* ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/etc/hadoop
	cp ${WORK_DIR}/ext-conf/ssoConf ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/etc/hadoop
	cp ${WORK_DIR}/ext-conf/ssl* ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/etc/hadoop
	cp ${WORK_DIR}/ext-conf/hadoop_version ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/etc/hadoop/
	cp -r ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/share/hadoop/common/* ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/share/hadoop/common/.
	cp ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/share/hadoop/yarn/hadoop-yarn-api* ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/share/hadoop/yarn/.
	cp ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/share/hadoop/yarn/sources/hadoop-yarn-api* ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/share/hadoop/yarn/sources/.
	cp ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/share/hadoop/yarn/lib/websocket-{api,client}-* ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/share/hadoop/yarn/lib/.
	cp ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/LICENSE.txt ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/.
	cp ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/NOTICE.txt ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/.
	cp ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/README.txt ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/.
	mv -v ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/etc/hadoop/yarn-site.xml ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/etc/hadoop/yarn-site.xml.template

	#create symlink for native libraries
	pushd ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/lib/native ; \
		rm -f libhadoop.so libhdfs.so libsnappy.so libsnappy.so.1 libzstd.so libzstd.so.1; \
		ln -s libhadoop.so.1.* libhadoop.so ; \
		ln -s libhdfs.so.0* libhdfs.so ; \
		ln -s libsnappy.so.1.* libsnappy.so ; \
		ln -s libsnappy.so.1.* libsnappy.so.1 ; \
		ln -s libzstd.so.1.* libzstd.so ; \
		ln -s libzstd.so.1.* libzstd.so.1 ; \
	popd ; \
	find ${HADOOP_UTIL_INSTALL_3DIGIT_DIR} -name "*.cmd" -exec rm -fv {} \;
	find ${HADOOP_UTIL_INSTALL_3DIGIT_DIR} -name "*.bat" -exec rm -fv {} \;
	echo ${PKG_3DIGIT_VERSION} > ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/../hadoopversion
	mkdir -pv ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/logs
	chmod -v 1755 ${HADOOP_UTIL_INSTALL_3DIGIT_DIR}/logs

	# DEVOPS-2673 - delete duplicate JARs
	# There are three possible cases:
	# 1. There is only a jar ending in "-SNAPSHOT.jar"
	# 2. There is only a jar ending in "-<Maven-generated SNAPSHOT timestamp>.jar"
	# 3. Both JARs exist
	# The 3rd case is the problem. In that situation, we look for case 1 and delete case 2
	SNAPSHOT_JAR_LIST=`find ${HADOOP_UTIL_INSTALL_3DIGIT_DIR} -type f -name "*SNAPSHOT.jar"` ; \
	for SNAPSHOT_JAR in $${SNAPSHOT_JAR_LIST} ; do \
		echo "finding possible duplicate of $${SNAPSHOT_JAR} ..." ; \
		BASE_JAR_NAME=`echo ${SNAPSHOT_JAR} | sed "s/-SNAPSHOT.jar//g"` ; \
		echo "base JAR name read as $${BASE_JAR_NAME}" ; \
		POSSIBLE_DUPE_LIST=`find ${HADOOP_UTIL_INSTALL_3DIGIT_DIR} -type f -path "$${BASE_JAR_NAME}*.jar"` ; \
		for POSSIBLE_DUPE in ${POSSIBLE_DUPE_LIST} ; do \
			if [[ "$${POSSIBLE_DUPE}" != "$${SNAPSHOT_JAR}" ]]; then \
				echo "dupe ${POSSIBLE_DUPE} detected" ; \
				rm -fv ${POSSIBLE_DUPE} ; \
			fi ; \
		done ; \
	done ;
	# handle case of JARs with classifier "tests"
	SNAPSHOT_JAR_LIST=`find ${HADOOP_UTIL_INSTALL_3DIGIT_DIR} -type f -name "*SNAPSHOT-tests.jar"` ; \
	for SNAPSHOT_JAR in $${SNAPSHOT_JAR_LIST} ; do \
		echo "finding possible duplicate of $${SNAPSHOT_JAR} ..." ; \
		BASE_JAR_NAME=`echo $${SNAPSHOT_JAR} | sed "s/-SNAPSHOT-tests.jar//g"` ; \
		echo "base JAR name read as $${BASE_JAR_NAME}" ; \
		POSSIBLE_DUPE_LIST=`find ${HADOOP_UTIL_INSTALL_3DIGIT_DIR} -type f -path "$${BASE_JAR_NAME}*tests.jar"` ; \
		for POSSIBLE_DUPE in ${POSSIBLE_DUPE_LIST} ; do \
			if [[ "$${POSSIBLE_DUPE}" != "$${SNAPSHOT_JAR}" ]]; then \
				echo "dupe ${POSSIBLE_DUPE} detected" ; \
				rm -fv ${POSSIBLE_DUPE} ; \
			fi ; \
		done ; \
	done ;
}

prepare_hadoop_client() {
  HADOOP_CLIENT_INSTALL_3DIGIT_DIR="${BUILD_ROOT}/root/hadoop-client${PKG_INSTALL_ROOT}"
	mkdir -pv ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}

	mkdir -pv "${BUILD_ROOT}/root/hadoop-client${INSTALLATION_PREFIX}/roles"
  echo -e "PKG_HOME_DIR=${PKG_INSTALL_ROOT}\nPKG_CONFIG_COMMAND=${PKG_INSTALL_ROOT}/bin/configure.sh\n" > "${BUILD_ROOT}/root/hadoop-client${INSTALLATION_PREFIX}/roles/hadoop-client"

	mkdir -p ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/bin
	mkdir -p ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/share/hadoop
	mkdir -p ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/share/hadoop/yarn
	mkdir -p ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/etc/hadoop
	mkdir -p ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/etc/hadoop/scram
	cat ${WORK_DIR}/ext-conf/configure.sh | sed -e "s|__INSTALL__|${PKG_INSTALL_ROOT}|g;s|__VERSION_3DIGIT__|${PKG_3DIGIT_VERSION}|g;s|__PREFIX__|${INSTALLATION_PREFIX}|g" > ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/bin/configure.sh
	chmod 755 ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/bin/configure.sh
	cat ${WORK_DIR}/ext-conf/scramConfigure.sh | sed -e "s|__INSTALL__|${PKG_INSTALL_ROOT}|g;s|__VERSION_3DIGIT__|${PKG_3DIGIT_VERSION}|g;s|__PREFIX__|${INSTALLATION_PREFIX}|g" > ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/bin/scramConfigure.sh
	chmod 755 ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/bin/scramConfigure.sh
	cat ${WORK_DIR}/ext-conf/hadoop_client_symlinks.sh | sed -e "s|__INSTALL__|${PKG_INSTALL_ROOT}|g;s|__VERSION_3DIGIT__|${PKG_3DIGIT_VERSION}|g;s|__PREFIX__|${INSTALLATION_PREFIX}|g" > ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/bin/hadoop_client_symlinks.sh
	chmod 755 ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/bin/hadoop_client_symlinks.sh
	cp ${WORK_DIR}/ext-conf/hadoop_version_util.sh ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/bin
	cp ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/bin/* ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/bin
	rm -f ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/bin/hadoop
	#remove conf files that already copied to hadoop-util
	cp -r ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/etc/hadoop/* ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/etc/hadoop/.
	cp ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/etc/hadoop/scram-site* ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/etc/hadoop/scram/.
	rm -f ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/etc/hadoop/core-site*
	rm -f ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/etc/hadoop/scram-site*
	rm -f ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/etc/hadoop/log4j*
	rm -f ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/etc/hadoop/hadoop-env*
	cp -r ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/share/hadoop/hdfs ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/share/hadoop/.
	cp -r ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/share/hadoop/tools ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/share/hadoop/.
	cp -r ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/share/hadoop/mapreduce ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/share/hadoop/.
	cp -r ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/share/hadoop/client ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/share/hadoop/.
	cp ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/share/hadoop/yarn/hadoop-yarn-client* ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/share/hadoop/yarn/.
	cp ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/share/hadoop/yarn/hadoop-yarn-common* ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR}/share/hadoop/yarn/.
	find ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR} -name "*.cmd" -exec rm -fv {} \;
	find ${HADOOP_CLIENT_INSTALL_3DIGIT_DIR} -name "*.bat" -exec rm -fv {} \;
}

prepare_hadoop_core() {
  HADOOP_CORE_INSTALL_3DIGIT_DIR="${BUILD_ROOT}/root/hadoop-core${PKG_INSTALL_ROOT}"
  mkdir -pv ${HADOOP_CORE_INSTALL_3DIGIT_DIR}

	mkdir -p ${HADOOP_CORE_INSTALL_3DIGIT_DIR}/bin
	mkdir -p ${HADOOP_CORE_INSTALL_3DIGIT_DIR}/sbin
	mkdir -p ${HADOOP_CORE_INSTALL_3DIGIT_DIR}/share/hadoop/yarn
	cp -r ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/sbin/* ${HADOOP_CORE_INSTALL_3DIGIT_DIR}/sbin/.
	#remove jars that already copied to hadoop-client
	cp -r ${BUILD_ROOT}/distribution/${HADOOP_DEST_NAME}/share/hadoop/yarn/* ${HADOOP_CORE_INSTALL_3DIGIT_DIR}/share/hadoop/yarn/.
	rm -f ${HADOOP_CORE_INSTALL_3DIGIT_DIR}/share/hadoop/yarn/hadoop-yarn-api*
	rm -f ${HADOOP_CORE_INSTALL_3DIGIT_DIR}/share/hadoop/yarn/sources/hadoop-yarn-api*
	rm -f ${HADOOP_CORE_INSTALL_3DIGIT_DIR}/share/hadoop/yarn/hadoop-yarn-client*
	rm -f ${HADOOP_CORE_INSTALL_3DIGIT_DIR}/share/hadoop/yarn/hadoop-yarn-common*
	rm -f ${HADOOP_CORE_INSTALL_3DIGIT_DIR}/share/hadoop/yarn/lib/websocket-{api,client}-*
	find ${HADOOP_CORE_INSTALL_3DIGIT_DIR} -name "*.cmd" -exec rm -fv {} \;
	find ${HADOOP_CORE_INSTALL_3DIGIT_DIR} -name "*.bat" -exec rm -fv {} \;
}

deploy() {
  cd "${WORK_DIR}"
	if [ "${OS}" = "redhat" ]; then \
		mvn -U -Pdist -Pnative -Pyarn-ui -Drequire.snappy=true -Dbundle.snappy=true -Dsnappy.lib=/usr/local/lib/ -Dmaven.javadoc.skip=true -DskipTests -Dtar deploy ; \
	fi ;
	root_path=$(pwd)
	# All platforms need to deploy their own build of the Hadoop 3 distribution and HDFS3 sources
	cd "mapr-devops" && \
		./gradlew publish \
		"${GRADLE_OPTS}" \
		-PhadoopVersion="${ARTIFACTS_VERSION}" \
		-PplatformString="${OS}" \
		-PdistPath="${root_path}/${HADOOP_IMAGE_DIST}/${HADOOP_IMAGE_NAME}" \
		-PHDFS_DIR="${root_path}/hadoop-hdfs-project/hadoop-hdfs-native-client" \
		-PmavenRepo="${MAPR_MAVEN_REPO}" \
		-PmavenUser="${MAPR_MAVEN_USER}" \
    -PmavenPass="${MAPR_MAVEN_PASS}" ;
  cd "${root_path}"
}

main() {
  if [ "${OS}" = "suse" ]; then
    echo "Source proto profile for SUSE"
    source /etc/profile.d/proto-profile.sh
  elif [ "${OS}" = "redhat" ]; then
    echo "Source gcc toolset for Centos"
    source /opt/rh/gcc-toolset-9/enable
  fi
  echo "Cleaning '${BUILD_ROOT}' dir..."
  rm -rf "${BUILD_ROOT}"

  echo "Building project..."
  build_hadoop

  echo "Preparing hadoop util package"
  prepare_hadoop_util
  echo "Preparing hadoop client package"
  prepare_hadoop_client
  echo "Preparing hadoop core package"
  prepare_hadoop_core
  echo "Symlinks for hadoop-util"
  setup_package_links "hadoop-util"

  echo "Preparing roles packages..."
  echo "Resourcemanager service"
  setup_role_package "resourcemanager"
  echo "Nodemanager service"
  setup_role_package "nodemanager"
  echo "Historyserver service"
  setup_role_package "historyserver"
  echo "Httpfs service"
  setup_role_package "httpfs"
  echo "Timelineserver service"
  setup_role_package "timelineserver"
  echo "Timelineserverv1 service"
  setup_role_package "timelineserverv1"

  echo "Building packages..."
  build_package "hadoop-util"
  build_package "hadoop-client"
  build_package "hadoop-core"
  build_package "resourcemanager"
  build_package "nodemanager"
  build_package "historyserver"
  build_package "httpfs"
  build_package "timelineserver"
  build_package "timelineserverv1"

  echo "Deploy artifacts"
  deploy

  echo "Resulting packages:"
  find "${DIST_DIR}" -exec readlink -f {} \;
}

main
