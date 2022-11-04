#!/bin/bash
# This gets fillled out at package time
HADOOP_HOME="${HADOOP_HOME:-__PREFIX_INSTALL__/hadoop/hadoop-__VERSION_3DIGIT__}"
HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
HADOOP_SCRAM_CONF_DIR=${HADOOP_CONF_DIR}/scram
HADOOP_SCRAM_CONF=${HADOOP_SCRAM_CONF_DIR}/scram-site.xml
HADOOP_SSL_CONFIG=${HADOOP_CONF_DIR}/ssl-server.xml
MAPR_HOME=${MAPR_HOME:-__PREFIX_INSTALL__}
MAPR_CLUSTRERS_CONF_FILE=${MAPR_HOME}/conf/mapr-clusters.conf

isFips="false"
serviceName=""
#default path
YARN_PATH="/var/mapr/cluster/yarn"

function isSecureEnable() {
    local  __resultvar=$1
    local result=$(head -1 ${MAPR_CLUSTRERS_CONF_FILE} | grep -o 'secure=\w*' | cut -d= -f2)
    eval $__resultvar="'$result'"
}

function isFipsConfigured() {
    if [ ! -f $HADOOP_SSL_CONFIG ]; then
        echo "File $HADOOP_SSL_CONFIG does not exist"
        exit 1
    fi
    #
    # Gets the key store type
    #
    keyStoreType=`awk '/ssl.server.keystore.type/{getline; print}' "$HADOOP_SSL_CONFIG" |sed 's/\s*<value>\(.*\)<\/value>/\1/'`
    if [ "$keyStoreType" != "bcfks" ]; then
        isFips="false"
        return
    fi
    #
    # Gets the trust store type
    #
    trustStoreType=`awk '/ssl.server.truststore.type/{getline; print}' "$HADOOP_SSL_CONFIG" |sed 's/\s*<value>\(.*\)<\/value>/\1/'`
    if [ "$trustStoreType" != "bcfks" ]; then
        isFips="false"
        return
    fi
    #
    # If we get here, then both key and trust stores are BCFKS stores
    isFips="true"
    return
}


function ConfigureScram() {
    isSecureEnable isSecure
    if [ "$isSecure" = "true"  ] && [ -f "${MAPR_HOME}/conf/mapruserticket" ]; then
        export MAPR_TICKETFILE_LOCATION="${MAPR_HOME}/conf/mapruserticket"
    fi
    if [ "$isFips" != "true" ]; then
        credType="jceks"
    else
        credType="bcfks"
    fi
    SCRAM_CREDS_FILE=$HADOOP_SCRAM_CONF_DIR/scram.${credType}
    SCRAM_CREDS_FILE_WITH_STORE=local${credType}://file$SCRAM_CREDS_FILE
    CLEAR_PASS=$HADOOP_SCRAM_CONF_DIR/scram-password.txt
    if [ ! -f $SCRAM_CREDS_FILE ];then
        hadoop fs -test -f $YARN_PATH/scram.${credType}
        if [ $? -ne 0 ]; then
            if [ "$serviceName" == "resourceManager" ];then
                local SCRAM_PASSW=`maprcli security genkey 2>/dev/null | head -c8`
                hadoop credential create scram.password -value $SCRAM_PASSW -provider $SCRAM_CREDS_FILE_WITH_STORE &> /dev/null
                echo "$SCRAM_PASSW" > $CLEAR_PASS
                chmod 400 $CLEAR_PASS
                hadoop fs -put -p $SCRAM_CREDS_FILE $YARN_PATH
                hadoop fs -put -p $CLEAR_PASS $YARN_PATH
                rm -f $CLEAR_PASS
            else
                hadoop fs -test -f $YARN_PATH/scram-password.txt
                if [ $? -eq 0 ]; then
                    local SCRAM_PASSW=`hadoop fs -cat $YARN_PATH/scram-password.txt`
                    hadoop credential create scram.password -value $SCRAM_PASSW -provider $SCRAM_CREDS_FILE_WITH_STORE &> /dev/null
                    hadoop fs -put -p $SCRAM_CREDS_FILE $YARN_PATH
                else
                    echo "Please check that RM is started and generated SCRAM password."
                    exit 1
                fi
            fi
        else
          hadoop fs -copyToLocal -p $YARN_PATH/scram.${credType} $SCRAM_CREDS_FILE
        fi
    fi
    if ! grep -q "hadoop.security.credential.provider.path" "$HADOOP_SCRAM_CONF"; then
        sed -i -e "s|</configuration>|  <property>\n    <name>hadoop.security.credential.provider.path</name>\n    <value>${SCRAM_CREDS_FILE_WITH_STORE}</value>\n  </property>\n</configuration>|" $HADOOP_SCRAM_CONF
    fi
    chmod 644 $HADOOP_SCRAM_CONF_DIR/*
}

while (($#)); do
    case "$1" in
        -service)
            serviceName=$2
            shift 2
            ;;
        -path)
            YARN_PATH=$2
            shift 2
            ;;
        --help | -h)
            echo -e ${usage}
            return 2 2>/dev/null || exit 2
            ;;
        --)
            shift
            ;;
        *)
            echo "Unknown option $1"
            return 2 2>/dev/null || exit 2
            ;;
    esac
done

isFipsConfigured

ConfigureScram
