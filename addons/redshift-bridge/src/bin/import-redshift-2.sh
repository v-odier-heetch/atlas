#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#
# resolve links - $0 may be a softlink
PRG="${0}"

[[ `uname -s` == *"CYGWIN"* ]] && CYGWIN=true

while [ -h "${PRG}" ]; do
  ls=`ls -ld "${PRG}"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "${PRG}"`/"$link"
  fi
done

BASEDIR=`dirname ${PRG}`
BASEDIR=`cd ${BASEDIR}/..;pwd`

if test -z "${JAVA_HOME}"
then
    JAVA_BIN=`which java`
    JAR_BIN=`which jar`
else
    JAVA_BIN="${JAVA_HOME}/bin/java"
    JAR_BIN="${JAVA_HOME}/bin/jar"
fi
export JAVA_BIN

if [ ! -e "${JAVA_BIN}" ] || [ ! -e "${JAR_BIN}" ]; then
  echo "$JAVA_BIN and/or $JAR_BIN not found on the system. Please make sure java and jar commands are available."
  exit 1
fi

# Construct Atlas classpath using jars from hook/redshift/atlas-redshift-plugin-impl/ directory.
for i in "${BASEDIR}/hook/redshift/atlas-redshift-plugin-impl/"*.jar; do
  ATLASCPPATH="${ATLASCPPATH}:$i"
done

if [ -z "${ATLAS_CONF_DIR}" ] && [ -e /etc/atlas/conf ];then
    ATLAS_CONF_DIR=/etc/atlas/conf
fi
ATLASCPPATH=${ATLASCPPATH}:${ATLAS_CONF_DIR}

# log dir for applications
ATLAS_LOG_DIR="${ATLAS_LOG_DIR:-$BASEDIR/logs}"
export ATLAS_LOG_DIR
LOGFILE="$ATLAS_LOG_DIR/import-redshift.log"

TIME=`date +%Y%m%d%H%M%s`

#Add redshift conf in classpath
if [ ! -z "$REDSHIFT_CONF_DIR" ]; then
    REDSHIFT_CONF=$REDSHIFT_CONF_DIR
elif [ ! -z "$REDSHIFT_HOME" ]; then
    REDSHIFT_CONF="$REDSHIFT_HOME/conf"
elif [ -e /etc/redshift/conf ]; then
    REDSHIFT_CONF="/etc/redshift/conf"
else
    echo "Could not find a valid REDSHIFT configuration"
    exit 1
fi

echo Using Redshift configuration directory ["$REDSHIFT_CONF"]


if [ -f "${REDSHIFT_CONF}/redshift-env.sh" ]; then
  . "${REDSHIFT_CONF}/redshift-env.sh"
fi

if [ -z "$REDSHIFT_HOME" ]; then
    if [ -d "${BASEDIR}/../redshift" ]; then
        REDSHIFT_HOME=${BASEDIR}/../redshift
    else
        echo "Please set REDSHIFT_HOME to the root of REDSHIFT installation"
        exit 1
    fi
fi

REDSHIFT_CP="${REDSHIFT_CONF}"

for i in "${REDSHIFT_HOME}/lib/"*.jar; do
    REDSHIFT_CP="${REDSHIFT_CP}:$i"
done

CP="${REDSHIFT_CP}:${ATLASCPPATH}"

# If running in cygwin, convert pathnames and classpath to Windows format.
if [ "${CYGWIN}" == "true" ]
then
   ATLAS_LOG_DIR=`cygpath -w ${ATLAS_LOG_DIR}`
   LOGFILE=`cygpath -w ${LOGFILE}`
   REDSHIFT_CP=`cygpath -w ${REDSHIFT_CP}`
   CP=`cygpath -w -p ${CP}`
fi

JAVA_PROPERTIES="$ATLAS_OPTS -Datlas.log.dir=$ATLAS_LOG_DIR -Datlas.log.file=import-redshift.log
-Dlog4j.configuration=atlas-redshift-import-log4j.xml"

IMPORT_ARGS=
JVM_ARGS=

while true
do
  option=$1
  shift

  case "$option" in
    -d) IMPORT_ARGS="$IMPORT_ARGS -d $1"; shift;;
    -t) IMPORT_ARGS="$IMPORT_ARGS -t $1"; shift;;
    -f) IMPORT_ARGS="$IMPORT_ARGS -f $1"; shift;;
    --database) IMPORT_ARGS="$IMPORT_ARGS --database $1"; shift;;
    --table) IMPORT_ARGS="$IMPORT_ARGS --table $1"; shift;;
    --filename) IMPORT_ARGS="$IMPORT_ARGS --filename $1"; shift;;
    "") break;;
    *) JVM_ARGS="$JVM_ARGS $option"
  esac
done

JAVA_PROPERTIES="${JAVA_PROPERTIES} ${JVM_ARGS}"

echo "Log file for import is $LOGFILE"

"${JAVA_BIN}" ${JAVA_PROPERTIES} -cp "${CP}" org.apache.atlas.redshift.bridge.RedshiftBridge $IMPORT_ARGS

RETVAL=$?
[ $RETVAL -eq 0 ] && echo Redshift Meta Data imported successfully!!!
[ $RETVAL -ne 0 ] && echo Failed to import Redshift Meta Data!!!

exit $RETVAL

