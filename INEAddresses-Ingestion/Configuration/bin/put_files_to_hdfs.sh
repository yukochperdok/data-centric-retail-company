#!/usr/bin/env bash

# INPUT VARS
FILES_PATH=$1
FILE_WITH_FILES_TO_PROCESS=$2
HDFS_PATH=$3
DATE=$4
PARALLELIZATION_HDFS=$5

# INTERNAL VARS
USER=`whoami`
HDFS_OUTPUT_PATH=${HDFS_PATH}/${DATE}
OK=0
KO=1
KO_XARGS=255

# FUNCTIONS
function validateParameters() {
  if [[ -z ${FILES_PATH} ]] || [[ -z ${FILE_WITH_FILES_TO_PROCESS} ]] || [[ -z ${HDFS_PATH} ]] || [[ -z ${DATE} ]] || [[ -z ${PARALLELIZATION_HDFS} ]]; then
      echo "ERROR - Please, this script must be called with the following arguments: FILES_PATH FILE_WITH_FILES_TO_PROCESS HDFS_PATH DATE PARALLELIZATION_HDFS"
      exit ${KO}
  else
      echo "INFO - The script has been called with the following arguments: ${FILES_PATH} ${FILE_WITH_FILES_TO_PROCESS} ${HDFS_PATH} ${DATE} ${PARALLELIZATION_HDFS}"
  fi
}

function createDirectoryInHDFS() {
  hdfs dfs -mkdir -p ${HDFS_OUTPUT_PATH}

  if [[ $? -ne ${OK} ]]; then
    echo "ERROR - Could not create directory '${HDFS_OUTPUT_PATH}' in HDFS, check connectivity and permissions"
    echo "Process finished with ERROR."
    exit ${KO}
  fi
}

function putFilesToHDFS() {
  export -f getKerberosTicket
  export HDFS_OUTPUT_PATH
  export FILES_PATH
  export KO
  export OK
  export KO_XARGS

  < ${FILES_PATH}/${FILE_WITH_FILES_TO_PROCESS} xargs -n 1 -P ${PARALLELIZATION_HDFS} -I '{}' bash -c 'echo "uploading ${FILES_PATH}/"{}""; hdfs dfs -put -f ${FILES_PATH}/"{}" ${HDFS_OUTPUT_PATH}/ || exit ${KO_XARGS}' bash
  if [[ $? -ne ${OK} ]]; then
    echo "ERROR - Could not put files to hdfs"
    echo "Process finished with ERROR."
    exit ${KO}
  fi

  echo "INFO - HDFS Put Finished"
}

function getKerberosTicket() {

  kinit -f -l 300m -k -t ${HOME}/${USER}.keytab ${USER}@DOMAIN

  if [[ $? -ne ${OK} ]]; then
    echo "ERROR - Could not get kerberos ticket, check keytab file, user and connectivity"
    echo "Process finished with ERROR."
    exit ${KO}
  fi
}

######################## SCRIPT START ###############################

validateParameters

getKerberosTicket

createDirectoryInHDFS

putFilesToHDFS

echo "Process finished with SUCCESS."
exit ${OK}
