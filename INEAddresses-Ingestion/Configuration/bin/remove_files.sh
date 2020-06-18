#!/usr/bin/env bash

# INPUT VARS
FILES_PATH=$1
FILE_WITH_FILES_TO_PROCESS=$2

# INTERNAL VARS
USER=`whoami`
OK=0
KO=1
KO_XARGS=255

# FUNCTIONS
function validateParameters() {
  if [[ -z ${FILES_PATH} ]] || [[ -z ${FILE_WITH_FILES_TO_PROCESS} ]]; then
      echo "ERROR - Please, this script must be called with the following arguments: FILES_PATH FILE_WITH_FILES_TO_PROCESS"
      exit ${KO}
  else
      echo "INFO - The script has been called with the following arguments: ${FILES_PATH} ${FILE_WITH_FILES_TO_PROCESS}"
  fi
}

function removeFilesInFileFromPath() {
  export FILES_PATH
  export KO_XARGS
  export KO

  echo "INFO - Checked permissions of files"

  < ${FILES_PATH}/${FILE_WITH_FILES_TO_PROCESS} xargs -I '{}' bash -c 'echo "removing ${FILES_PATH}/"{}""; rm ${FILES_PATH}/"{}"' bash

  if [[ $? -ne ${OK} ]]; then
    echo "ERROR - Could not remove files from ${FILES_PATH}, check existence and permissions of files included in ${FILE_WITH_FILES_TO_PROCESS}"
    echo "Process finished with ERROR."
    exit ${KO}
  fi

  echo "INFO - Finished removing files from path ${FILES_PATH}"
}


######################## SCRIPT START ###############################

validateParameters

removeFilesInFileFromPath

echo "Process finished with SUCCESS."
exit ${OK}
