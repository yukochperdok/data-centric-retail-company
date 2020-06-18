#!/usr/bin/env bash

# INPUT VARS
FILES_PATH=$1
TRAM_PREFIX=$2
VIAS_PREFIX=$3
OUTPUT_FILE=$4
OUTPUT_FILE_TO_REMOVE=$5

# INTERNAL VARS
OUTPUT_FILE_WITH_PATH="${FILES_PATH}/${OUTPUT_FILE}"
OUTPUT_FILE_TO_REMOVE_WITH_PATH="${FILES_PATH}/${OUTPUT_FILE_TO_REMOVE}"
OK=0
KO=1

# CONFIGURATION
set -o pipefail

# FUNCTIONS
function checkInputParameters() {

  if [[ -z ${FILES_PATH} ]] || [[ -z ${TRAM_PREFIX} ]] || [[ -z ${OUTPUT_FILE} ]] || [[ -z ${OUTPUT_FILE_TO_REMOVE} ]] || [[ -z ${VIAS_PREFIX} ]]; then
      echo "ERROR - Please, this script must be called with the following arguments: FILES_PATH TRAM_PREFIX OUTPUT_FILE OUTPUT_FILE_TO_REMOVE and VIAS_PREFIX"
      exit ${KO}
  else
      echo "INFO - The script has been called with the following arguments: ${FILES_PATH} ${TRAM_PREFIX} ${OUTPUT_FILE} ${OUTPUT_FILE_TO_REMOVE} ${VIAS_PREFIX}"
  fi

}

function searchFileToProcess() {

  # find the most recent files
  FILE_WITH_PREFIX_TRAM=$(find ${FILES_PATH} -maxdepth 1 -type f -name ${TRAM_PREFIX} -printf "%f\n" | sort -nr | head -n 1)
  FILE_WITH_PREFIX_VIAS=$(find ${FILES_PATH} -maxdepth 1 -type f -name ${VIAS_PREFIX} -printf "%f\n" | sort -nr | head -n 1)

  # find all files to remove
  FILES_TO_REMOVE_TRAM=$(find ${FILES_PATH} -maxdepth 1 -type f -name ${TRAM_PREFIX} -printf "%f\n")
  FILES_TO_REMOVE_VIAS=$(find ${FILES_PATH} -maxdepth 1 -type f -name ${VIAS_PREFIX} -printf "%f\n")

  if [[ -n ${FILE_WITH_PREFIX_TRAM} ]] && [[ -n ${FILE_WITH_PREFIX_VIAS} ]] ; then
    printf "INFO - Files found in path: \n${FILE_WITH_PREFIX_TRAM}\n${FILE_WITH_PREFIX_VIAS}"
    echo -e "${FILE_WITH_PREFIX_TRAM}\n${FILE_WITH_PREFIX_VIAS}" > ${OUTPUT_FILE_WITH_PATH}
    echo -e "${FILES_TO_REMOVE_TRAM}\n${FILES_TO_REMOVE_VIAS}" > ${OUTPUT_FILE_TO_REMOVE_WITH_PATH}
  else
    printf "ERROR - There are no files to process\n"
    > ${OUTPUT_FILE_WITH_PATH}
    > ${OUTPUT_FILE_TO_REMOVE_WITH_PATH}
    exit ${KO}
  fi

}

######################## SCRIPT START ###############################

checkInputParameters

searchFileToProcess

echo "Process finished with SUCCESS."
exit ${OK}
