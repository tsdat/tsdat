#!/bin/bash
###################################################################
# Script Name:  build.sh
# Description:  Script to build image locally with same tag used
#               by the remote registry so we can test changes
#               locally before committing.
#
# Prerequisite: You must log into gitlab.com before you can run
#               this script:
#               docker login registry.gitlab.com
#
###################################################################
docker build -t registry.gitlab.com/gov-doe-arm/mplcmask .

docker push registry.gitlab.com/gov-doe-arm/mplcmask