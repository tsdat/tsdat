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
# TODO: this should be automated with CICD build to create new version
# of docker image every time a new version is released.

# Build the image
docker build -f ./Dockerfile -t tsdat/tsdat-lambda:latest .

# Push to docker hub
# Make sure to log in first:
#     docker login --username=clansing
docker push tsdat/tsdat-lambda:latest




