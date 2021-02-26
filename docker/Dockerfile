#---------------------------------------------
# Dockerfile to pre-package tsdat requirements
# for lambda container.  This is a simple
# install using conda, but the image is larger.
# We need conda for now because installing
# the cartopy dependencies for act-atmos is
# really complicated.
#---------------------------------------------
ARG FUNCTION_DIR="/var/task/"
ARG ENV_DIR="/opt/conda/envs/tsdat_env/lib/python3.8/site-packages"

FROM continuumio/miniconda3 as build-image

RUN conda create -n tsdat_env -c conda-forge python=3.8 act-atmos cfunits yamllint

# TODO: use conda-pack to extract the conda libs to a folder
# that can be moved to other container
# https://pythonspeed.com/articles/conda-docker-image-size/

# Multi-stage build: grab a fresh copy of the base image
# Note that this image contains the /lambda-entrypoint.sh file
# https://github.com/aws/aws-lambda-base-images/blob/python3.8/Dockerfile.python3.8
FROM public.ecr.aws/lambda/python:3.8

# Include global args in this stage of the build
ARG FUNCTION_DIR
ARG ENV_DIR

# Copy in the build image dependencies
COPY --from=build-image ${ENV_DIR} ${FUNCTION_DIR}

# Add tsdat package
COPY tsdat ${FUNCTION_DIR}

# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}



