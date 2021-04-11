# TSDAT Docker Container
This folder contains the dockerfile definition and build script for an AWS-Lambda
base image for TSDAT.  It extends Amazon's public.ecr.aws/lambda/python:3.8 base image.

See here for the base image's Dockerfile:
 https://github.com/aws/aws-lambda-base-images/blob/python3.8/Dockerfile.python3.8

To start an instance of the public.ecr.aws/lambda/python:3.8 base image in interactive
mode, run this:

```bash
winpty docker run --rm -it --entrypoint bash public.ecr.aws/lambda/python:3.8
```

**Note:**  Other docker containers may be added in the future.


