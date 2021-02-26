# TSDAT Docker Container
This folder contains the dockerfile definition and build script for an AWS-Lambda
base image for TSDAT.  

It is currently underdevelopment using a miniconda and a multistage docker build to
add the required python libraries to the public.ecr.aws/lambda/python:3.8 base image.
** It does not work yet. **


To start an instance of the public.ecr.aws/lambda/python:3.8 base image in interactive
mode, run this:

```bash
winpty docker run --rm -it --entrypoint bash public.ecr.aws/lambda/python:3.8
```