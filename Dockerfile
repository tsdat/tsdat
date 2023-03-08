FROM ubuntu:22.04

# Install cython build requirements
RUN apt-get update && \
    apt-get install -y build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
# TODO: pkg-config

# Install conda
RUN apt-get update && \
    apt-get install -y wget && \
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash Miniconda3-latest-Linux-x86_64.sh -b && \
    rm Miniconda3-latest-Linux-x86_64.sh && \
    echo "export PATH=/root/miniconda3/bin:\$PATH" >> ~/.bashrc && \
    /root/miniconda3/bin/conda init bash

# Set up PATH environment variable for conda
ENV PATH=/root/miniconda3/bin:$PATH

# Copy the conda environment file into the container
RUN mkdir /app
COPY conda_environment.yml /app/

# Create conda environment
RUN conda env create -f /app/conda_environment.yml && \
    echo "conda activate tsdat" >> ~/.bashrc

# Set up working directory
WORKDIR /app

CMD ["tail", "-f", "/dev/null"]

# Command to mount is:
# docker run -it -v /Users/levi260/sandbox/tsdat:/app my-tsdat-image
