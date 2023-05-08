FROM ubuntu:22.04

# Install cython build requirements
RUN apt-get update && \
    apt-get install -y git && \
    apt-get install -y build-essential && \
    apt-get install -y pkg-config && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install mamba
RUN apt-get update && \
    apt-get install -y wget && \
    wget https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-Linux-x86_64.sh && \
    bash Mambaforge-Linux-x86_64.sh -b && \
    rm Mambaforge-Linux-x86_64.sh && \
    echo "export PATH=/root/mambaforge/bin:\$PATH" >> ~/.bashrc && \
    /root/mambaforge/bin/mamba init bash

# Set up PATH environment variable for mamba
ENV PATH=/root/mambaforge/bin:$PATH

# Copy the mamba environment file into the container
RUN mkdir /app
COPY environment.yml /app/

# Set up working directory
WORKDIR /app

# Create mamba environment
RUN mamba env create -f environment.yml && \
    echo "mamba activate tsdat" >> ~/.bashrc

SHELL ["mamba", "run", "-n", "tsdat", "/bin/bash", "-c"]

WORKDIR /workspaces/tsdat

# Install pip requirements
RUN pip install -e ".[dev]"

CMD ["tail", "-f", "/dev/null"]
