#!/bin/sh

export PKG_CONFIG_PATH=$CONDA_PREFIX/lib/pkgconfig
export TSDAT_VERSION='1.0'
python setup.py build_ext --inplace