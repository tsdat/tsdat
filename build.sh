#!/bin/sh

export PKG_CONFIG_PATH=$CONDA_PREFIX/lib/pkgconfig
export TSDAT_VERSION='test'
python setup.py build_ext --inplace