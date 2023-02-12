For windows development, use WSL.
From cmd prompt, run 'wsl install'
Fix DNS resolv.conf file

Install C compiler into WSL for Cython
sudo apt-get install build-essential

Install pkg-config
sudo apt-get install -y pkg-config

Set PKG_CONFIG_PATH env variable
 export PKG_CONFIG_PATH=~/anaconda3/envs/tsdat/lib/pkgconfig


Building cython bindings locally:
python setup.py build_ext --inplace

TODO: add TSDAT_VERSION and PKG_CONFIG_PATH to .bashrc

TODO: How to get PyCharm to find C headers so you won't get errors in the
cython pyx files?  I tried to attach the include/ folder from
my anaconda environment, but that did not work.
