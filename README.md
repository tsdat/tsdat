#Time Series Data Library
This library provides general utility methods for working with 
time series datasets, which are stored as Xarray Dataset objects.
In particular, it will provide declarative methods for being able
standardize, apply Q/C checks, correct, and transform datastreams
as a whole, reducing the amount of coding required for data
processing.

# Installation
This library depends on the ARM ACT library which will be used
for plotting and data standardization.  You can install it via
pip, but it has problems on Windows because some of the 
dependencies require C code to be built.  It's way easier to 
install the environment via Anaconda, which is described below.
If you do not want to use Anaconda, you can install the tsdat
requirements via:

```bash
pip3 install -r requirements.txt
```

## 1) Install Anaconda
We recommend using Anaconda to install the required Python environment.
because some of our plotting dependencies
require libraries that are difficult to set up on windows machines.

https://www.anaconda.com/download/#


## 2) Start the Anaconda Prompt
![image info](./doc/win-anaconda-prompt2.png)

## 3) Create Anaconda Environment
```bash
conda create -n mhkit_act_env -c conda-forge python=3.7 act-atmos
```
