# Creating a new tsdat release
This document describes the steps to create a new release of tsdat.
We are including instructions for now until an automated build is set up.


### Prereq: Make sure that you have twine installed
```bash
pip install twine
```

### 1) Update the version numbers
1. setup.py
2. docker/docker-compose.yml
3. docker/build.sh

Then commit tsdat with the new build numbers.

### 2) Deploy the new release to pypi.

```bash
cd tsdat
python setup.py sdist bdist_wheel
twine upload dist/*
```

### 3) Deploy the new release to docker hub

**Prereq: Log in to docker hub**
```bash
docker login --username=clansing
```

```bash
cd tsdat/docker
./build.sh
```

### 4) Create a tagged release in GitHub