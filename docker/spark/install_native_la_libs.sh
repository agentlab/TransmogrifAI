#!/usr/bin/env bash

if [ -z $USE_NATIVE_LA_LIBS ]; then
  echo "No native linear algebra libraries need to be installed. Exiting"
  exit 0
else
  echo "Installing OpenBLAS and libgfortran"
  apt-get update &&
    apt-get install -y --no-install-recommends wget libatlas3-base libopenblas-base &&
    rm -rf /var/lib/apt/lists/* &&
    cd /tmp &&
    # see https://gist.github.com/sakethramanujam/faf5b677b6505437dbdd82170ac55322
    wget -q http://archive.ubuntu.com/ubuntu/pool/universe/g/gcc-6/gcc-6-base_6.4.0-17ubuntu1_amd64.deb &&
    dpkg -i gcc-6-base_6.4.0-17ubuntu1_amd64.deb &&
    wget -q http://archive.ubuntu.com/ubuntu/pool/universe/g/gcc-6/libgfortran3_6.4.0-17ubuntu1_amd64.deb &&
    dpkg -i libgfortran3_6.4.0-17ubuntu1_amd64.deb

  if [ $? -ne 0 ]; then
    "Failed to install libraries"
    exit 1
  fi
fi

exit 0
