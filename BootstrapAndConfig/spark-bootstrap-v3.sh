#!/bin/bash -xe

# Non-standard and non-Amazon Machine Image Python modules:
sudo python3.4 -m pip install -U \
awscli \
boto3 \
pandas \
numpy
  
sudo python3 -m pip install -U \
awscli \
boto3 \
pandas \
numpy
  
sudo pip install -U \
awscli \
boto3 \
pandas \
numpy

  