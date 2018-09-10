#!/bin/bash -xe

sudo yum-config-manager --enable epel epel-source epel-debuginfo -y
sudo yum repolist -y
sudo su
curl https://packages.microsoft.com/config/rhel/6/prod.repo > /etc/yum.repos.d/mssql-release.repo
sudo ACCEPT_EULA=Y yum install msodbcsql -y
sudo yum install postgresql-odbc.x86_64 -y
sudo yum install gcc-c++ -y
sudo yum install python-devel -y
sudo yum install unixODBC-devel -y

# Non-standard and non-Amazon Machine Image Python modules:
sudo pip install -U \
  awscli \
  boto3 \
  shapely \
  pandas \
  pyodbc \
  pymssql
