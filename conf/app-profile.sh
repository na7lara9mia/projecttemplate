##############################################################################
# ENVIRONMENT VARIABLES
##############################################################################
export CONF="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
export REPO="$( dirname $CONF )"
export BIN="$REPO/bin"
export DATA="$REPO/data"
export HISTO="$REPO/histo"
echo "Loading app-profile.sh from this repository: $CONF"

# This folder should contain the source code of the application
# (this is the relative folder in the BIN)
export PACKAGE_DIR="distribution_cost"

if [ ! -e $BIN/$PACKAGE_DIR ]
then
	echo "ERROR! python package '$PACKAGE_DIR' not found in the '$BIN' directory"
	return
fi


##############################################################################
# GENERAL CONFIGURATION
##############################################################################
export LANG=en_US.utf8
export TMOUT=0

# Oracle profile
if [ -e /soft/ora1210/fileso/profile ]
then
	. /soft/ora1210/fileso/profile
fi

# Git
export PATH=/soft/git/bin:$PATH

# This folder contains the correct version of make that we want to use
export PATH=/usr/bin/:$PATH


##############################################################################
# PYTHON SPECIFIC CONFIGURATION
##############################################################################
# Add virtualenv binary to the PATH
export PATH=$PATH:$REPO/.venv3/bin/

# Add the repo to the PYTHONPATH
export PYTHONPATH=$BIN:$PYTHONPATH

# Artifactory PSA pypi repository
# NOTE: 'pypi-virtual' contains both pypi libraries and PSA-specific libraries
export PIP_OPTIONS="-i http://repository.inetpsa.com/api/pypi/pypi-virtual/simple --trusted-host repository.inetpsa.com"

# Required library and compiler options to install Jupyter
export LD_LIBRARY_PATH=/gpfs/user/common/jupyter/sqlite/sqlite/lib:$LD_LIBRARY_PATH
export CPPFLAGS="-I /gpfs/user/common/jupyter/sqlite/sqlite/include -L /gpfs/user/common/jupyter/sqlite/sqlite/lib"

# Activate the virtual environment
if [ -e $REPO/.venv3/bin/activate ]
then
	source $REPO/.venv3/bin/activate
fi


##############################################################################
# SPARK SPECIFIC CONFIGURATION
##############################################################################
# Spark version
export SPARK_MAJOR_VERSION=2

# Add SPARK_HOME to PYTHONPATH to be able to execute Spark jobs through Python
export SPARK_HOME=/usr/hdp/current/spark2-client
export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python/

# Python3 support on HDP (solving 'print rack' issue)
export HADOOP_CONF_DIR=/etc/hadoop_spark/conf

# Pyspark: specify python binary
export PYSPARK_PYTHON=$REPO/.venv3/bin/python
export PYSPARK_DRIVER_PYTHON=$REPO/.venv3/bin/python

# The log4j.properties file tells Spark to redirect its logging to specific files in the repo
# instead of logging to the console (which gets mixed up with the application logging)
export SPARK_OPTIONS="\
		--driver-java-options '-Dlog4j.configuration=file:$BIN/log4j.properties' \
	"
