# This script is usefull to debug things in TeamCity
echo "Where are we? on host <$(hostname)> at <$(pwd)>"
echo "What files were copied ?"
tree -L 2 .
echo "Python versions:"
echo "Python 2: <$(python2 --version)> at <$(which python2)>"
echo "pip: <$(which pip)>"
echo "virtualenv: <$(which virtualenv)>"
echo "Python 3: <$(python3 --version)> at <$(which python3)>"
echo "Some environment variables:"
echo "JAVA_HOME: <$JAVA_HOME>"
echo "PATH: <$PATH>"
echo "SPARK_HOME: <$SPARK_HOME>"
echo "Available disk space:"
df -h
echo "Listing directories: "
tree -L 1 /
tree -L 1 /bin
tree -L 1 /soft
tree -L 1 /usr
