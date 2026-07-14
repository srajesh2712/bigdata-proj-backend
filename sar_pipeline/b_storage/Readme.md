# 1. Point to your actual Java 21 installation
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# 2. Point to your local Hadoop folder
export HADOOP_HOME=$HOME/Documents/summer-project/hadoop-3.4.1-bin

# 3. Tell PyArrow where the native HDFS library is
export ARROW_LIBHDFS_DIR=$HADOOP_HOME/lib/native

# 4. Link the JVM and Hadoop libraries (Critical for the .so loader)
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$JAVA_HOME/lib/server:$LD_LIBRARY_PATH

# 5. Generate the Classpath (This should work perfectly now)
export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath --glob)

# 6. Set the Hadoop User
export HADOOP_USER_NAME=btcchl0040
