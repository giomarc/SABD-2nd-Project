FROM flink:1.8-scala_2.11

USER root

# # hadoop
RUN wget http://mirror.nohup.it/apache/hadoop/common/hadoop-3.1.2/hadoop-3.1.2.tar.gz ; tar -zxf hadoop-3.1.2.tar.gz -C /usr/local/ ; rm hadoop-3.1.2.tar.gz
RUN cd /usr/local && ln -s ./hadoop-3.1.2 hadoop

ENV HADOOP_COMMON_HOME /usr/local/hadoop
ENV HADOOP_HDFS_HOME /usr/local/hadoop
ENV HADOOP_HOME /usr/local/hadoop
ENV HADOOP_MAPRED_HOME /usr/local/hadoop
ENV HADOOP_YARN_HOME /usr/local/hadoop
ENV HADOOP_CONF_DIR /usr/local/hadoop/etc/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin
ENV HDFS_NAMENODE_USER "root"
ENV HDFS_DATANODE_USER "root"
ENV HDFS_SECONDARYNAMENODE_USER "root"

ADD hdfs-config/core-site.xml $HADOOP_CONF_DIR/core-site.xml
ADD hdfs-config/hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml

RUN chmod 777 $HADOOP_CONF_DIR/core-site.xml
RUN chmod 777 $HADOOP_CONF_DIR/hdfs-site.xml

COPY conf/docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh

COPY external_jar_lib/*.jar $FLINK_HOME/lib/

ENTRYPOINT ["/docker-entrypoint.sh"]
EXPOSE 6123 8081
CMD ["help"]