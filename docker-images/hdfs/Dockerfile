# Creates pseudo distributed hadoop 3.1.2
#
# docker build -t effeerre/hadoop .

FROM effeerre/hadoop
USER root

RUN apt-get update;
RUN apt-get install nano;

# HDFS CONFIG FILES
ADD hdfs-config/hadoop-env.sh $HADOOP_HOME/etc/hadoop/hadoop-env.sh
ADD hdfs-config/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
ADD hdfs-config/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
ADD hdfs-config/mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml
ADD hdfs-config/workers $HADOOP_HOME/etc/hadoop/workers


# BOOTSTRAP SERVICE SCRIPT
RUN mkdir /sabd

ADD start-services.sh /sabd
RUN chmod 700 /sabd/start-services.sh
ADD stop-services.sh /sabd
RUN chmod 700 /sabd/stop-services.sh


ADD hdfs-config/bootstrap.sh /usr/local/bootstrap.sh
RUN chown root:root /usr/local/bootstrap.sh
RUN chmod 700 /usr/local/bootstrap.sh
ENV BOOTSTRAP /usr/local/bootstrap.sh
CMD /usr/local/bootstrap.sh

# # HDFS ports
EXPOSE 9866 9867 9870 9864 9868 9820 9000
#
# SSH port
EXPOSE 22