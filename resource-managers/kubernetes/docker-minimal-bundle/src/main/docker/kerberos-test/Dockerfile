#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM spark-base

# If this docker file is being used in the context of building your images from a Spark distribution, the docker build
# command should be invoked from the top level directory of the Spark distribution. E.g.:
# docker build -t kerberos-test:latest -f dockerfiles/kerberos-test/Dockerfile .

COPY examples /opt/spark/examples
RUN apk add --no-cache --update krb5 krb5-libs
COPY hconf/krb5.conf /etc/krb5.conf
COPY test-scripts/test-env.sh /

CMD /opt/spark/bin/spark-submit \
      --deploy-mode cluster \
      --class ${CLASS_NAME} \
      --master k8s://${MASTER_URL} \
      --kubernetes-namespace ${NAMESPACE} \
      --conf spark.executor.instances=1 \
      --conf spark.app.name=spark-hdfs \
      --conf spark.kubernetes.driver.docker.image=spark-driver:latest \
      --conf spark.kubernetes.executor.docker.image=spark-executor:latest \
      --conf spark.kubernetes.initcontainer.docker.image=spark-init:latest \
      --conf spark.kubernetes.kerberos=true \
      --conf spark.kubernetes.kerberos.keytab=/var/keytabs/hdfs.keytab \
      --conf spark.kubernetes.kerberos.principal=hdfs/nn.${NAMESPACE}.svc.cluster.local@CLUSTER.LOCAL \
      --conf spark.kubernetes.driver.labels=spark-app-locator=${APP_LOCATOR_LABEL} \
      --files local:///etc/hadoop/core-site.xml,local:///etc/hadoop/hdfs-site.xml,local:///etc/hadoop/yarn-site.xml \
      ${SUBMIT_RESOURCE} \
      hdfs://nn.${NAMESPACE}.svc.cluster.local:9000/user/ifilonenko/wordcount.txt