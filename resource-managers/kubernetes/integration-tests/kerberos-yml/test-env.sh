#!/usr/bin/env bash
sed -i -e 's/#//' -e 's/default_ccache_name/# default_ccache_name/' /etc/krb5.conf
cp ${TMP_KRB_LOC} /etc/krb5.conf
cp ${TMP_CORE_LOC} /opt/spark/hconf/core-site.xml
cp ${TMP_HDFS_LOC} /opt/spark/hconf/hdfs-site.xml
mkdir -p /etc/krb5.conf.d
/usr/bin/kinit -kt /var/keytabs/hdfs.keytab hdfs/nn.${NAMESPACE}.svc.cluster.local

/opt/spark/bin/spark-submit \
      --deploy-mode cluster \
      --class ${CLASS_NAME} \
      --master k8s://${MASTER_URL} \
      --kubernetes-namespace ${NAMESPACE} \
      --conf spark.executor.instances=1 \
      --conf spark.app.name=spark-hdfs \
      --conf spark.kubernetes.driver.docker.image=spark-driver:latest \
      --conf spark.kubernetes.executor.docker.image=spark-executor:latest \
      --conf spark.kubernetes.initcontainer.docker.image=spark-init:latest \
      --conf spark.hadoop.fs.defaultFS=hdfs://nn.${NAMESPACE}.svc.cluster.local:9000 \
      --conf spark.kubernetes.kerberos=true \
      --conf spark.kubernetes.kerberos.keytab=/var/keytabs/hdfs.keytab \
      --conf spark.kubernetes.kerberos.principal=hdfs/nn.${NAMESPACE}.svc.cluster.local@CLUSTER.LOCAL \
      --conf spark.kubernetes.driver.labels=spark-app-locator=${APP_LOCATOR_LABEL} \
      ${SUBMIT_RESOURCE} \
      hdfs://nn.${NAMESPACE}.svc.cluster.local:9000/user/ifilonenko/wordcount.txt