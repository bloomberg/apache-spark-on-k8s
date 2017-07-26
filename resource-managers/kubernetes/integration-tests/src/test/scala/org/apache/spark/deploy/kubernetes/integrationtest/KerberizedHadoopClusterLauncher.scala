/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.kubernetes.integrationtest

import java.io.{File, FileInputStream}

import io.fabric8.kubernetes.api.model.extensions.{Deployment, DeploymentBuilder}
import io.fabric8.kubernetes.api.model.{ConfigMapBuilder, KeyToPathBuilder, Service}
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.commons.io.FileUtils.readFileToString
import org.apache.spark.deploy.kubernetes.submit.ContainerNameEqualityPredicate

 /**
  * Stuff
  */
private[spark] class KerberizedHadoopClusterLauncher(
  kubernetesClient: KubernetesClient,
  namespace: String) {
  private def yamlLocation(loc: String) = s"kerberos-yml/$loc.yml"
  private def loadFromYaml(resource: String) =
    kubernetesClient.load(new FileInputStream(new File(yamlLocation(resource))))
//  private val regex = "REPLACE_ME".r
//  private val krb5ConfFile =
//    regex.replaceAllIn(
//      readFileToString(new File("src/test/resources/krb5.conf")),
//      namespace)
  private val KRB_VOLUME = "krb5-conf"
  private val KRB_FILE_DIR = "/etc"
  private val KRB_CONFIG_MAP_NAME = "krb-config-map"
  private val KRB_CONF_FILE = "krb5.conf"
  private val KRB_KEY_PATH =
    new KeyToPathBuilder()
      .withKey(KRB_CONF_FILE)
      .withPath(KRB_CONF_FILE)
      .build()

  def launchKerberizedCluster(): Unit = {
    val persistantVolumeList = Seq(
      "namenode-hadoop",
      "namenode-hadoop-pv",
      "server-keytab",
      "server-keytab-pv")
    val deploymentServiceList = Seq(
      "kerberos-deployment",
      "kerberos-service",
      "nn-deployment",
      "nn-service",
      "dn1-deployment",
      "dn1-service",
      "data-populator-deployment",
      "data-populator-service")
    persistantVolumeList.foreach{resource =>
      loadFromYaml(resource).createOrReplace()
      Thread.sleep(10000)}
//    val configMap = new ConfigMapBuilder()
//      .withNewMetadata()
//      .withName(KRB_CONFIG_MAP_NAME)
//      .endMetadata()
//      .addToData(KRB_CONF_FILE, krb5ConfFile)
//      .build()
//    kubernetesClient.configMaps().inNamespace(namespace).createOrReplace(configMap)
//    Thread.sleep(2000)
    deploymentServiceList.foreach{ resource => loadFromYaml(resource).get().get(0) match {
      case deployment: Deployment =>
        val deploymentWithEnv = new DeploymentBuilder(deployment)
          .editSpec()
            .editTemplate()
                .editSpec()
                  .addNewVolume()
                      .withName(KRB_VOLUME)
                      .withNewConfigMap()
                        .withName(KRB_CONFIG_MAP_NAME)
                        .withItems(KRB_KEY_PATH)
                        .endConfigMap()
                    .endVolume()
                  .editMatchingContainer(new ContainerNameEqualityPredicate(
                    deployment.getMetadata.getName))
                    .addNewEnv()
                      .withName("NAMESPACE")
                      .withValue(namespace)
                      .endEnv()
                    .addNewVolumeMount()
                      .withName(KRB_VOLUME)
                      .withMountPath(KRB_FILE_DIR)
                      .endVolumeMount()
                    .endContainer()
                    .endSpec()
                  .endTemplate()
                .endSpec()
            .build()
        kubernetesClient.extensions().deployments().inNamespace(namespace).create(deploymentWithEnv)
        Thread.sleep(10000)
      case service: Service =>
        loadFromYaml(resource).createOrReplace()
        Thread.sleep(10000)}
    }
  }
}