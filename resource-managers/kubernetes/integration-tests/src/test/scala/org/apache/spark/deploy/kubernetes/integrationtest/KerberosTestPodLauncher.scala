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
import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.deploy.kubernetes.submit.ContainerNameEqualityPredicate

 /**
  * Stuff
  */
private[spark] class KerberosTestPodLauncher(
  kubernetesClient: KubernetesClient,
  namespace: String) {

  private val yamlLocation = "kerberos-yml/kerberos-test.yml"
  def startKerberosTest(resource: String, className: String, appLabel: String): Unit = {
    kubernetesClient.load(new FileInputStream(new File(yamlLocation)))
      .get().get(0) match {
      case deployment: Deployment =>
        val deploymentWithEnv: Deployment = new DeploymentBuilder(deployment)
        .editSpec()
          .editTemplate()
            .editSpec()
              .editMatchingContainer(new ContainerNameEqualityPredicate(
                deployment.getMetadata.getName))
                .addNewEnv()
                  .withName("NAMESPACE")
                  .withValue(namespace)
                  .endEnv()
                .addNewEnv()
                  .withName("MASTER_URL")
                  .withValue(kubernetesClient.getMasterUrl.toString)
                .endEnv()
                .addNewEnv()
                  .withName("SUBMIT_RESOURCE")
                  .withValue(resource)
                 .endEnv()
                .addNewEnv()
                  .withName("CLASS_NAME")
                  .withValue(className)
                  .endEnv()
                .addNewEnv()
                  .withName("HADOOP_CONF_DIR")
                  .withValue("hconf")
                  .endEnv()
                .addNewEnv()
                  .withName("APP_LOCATOR_LABEL")
                  .withValue(appLabel)
                  .endEnv()
                .addNewEnv()
                  .withName("SPARK_PRINT_LAUNCH_COMMAND")
                  .withValue("true")
                  .endEnv()
                .endContainer()
              .endSpec()
            .endTemplate()
          .endSpec()
        .build()
        kubernetesClient.extensions().deployments()
          .inNamespace(namespace).create(deploymentWithEnv)}
  }
}