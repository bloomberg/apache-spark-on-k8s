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
package org.apache.spark.deploy.k8s

import io.fabric8.kubernetes.api.model.ContainerBuilder

import org.apache.spark.deploy.k8s.constants._

// This trait is responsible for setting ENV_SPARK_USER when HADOOP_FILES are detected
// however, this step would not be run if Kerberos is enabled, as Kerberos sets SPARK_USER
private[spark] trait HadoopConfSparkUserBootstrap {
  def bootstrapMainContainerAndVolumes(originalPodWithMainContainer: PodWithMainContainer)
  : PodWithMainContainer
}

private[spark] class HadoopConfSparkUserBootstrapImpl(hadoopUGIUtil: HadoopUGIUtil)
  extends HadoopConfSparkUserBootstrap {

  override def bootstrapMainContainerAndVolumes(originalPodWithMainContainer: PodWithMainContainer)
    : PodWithMainContainer = {
    val envModifiedContainer = new ContainerBuilder(
      originalPodWithMainContainer.mainContainer)
      .addNewEnv()
        .withName(ENV_SPARK_USER)
        .withValue(hadoopUGIUtil.getShortUserName)
        .endEnv()
      .build()
    originalPodWithMainContainer.copy(
      pod = originalPodWithMainContainer.pod,
      mainContainer = envModifiedContainer)
  }
}
