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
package org.apache.spark.deploy.k8s.submit.submitsteps.hadoopsteps

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model._
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.{HadoopConfSparkUserBootstrap, PodWithMainContainer}


private[spark] class HadoopConfSparkUserStepSuite extends SparkFunSuite with BeforeAndAfter{
  private val POD_LABEL = Map("bootstrap" -> "true")
  private val DRIVER_CONTAINER_NAME = "driver-container"

  @Mock
  private var hadoopConfSparkUserBootstrap : HadoopConfSparkUserBootstrap = _

  before {
    MockitoAnnotations.initMocks(this)
    when(hadoopConfSparkUserBootstrap.bootstrapMainContainerAndVolumes(
      any[PodWithMainContainer])).thenAnswer(new Answer[PodWithMainContainer] {
      override def answer(invocation: InvocationOnMock) : PodWithMainContainer = {
        val pod = invocation.getArgumentAt(0, classOf[PodWithMainContainer])
        pod.copy(
          pod =
            new PodBuilder(pod.pod)
              .withNewMetadata()
              .addToLabels("bootstrap", "true")
              .endMetadata()
              .withNewSpec().endSpec()
              .build(),
          mainContainer =
            new ContainerBuilder()
              .withName(DRIVER_CONTAINER_NAME).build()
        )}})
  }

  test("Test of calling the SPARK_USER bootstrap to modify the HadoopConfSpec") {
    val hadoopSparkUserStep = new HadoopConfSparkUserStep(hadoopConfSparkUserBootstrap)
    val hadoopConfSpec = HadoopConfigSpec(
      Map.empty[String, String],
      new Pod(),
      new Container(),
      Map.empty[String, String],
      None,
      "",
      "")
    val returnContainerSpec = hadoopSparkUserStep.configureContainers(hadoopConfSpec)
    assert(returnContainerSpec.driverContainer.getName == DRIVER_CONTAINER_NAME)
    assert(returnContainerSpec.driverPod.getMetadata.getLabels.asScala === POD_LABEL)
  }
}
