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
package org.apache.spark.deploy.k8s.submit

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model._
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.{HadoopUGIUtilImpl, PodWithMainContainer}
import org.apache.spark.deploy.k8s.HadoopConfSparkUserBootstrapImpl
import org.apache.spark.deploy.k8s.constants._

private[spark] class HadoopConfSparkUserBootstrapSuite extends SparkFunSuite with BeforeAndAfter{
  private val SPARK_USER_VALUE = "sparkUser"

  @Mock
  private var hadoopUtil: HadoopUGIUtilImpl = _

  before {
    MockitoAnnotations.initMocks(this)
    when(hadoopUtil.getShortUserName).thenReturn(SPARK_USER_VALUE)
  }

  test("Test of bootstrapping hadoop_conf_dir files") {
    val hadoopConfStep = new HadoopConfSparkUserBootstrapImpl(hadoopUtil)
    val emptyPod = new PodBuilder().withNewSpec().endSpec().build()
    val podWithMain = PodWithMainContainer(
      emptyPod,
      new Container())
    val returnedPodContainer = hadoopConfStep.bootstrapMainContainerAndVolumes(podWithMain)
    assert(emptyPod === returnedPodContainer.pod)
    assert(returnedPodContainer.mainContainer.getEnv.asScala.head ===
      new EnvVarBuilder().withName(ENV_SPARK_USER).withValue(SPARK_USER_VALUE).build())
  }
}
