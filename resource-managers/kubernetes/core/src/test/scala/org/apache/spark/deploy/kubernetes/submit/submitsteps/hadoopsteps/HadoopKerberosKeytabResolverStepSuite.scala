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
package org.apache.spark.deploy.kubernetes.submit.submitsteps.hadoopsteps

import java.io.File
import java.util.UUID

import scala.collection.JavaConverters._
import com.google.common.io.Files
import io.fabric8.kubernetes.api.model._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Matchers.{any, anyString}
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.kubernetes.HadoopUGIUtil
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.util.Utils

private[spark] class HadoopKerberosKeytabResolverStepSuite
  extends SparkFunSuite with BeforeAndAfter{
  private val POD_LABEL = Map("bootstrap" -> "true")
  private val DRIVER_CONTAINER_NAME = "driver-container"
  private val TEMP_KEYTAB_FILE = createTempFile("keytab")
  private val KERB_PRINCIPAL = "user@k8s.com"
  private val SPARK_USER_VALUE = "sparkUser"
  private var oldCredentials = new Credentials()
  private val TEST_IDENTIFIER = "identifier"
  private val TEST_PASSWORD = "password"
  private val TEST_TOKEN_VALUE = "data"
  private def getByteArray(input: String) = input.toCharArray.map(_.toByte)
  private def getStringFromArray(input: Array[Byte]) = new String(input)
  private val TEST_TOKEN = new Token[AbstractDelegationTokenIdentifier](
    getByteArray(TEST_IDENTIFIER),
    getByteArray(TEST_PASSWORD),
    new Text("kind"),
    new Text("service"))
  oldCredentials.addToken(new Text("testToken"), TEST_TOKEN)
  private val dfs = FileSystem.get(SparkHadoopUtil.get.newConfiguration(new SparkConf()))
  private val hadoopUGI = new HadoopUGIUtil()

  @Mock
  private var hadoopUtil: HadoopUGIUtil = _

  @Mock
  private var ugi: UserGroupInformation = _

  @Mock
  private var credentials: Credentials = _

  before {
    MockitoAnnotations.initMocks(this)
    when(hadoopUtil.loginUserFromKeytabAndReturnUGI(any[String], any[String]))
      .thenAnswer(new Answer[UserGroupInformation] {
      override def answer(invocation: InvocationOnMock): UserGroupInformation = {
        hadoopUGI.getCurrentUser
      }
    })
    when(hadoopUtil.getCurrentUser).thenReturn(ugi)
    when(hadoopUtil.getShortName).thenReturn(SPARK_USER_VALUE)
    when(hadoopUtil.dfsAddDelegationToken(any[Configuration](), anyString(), any[Credentials]()))
      .thenReturn(Array(TEST_TOKEN))
    when(ugi.getCredentials).thenReturn(oldCredentials)
    val tokens = List[Token[_ <: TokenIdentifier]](TEST_TOKEN).asJavaCollection
    when(credentials.getAllTokens).thenReturn(tokens)
  }

  test("Testing keytab login") {
    when(hadoopUtil.isSecurityEnabled).thenReturn(true)
    val keytabStep = new HadoopKerberosKeytabResolverStep(
      new SparkConf(),
      Some(KERB_PRINCIPAL),
      Some(TEMP_KEYTAB_FILE),
      hadoopUtil)
    val hadoopConfSpec = HadoopConfigSpec(
      Map.empty[String, String],
      new PodBuilder()
        .withNewMetadata()
        .addToLabels("bootstrap", "true")
        .endMetadata()
        .withNewSpec().endSpec()
        .build(),
      new ContainerBuilder().withName(DRIVER_CONTAINER_NAME).build(),
      Map.empty[String, String],
      None,
      "",
      "")
    val returnContainerSpec = keytabStep.configureContainers(hadoopConfSpec)
    assert(returnContainerSpec.additionalDriverSparkConf(HADOOP_KERBEROS_CONF_ITEM_KEY)
        .contains(KERBEROS_SECRET_LABEL_PREFIX))
    assert(returnContainerSpec.additionalDriverSparkConf(HADOOP_KERBEROS_CONF_SECRET) ===
      HADOOP_KERBEROS_SECRET_NAME)
    assert(returnContainerSpec.driverContainer.getName == DRIVER_CONTAINER_NAME)
    assert(returnContainerSpec.driverPod.getMetadata.getLabels.asScala === POD_LABEL)
    assert(returnContainerSpec.dtSecretItemKey.contains(KERBEROS_SECRET_LABEL_PREFIX))
    assert(returnContainerSpec.dtSecretName === HADOOP_KERBEROS_SECRET_NAME)
    assert(returnContainerSpec.dtSecret.get.getMetadata.getLabels.asScala ===
      Map("refresh-hadoop-tokens" -> "yes"))
    assert(returnContainerSpec.dtSecret.nonEmpty)
    assert(returnContainerSpec.dtSecret.get.getMetadata.getName === HADOOP_KERBEROS_SECRET_NAME)
  }

  private def createTempFile(contents: String): File = {
    val dir = Utils.createTempDir()
    val file = new File(dir, s"${UUID.randomUUID().toString}")
    Files.write(contents.getBytes, file)
    file
  }
}
