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

import java.io._
import java.security.PrivilegedExceptionAction

import scala.collection.JavaConverters._
import scala.util.Try

import io.fabric8.kubernetes.api.model.SecretBuilder
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.kubernetes.{HadoopUGIUtil, KerberosTokenConfBootstrapImpl, PodWithMainContainer}
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.internal.Logging

 /**
  * This step does all the heavy lifting for Delegation Token logic. This step
  * assumes that the job user has either specified a principal and keytab or ran
  * $kinit before running spark-submit. With a TGT stored locally, by running
  * UGI.getCurrentUser you are able to obtain the current user, alternatively
  * you can run UGI.loginUserFromKeytabAndReturnUGI and by running .doAs run
  * as the logged into user instead of the current user. With the Job User principal
  * you then retrieve the delegation token from the NameNode and store values in
  * DelegationToken. Lastly, the class puts the data into a secret. All this is
  * appended to the current HadoopSpec which in turn will append to the current
  * DriverSpec.
  */
private[spark] class HadoopKerberosKeytabResolverStep(
  submissionSparkConf: SparkConf,
  maybePrincipal: Option[String],
  maybeKeytab: Option[File],
  hadoopUGI: HadoopUGIUtil) extends HadoopConfigurationStep with Logging{
    private var originalCredentials: Credentials = _
    private var renewer: String = _
    private var credentials: Credentials = _
    private var tokens: Iterable[Token[_ <: TokenIdentifier]] = _

    override def configureContainers(hadoopConfigSpec: HadoopConfigSpec): HadoopConfigSpec = {
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(submissionSparkConf)
    logDebug(s"Hadoop Configuration: ${hadoopConf.toString}")
    if (hadoopUGI.isSecurityEnabled) logError("Hadoop not configuration with Kerberos")
    val maybeJobUserUGI =
      for {
        principal <- maybePrincipal
        keytab <- maybeKeytab
      } yield {
        // Not necessary with [Spark-16742]
        // Reliant on [Spark-20328] for changing to YARN principal
        submissionSparkConf.set("spark.yarn.principal", principal)
        submissionSparkConf.set("spark.yarn.keytab", keytab.toURI.toString)
        logDebug("Logged into KDC with keytab using Job User UGI")
        hadoopUGI.loginUserFromKeytabAndReturnUGI(
          principal,
          keytab.toURI.toString)
      }
    // In the case that keytab is not specified we will read from Local Ticket Cache
    val jobUserUGI = maybeJobUserUGI.getOrElse(hadoopUGI.getCurrentUser)
    // It is necessary to run as jobUserUGI because logged in user != Current User
    jobUserUGI.doAs(new PrivilegedExceptionAction[Void] {
      override def run(): Void = {
        logDebug(s"Retrieved Job User UGI: $jobUserUGI")
        originalCredentials = jobUserUGI.getCredentials
        logDebug(s"Original tokens: ${originalCredentials.toString}")
        logDebug(s"All tokens: ${originalCredentials.getAllTokens}")
        logDebug(s"All secret keys: ${originalCredentials.getAllSecretKeys}")
        // This is not necessary with [Spark-20328] since we would be using
        // Spark core providers to handle delegation token renewal
        renewer = jobUserUGI.getShortUserName
        logDebug(s"Renewer is: $renewer")
        credentials = new Credentials(originalCredentials)
        hadoopUGI.dfsAddDelegationToken(hadoopConf, renewer, credentials)
        // This is difficult to Mock and will require refactoring
        tokens = credentials.getAllTokens.asScala
        logDebug(s"Tokens: ${credentials.toString}")
        logDebug(s"All tokens: ${tokens.mkString(",")}")
        logDebug(s"All secret keys: ${credentials.getAllSecretKeys}")
        null
      }})
    credentials.getAllTokens.asScala.isEmpty
    tokens.isEmpty
    if (tokens.isEmpty) logError("Did not obtain any Delegation Tokens")
    val data = serialize(credentials)
    val renewalTime = getTokenRenewalInterval(tokens, hadoopConf).getOrElse(Long.MaxValue)
    val currentTime: Long = System.currentTimeMillis()
    val initialTokenDataKeyName = s"$KERBEROS_SECRET_LABEL_PREFIX-$currentTime-$renewalTime"
    val secretDT =
      new SecretBuilder()
        .withNewMetadata()
          .withName(HADOOP_KERBEROS_SECRET_NAME)
          .withLabels(Map("refresh-hadoop-tokens" -> "yes").asJava)
          .endMetadata()
          .addToData(initialTokenDataKeyName, Base64.encodeBase64String(data))
      .build()
    val bootstrapKerberos = new KerberosTokenConfBootstrapImpl(
      HADOOP_KERBEROS_SECRET_NAME,
      initialTokenDataKeyName,
      jobUserUGI.getShortUserName)
    val withKerberosEnvPod = bootstrapKerberos.bootstrapMainContainerAndVolumes(
      PodWithMainContainer(
        hadoopConfigSpec.driverPod,
        hadoopConfigSpec.driverContainer))
    hadoopConfigSpec.copy(
      additionalDriverSparkConf =
        hadoopConfigSpec.additionalDriverSparkConf ++ Map(
          HADOOP_KERBEROS_CONF_ITEM_KEY -> initialTokenDataKeyName,
          HADOOP_KERBEROS_CONF_SECRET -> HADOOP_KERBEROS_SECRET_NAME),
      driverPod = withKerberosEnvPod.pod,
      driverContainer = withKerberosEnvPod.mainContainer,
      dtSecret = Some(secretDT),
      dtSecretName = HADOOP_KERBEROS_SECRET_NAME,
      dtSecretItemKey = initialTokenDataKeyName)
  }

  // Functions that should be in Core with Rebase to 2.3
  @deprecated("Moved to core in 2.2", "2.2")
  private def getTokenRenewalInterval(
    renewedTokens: Iterable[Token[_ <: TokenIdentifier]],
    hadoopConf: Configuration): Option[Long] = {
      val renewIntervals = renewedTokens.filter {
        _.decodeIdentifier().isInstanceOf[AbstractDelegationTokenIdentifier]}
        .flatMap { token =>
        Try {
          val newExpiration = token.renew(hadoopConf)
          val identifier = token.decodeIdentifier().asInstanceOf[AbstractDelegationTokenIdentifier]
          val interval = newExpiration - identifier.getIssueDate
          logInfo(s"Renewal interval is $interval for token ${token.getKind.toString}")
          interval
        }.toOption}
      if (renewIntervals.isEmpty) None else Some(renewIntervals.min)
  }

  @deprecated("Moved to core in 2.2", "2.2")
  private def serialize(creds: Credentials): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(byteStream)
    creds.writeTokenStorageToStream(dataStream)
    byteStream.toByteArray
  }

  @deprecated("Moved to core in 2.2", "2.2")
  private def deserialize(tokenBytes: Array[Byte]): Credentials = {
    val creds = new Credentials()
    creds.readTokenStorageStream(new DataInputStream(new ByteArrayInputStream(tokenBytes)))
    creds
  }
}
