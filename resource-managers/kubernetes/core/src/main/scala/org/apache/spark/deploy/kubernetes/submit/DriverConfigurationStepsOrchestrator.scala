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
package org.apache.spark.deploy.kubernetes.submit

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.ConfigurationUtils
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.kubernetes.submit.submitsteps.hadoopsteps.HadoopStepsOrchestrator
import org.apache.spark.deploy.kubernetes.submit.submitsteps._
import org.apache.spark.deploy.kubernetes.submit.submitsteps.initcontainer.InitContainerConfigurationStepsOrchestrator
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.util.Utils

/**
 * Constructs the complete list of driver configuration steps to run to deploy the Spark driver.
 */
private[spark] class DriverConfigurationStepsOrchestrator(
    namespace: String,
    kubernetesAppId: String,
    launchTime: Long,
    mainAppResource: MainAppResource,
    appName: String,
    mainClass: String,
    appArgs: Array[String],
    additionalPythonFiles: Seq[String],
    submissionSparkConf: SparkConf) {

  // The resource name prefix is derived from the application name, making it easy to connect the
  // names of the Kubernetes resources from e.g. kubectl or the Kubernetes dashboard to the
  // application the user submitted. However, we can't use the application name in the label, as
  // label values are considerably restrictive, e.g. must be no longer than 63 characters in
  // length. So we generate a separate identifier for the app ID itself, and bookkeeping that
  // requires finding "all pods for this application" should use the kubernetesAppId.
  private val kubernetesResourceNamePrefix =
      s"$appName-$launchTime".toLowerCase.replaceAll("\\.", "-")
  private val jarsDownloadPath = submissionSparkConf.get(INIT_CONTAINER_JARS_DOWNLOAD_LOCATION)
  private val filesDownloadPath = submissionSparkConf.get(INIT_CONTAINER_FILES_DOWNLOAD_LOCATION)
  private val dockerImagePullPolicy = submissionSparkConf.get(DOCKER_IMAGE_PULL_POLICY)
  private val initContainerConfigMapName = s"$kubernetesResourceNamePrefix-init-config"
  private val hadoopConfigMapName = s"$kubernetesResourceNamePrefix-hadoop-config"

  def getAllConfigurationSteps(): Seq[DriverConfigurationStep] = {
    val additionalMainAppJar = mainAppResource match {
      case JavaMainAppResource(resource) if resource != SparkLauncher.NO_RESOURCE =>
        Option(resource)
      case _ => Option.empty
    }
    val additionalMainAppPythonFile = mainAppResource match {
      case PythonMainAppResource(resource) if resource != SparkLauncher.NO_RESOURCE =>
        Option(resource)
      case _ => Option.empty
    }
    val sparkJars = submissionSparkConf.getOption("spark.jars")
        .map(_.split(","))
        .getOrElse(Array.empty[String]) ++
        additionalMainAppJar.toSeq
    val sparkFiles = submissionSparkConf.getOption("spark.files")
        .map(_.split(","))
        .getOrElse(Array.empty[String]) ++
        additionalMainAppPythonFile.toSeq ++
        additionalPythonFiles
    val driverCustomLabels = ConfigurationUtils.combinePrefixedKeyValuePairsWithDeprecatedConf(
        submissionSparkConf,
        KUBERNETES_DRIVER_LABEL_PREFIX,
        KUBERNETES_DRIVER_LABELS,
        "label")
    require(!driverCustomLabels.contains(SPARK_APP_ID_LABEL), s"Label with key " +
        s" $SPARK_APP_ID_LABEL is not allowed as it is reserved for Spark bookkeeping" +
        s" operations.")
    val allDriverLabels = driverCustomLabels ++ Map(
        SPARK_APP_ID_LABEL -> kubernetesAppId,
        SPARK_ROLE_LABEL -> SPARK_POD_DRIVER_ROLE)
    val initialSubmissionStep = new BaseDriverConfigurationStep(
        kubernetesAppId,
        kubernetesResourceNamePrefix,
        allDriverLabels,
        dockerImagePullPolicy,
        appName,
        mainClass,
        appArgs,
        submissionSparkConf)
    val kubernetesCredentialsStep = new DriverKubernetesCredentialsStep(
        submissionSparkConf, kubernetesResourceNamePrefix)
    val hadoopConfigurations =
      sys.env.get("HADOOP_CONF_DIR").map{ conf => getHadoopConfFiles(conf)}
          .getOrElse(Array.empty[File])
    val hadoopConfigSteps =
      if (hadoopConfigurations.isEmpty) {
        Option.empty[DriverConfigurationStep]
      } else {
        val hadoopStepsOrchestrator = new HadoopStepsOrchestrator(
          namespace,
          kubernetesResourceNamePrefix,
          submissionSparkConf,
          hadoopConfigurations)
        val hadoopConfSteps =
          hadoopStepsOrchestrator.getHadoopSteps()
        Some(new HadoopConfigBootstrapStep(hadoopConfSteps))
      }
    val pythonStep = mainAppResource match {
      case PythonMainAppResource(mainPyResource) =>
        Option(new PythonStep(mainPyResource, additionalPythonFiles, filesDownloadPath))
      case _ => Option.empty[DriverConfigurationStep]
    }
    val initContainerBootstrapStep = if ((sparkJars ++ sparkFiles).exists { uri =>
      Option(Utils.resolveURI(uri).getScheme).getOrElse("file") != "local"
    }) {
      val initContainerConfigurationStepsOrchestrator =
          new InitContainerConfigurationStepsOrchestrator(
              namespace,
              kubernetesResourceNamePrefix,
              sparkJars,
              sparkFiles,
              jarsDownloadPath,
              filesDownloadPath,
              dockerImagePullPolicy,
              allDriverLabels,
              initContainerConfigMapName,
              INIT_CONTAINER_CONFIG_MAP_KEY,
              submissionSparkConf)
      val initContainerConfigurationSteps =
          initContainerConfigurationStepsOrchestrator.getAllConfigurationSteps()
      Some(new InitContainerBootstrapStep(initContainerConfigurationSteps,
        initContainerConfigMapName,
        INIT_CONTAINER_CONFIG_MAP_KEY))
    } else {
      Option.empty[DriverConfigurationStep]
    }
    val dependencyResolutionStep = new DependencyResolutionStep(
      sparkJars,
      sparkFiles,
      jarsDownloadPath,
      filesDownloadPath)
    Seq(
      initialSubmissionStep,
      kubernetesCredentialsStep,
      dependencyResolutionStep) ++
      initContainerBootstrapStep.toSeq ++
      hadoopConfigSteps.toSeq ++
      pythonStep.toSeq
  }
  private def getHadoopConfFiles(path: String) : Array[File] = {
    def isFile(file: File) = if (file.isFile) Some(file) else None
    val dir = new File(path)
    if (dir.isDirectory) {
      dir.listFiles.flatMap { file => isFile(file) }
    } else {
      Array.empty[File]
    }
  }
}
