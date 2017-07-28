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

private[spark] class KerberosPVWatcherCache() {
//   client: KubernetesClient,
//   dsNamespace: String,
//   dsLabels: Map[String, String]) extends Logging {
//
//  private var shufflePodCache = 2
//  private var watcher: Watch = _
//
//  def start(): Unit = {
//    // seed the initial cache.
//    val pvs = client.persistentVolumes().withLabels(dsLabels.asJava).list()
//    pvs.getItems.asScala.foreach {
//      pv =>
//        if (Readiness.isReady(pv)) {
//          pvs.len
//        } else {
//          logWarning(s"Found unready shuffle pod ${pod.getMetadata.getName} " +
//            s"on node ${pod.getSpec.getNodeName}")
//        }
//    }
//
//    watcher = client
//      .pods()
//      .inNamespace(dsNamespace)
//      .withLabels(dsLabels.asJava)
//      .watch(new Watcher[Pod] {
//        override def eventReceived(action: Watcher.Action, p: Pod): Unit = {
//          action match {
//            case Action.DELETED | Action.ERROR =>
//              shufflePodCache.remove(p.getSpec.getNodeName)
//            case Action.ADDED | Action.MODIFIED if Readiness.isReady(p) =>
//              addShufflePodToCache(p)
//          }
//        }
//        override def onClose(e: KubernetesClientException): Unit = {}
//      })
//  }
//
//  private def addShufflePodToCache(pod: Pod): Unit = {
//    if (shufflePodCache.contains(pod.getSpec.getNodeName)) {
//      val registeredPodName = shufflePodCache.get(pod.getSpec.getNodeName).get
//      logError(s"Ambiguous specification of shuffle service pod. " +
//        s"Found multiple matching pods: ${pod.getMetadata.getName}, " +
//        s"${registeredPodName} on ${pod.getSpec.getNodeName}")
//
//      throw new SparkException(s"Ambiguous specification of shuffle service pod. " +
//        s"Found multiple matching pods: ${pod.getMetadata.getName}, " +
//        s"${registeredPodName} on ${pod.getSpec.getNodeName}")
//    } else {
//      shufflePodCache(pod.getSpec.getNodeName) = pod.getStatus.getPodIP
//    }
//  }
//
//  def stop(): Unit = {
//    watcher.close()
//  }
//
//  def getShufflePodForExecutor(executorNode: String): String = {
//    shufflePodCache.get(executorNode)
//      .getOrElse(throw new SparkException(s"Unable to find shuffle pod on node $executorNode"))
//  }

}

