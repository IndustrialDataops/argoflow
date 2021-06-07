"""
version : 0.1
Generates spark operator configuration file from python code
Usage : 
template = sparkTemplate(
    appName="Testing",
    spec=sparkSpec(
        sparkType="Scala",
        sparkVersion="3.0.0",
        DeployMode="cluster",
        image="gcr.io/spark-operator/spark:v3.0.0-gcs-prometheus",
        imagePullPolicy="Always",
        mainClass="org.apache.spark.examples.SparkPi",
        mainApplicationFile="local:///opt/spark/examples/jars/spark-examples_2.12-3.0.0.jar",
        arguments=["100"],
        sparkConf={"spark.some.property": "true", "spark.some.another.property": "false"},
        driver=driverSpec(labels={"version": "3.0.0"}),
        executor=executorSpec(labels={"version": "3.0.0"}),
        restartPolicy=restartSpec(),
        monitoring=monitoringSpec(
            prometheus=prometheusSpec(
                jmxExporterJar="/prometheus/jmx_prometheus_javaagent-0.11.0.jar",
                port=8090,
            )
        ),
    ),
)
data = template.generateTemplate()
After dumping above dict into yaml , below format is generated
  apiversion: sparkoperator.k8s.io/v1beta2
  kind: SparkApplication
  metadata:
    generateName: Testing-
    namespace: default
  spec:
    SparkApplicationType: Scala
    sparkVersion: 3.0.0
    DeployMode: cluster
    image: gcr.io/spark-operator/spark:v3.0.0-gcs-prometheus
    imagePullPolicy: Always
    mainClass: org.apache.spark.examples.SparkPi
    mainApplicationFile: local:///opt/spark/examples/jars/spark-examples_2.12-3.0.0.jar
    arguments:
    - '100'
    sparkConf:
      spark.some.property: true
      spark.some.another.property: false
    driver:
      cores: 1
      coreLimit: coreLimit
      memory: 512m
      labels:
        version: 3.0.0
      serviceAccount: default
    executor:
      cores: 1
      instances: 1
      memory: 512m
      labels:
        version: 3.0.0
    restartPolicy:
      type: Never
    failureRetries: 0
    monitoring:
      exposeDriverMetrics: true
      exposeExecutorMetrics: true
      prometheus:
        jmxExporterJar: /prometheus/jmx_prometheus_javaagent-0.11.0.jar
        port: 8090
"""


from typing import Dict, List


class sparkTemplatingException(Exception):
    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        if self.message:
            return "Spark Templating Exception, {0} ".format(self.message)
        else:
            return "Spark Templating Exception has been raised"


class volumeSpec(object):
    pass  # TODO


class driverSpec(object):
    def __init__(
        self,
        cores: int = 1,
        coreLimit: str = "1200m",
        memory: str = "512m",
        labels: Dict = None,
        serviceAccount: str = "default",
    ):
        self.cores = cores
        self.coreLimit = coreLimit
        self.memory = memory
        self.labels = labels
        self.serviceAccount = serviceAccount

    def __repr__(self):
        return repr(
            {
                "cores": self.cores,
                "coreLimit": self.coreLimit,
                "memory": self.memory,
                "labels": self.labels,
                "serviceAccount": self.serviceAccount,
            }
        )


class executorSpec(object):
    def __init__(
        self,
        cores: int = 1,
        instances: int = 1,
        memory: str = "512m",
        labels: Dict = None,
    ):
        self.cores = cores
        self.instances = instances
        self.memory = memory
        self.labels = labels

    def __repr__(self):
        return repr(
            {
                "cores": self.cores,
                "instances": self.instances,
                "memory": self.memory,
                "labels": self.labels,
            }
        )


class restartSpec(object):
    def __init__(self, RestartPolicyType: Dict = {"type": "Never"}):
        self.RestartPolicyType = RestartPolicyType

    def __repr__(self):
        return repr(self.RestartPolicyType)


class prometheusSpec(object):
    def __init__(self, jmxExporterJar: str, port: int):
        self.jmxExporterJar = jmxExporterJar
        self.port = port

    def __repr__(self):
        return repr({"jmxExporterJar": self.jmxExporterJar, "port": self.port})


class monitoringSpec(object):
    def __init__(
        self,
        exposeDriverMetrics: bool = True,
        exposeExecutorMetrics: bool = True,
        prometheus: prometheusSpec = None,
    ):
        self.exposeDriverMetrics = exposeDriverMetrics
        self.exposeExecutorMetrics = exposeExecutorMetrics
        self.prometheus = prometheus

    def __repr__(self):
        return repr(
            {
                "exposeDriverMetrics": self.exposeDriverMetrics,
                "exposeExecutorMetrics": self.exposeExecutorMetrics,
                "prometheus": self.prometheus,
            }
        )


class dynamicSpec(object):
    def __init__(self, enabled: bool = False):
        self.enabled = enabled

    def __repr__(self):
        return repr({"enabled": self.enabled})


class sparkSpec(object):
    def __init__(
        self,
        sparkType: str,
        sparkVersion: str,
        DeployMode: str = "cluster",
        image: str = None,
        imagePullPolicy: str = None,
        mainClass: str = None,
        mainApplicationFile: str = None,
        arguments: List[str] = None,
        sparkConf: Dict = None,
        volumes: volumeSpec = None,
        driver: driverSpec = None,
        executor: executorSpec = None,
        restartPolicy: restartSpec = None,
        failureRetries: int = 0,
        pythonVersion: str = None,
        monitoring: monitoringSpec = None,
        dynamicAllocation: dynamicSpec = None,
    ):
        self.SparkApplicationType = sparkType
        self.sparkVersion = sparkVersion
        self.DeployMode = DeployMode
        self.image = image
        self.imagePullPolicy = imagePullPolicy
        self.mainClass = mainClass
        if mainApplicationFile is not None:
            self.mainApplicationFile = mainApplicationFile
        else:
            raise sparkTemplatingException("File location must be specified")
        self.arguments = arguments
        self.sparkConf = sparkConf
        self.volumes = volumes
        self.driver = driver
        self.executor = executor
        self.restartPolicy = restartPolicy
        self.failureRetries = failureRetries
        self.pythonVersion = pythonVersion
        self.monitoring = monitoring
        self.dynamicAllocation = dynamicAllocation

    def __repr__(self):
        data = {
            "type": self.SparkApplicationType,
            "sparkVersion": self.sparkVersion,
            "mode": self.DeployMode,
            "image": self.image,
            "imagePullPolicy": self.imagePullPolicy,
            "mainClass": self.mainClass,
            "mainApplicationFile": self.mainApplicationFile,
            "arguments": self.arguments,
            "sparkConf": self.sparkConf,
            "volumes": self.volumes,
            "driver": self.driver,
            "executor": self.executor,
            "restartPolicy": self.restartPolicy,
            "failureRetries": self.failureRetries,
            "pythonVersion": self.pythonVersion,
            "monitoring": self.monitoring,
            "dynamicAllocation": self.dynamicAllocation,
        }
        return repr({k: v for k, v in data.items() if v is not None})


class sparkTemplate(object):
    def __init__(
        self, appName: str, nameSpace: str = "default", spec: sparkSpec = None
    ):
        self.apiVersion = "sparkoperator.k8s.io/v1beta2"
        self.kind = "SparkApplication"
        self.metadata = dict({"generateName": f"{appName}-", "namespace": nameSpace})
        self.spec = spec

    def generateTemplate(self) -> Dict:
        return {
            "apiVersion": self.apiVersion,
            "kind": self.kind,
            "metadata": self.metadata,
            "spec": self.spec,
        }
