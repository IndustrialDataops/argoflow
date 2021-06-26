from abc import ABCMeta
from typing import List, Dict, Any
from argo.workflows.client import V1alpha1Arguments
from argoflow.sparktemplating import *
from argoflow.authorizedContainers import *

import networkx as nx
from pyvis.network import Network
from functools import reduce
import yaml
import ast


class TaskMeta(ABCMeta):
    def __new__(cls, name, bases, props: Dict[str, Any], **kwargs):
        props["tasks"] = {}
        klass = super().__new__(cls, name, bases, props)
        cls.__compile(klass, name, bases, props)
        return klass

    @classmethod
    def __compile(cls, klass, name, bases, props: Dict[str, Any]):
        tasks: List = []
        for key, prop in props.items():
            if isinstance(prop, dict) or isinstance(prop, list):
                tasks.append(prop)
        klass.task = tasks


def Pyspark(
    name: str, fileLocation: str, arguments: List = None, sparkConfig: Dict = None
) -> str:
    data = [x for x in authContainers().getData() if x["name"] == "sparkk8sScala"][0]
    template = sparkTemplate(
        appName=name,
        spec=sparkSpec(
            sparkType="Python",
            sparkVersion=data["sparkVersion"],
            DeployMode=data["mode"],
            image=data["image"],
            imagePullPolicy=data.get("imagepullpolicy", None),
            mainApplicationFile=fileLocation,
            arguments=arguments,
            sparkConf=sparkConfig,
            driver=driverSpec(labels={"version": data["sparkVersion"]}),
            executor=executorSpec(labels={"version": data["sparkVersion"]}),
            restartPolicy=restartSpec(),
            monitoring=monitoringSpec(
                prometheus=prometheusSpec(
                    jmxExporterJar=data.get("prometheusJar", None),
                    port=data.get("prometheusPort", None),
                )
            ),
        ),
    )
    sparkJob = template.generateTemplate()
    return yaml.dump(ast.literal_eval(str(sparkJob)), default_flow_style=False, sort_keys=False)


def sparkScala(
    name: str,
    className: str,
    fileLocation: str,
    arguments: List,
    sparkConfig: Dict = None,
) -> str:
    data = [x for x in authContainers().getData() if x["name"] == "sparkk8sScala"][0]
    template = sparkTemplate(
        appName=name,
        spec=sparkSpec(
            sparkType="Scala",
            sparkVersion=data["sparkVersion"],
            DeployMode=data["mode"],
            image=data["image"],
            imagePullPolicy=data.get("imagepullpolicy", None),
            mainClass=className,
            mainApplicationFile=fileLocation,
            arguments=arguments,
            sparkConf=sparkConfig,
            driver=driverSpec(labels={"version": data["sparkVersion"]}),
            executor=executorSpec(labels={"version": data["sparkVersion"]}),
            restartPolicy=restartSpec(),
            monitoring=monitoringSpec(
                prometheus=prometheusSpec(
                    jmxExporterJar=data.get("prometheusJar", None),
                    port=data.get("prometheusPort", None),
                )
            ),
        ),
    )
    sparkJob = template.generateTemplate()
    return yaml.dump(ast.literal_eval(str(sparkJob)), default_flow_style=False, sort_keys=False)


class taskFlow(metaclass=TaskMeta):
    def __init__(self, compile=True):
        self.dependencies = []
        self.graph = nx.DiGraph()
        if compile:
            self.compile()

    @property
    def model(self):
        return self.task

    def compile(self):
        return [tasks for tasks in self.task if tasks]

    def getDependencies(self):
        return self.dependencies

    def showDeps(self):
        options = {
            "node_color": "blue",
            "node_size": 600,
            "node_shape": "o",
            "width": 1,
            "arrowstyle": "-|>",
            "arrowsize": 12,
        }
        graph_data = []
        res = {k: v for d in self.dependencies for k, v in d.items()}
        deps = {k: v for k, v in res.items() if v is not None}
        for k, v in deps.items():
            if isinstance(v, list):
                result = map(lambda x: (x, k), v)
                graph_data += list(result)
            if isinstance(v, str):
                result = {k: v}
                graph_data += list(result)
        graph_data
        self.graph.add_edges_from(graph_data)
        nt = Network(directed=True, notebook=True)
        nt.from_nx(self.graph)
        return nt.show("nx.html")
        # return nx.draw_networkx(self.graph, arrows=True, **options)

    def addSparkJob(
        self, name: str, sparkManifest: str, dependencies: List = None, *args, **kwargs
    ) -> str:
        taskDict: Dict[str, Any] = {}
        taskDict["name"] = name
        taskDict["dependencies"] = dependencies
        taskDict["template"] = "sparkk8sScala"
        self.dependencies.append({name: dependencies})
        try:
            self.task.append(
                {
                    "resources": {"name": "sparkk8sScala", "manifest": sparkManifest},
                    "workflow": taskDict,
                }
            )
        except Exception as e:
            print(e)
        return "{0} added ".format(name)

    def addJob(
        self,
        name: str,
        parameters: List[Dict[str, str]] = None,
        dependencies: List = None,
        *args,
        **kwargs
    ):
        taskDict: Dict[str, Any] = {}
        taskDict["name"] = name
        taskDict["dependencies"] = dependencies
        taskDict["template"] = "sparkrunner"
        self.dependencies.append({name: dependencies})
        try:
            taskDict["arguments"] = V1alpha1Arguments(parameters=parameters)
        except Exception as e:
            print(e)
        try:
            self.task.append({"workflow": taskDict})
        except Exception as e:
            print(e)
        return "task added"

    def runProfilerClient(
        self,
        name: str,
        parameters: List[Dict[str, str]] = None,
        dependencies: List = None,
        *args,
        **kwargs
    ):
        taskDict: Dict[str, Any] = {}
        taskDict["name"] = name
        taskDict["dependencies"] = dependencies
        taskDict["template"] = "jobprofilerclient"
        self.dependencies.append({name: dependencies})
        try:
            taskDict["arguments"] = V1alpha1Arguments(parameters=parameters)
        except Exception as e:
            print(e)
        try:
            self.task.append(taskDict)
        except Exception as e:
            print(e)
        return "task added"

    def viewData(
        self,
        name: str,
        parameters: List[Dict[str, str]] = None,
        dependencies: List = None,
        *args,
        **kwargs
    ):
        taskDict: Dict[str, Any] = {}
        taskDict["name"] = name
        taskDict["dependencies"] = dependencies
        taskDict["template"] = "viewdata"
        self.dependencies.append({name: dependencies})
        try:
            taskDict["arguments"] = V1alpha1Arguments(parameters=parameters)
        except Exception as e:
            print(e)
        try:
            self.task.append(taskDict)
        except Exception as e:
            print(e)
        return "task added"

    def runPromethuesJob(
        self,
        name: str,
        parameters: List[Dict[str, str]] = None,
        dependencies: List = None,
        *args,
        **kwargs
    ):
        taskDict: Dict[str, Any] = {}
        taskDict["name"] = name
        taskDict["dependencies"] = dependencies
        taskDict["template"] = "promethuesrunner"
        self.dependencies.append({name: dependencies})
        try:
            taskDict["arguments"] = V1alpha1Arguments(parameters=parameters)
        except Exception as e:
            print(e)
        try:
            self.task.append(taskDict)
        except Exception as e:
            print(e)
        return "task added"
