from abc import ABCMeta

from typing import Dict, Any, List
from inflection import camelize
from inflection import dasherize
from inflection import underscore
from argo.workflows.client import (
    V1alpha1DAGTemplate,
    V1alpha1Workflow,
    V1ObjectMeta,
    V1alpha1WorkflowSpec,
    V1Volume,
    V1PersistentVolumeClaimVolumeSource,
)
from argoflow.utils import *
from argoflow.authorizedContainers import *


from argo.workflows.client import (
    ApiClient,
    Configuration,
    WorkflowServiceApi,
    V1alpha1WorkflowCreateRequest,
)


class workflowMeta(ABCMeta):
    def __new__(cls, name, bases, props: Dict[str, Any], **kwargs):
        spec_dict: Dict[str, Any] = {}
        templates_dict: Dict[str, Any] = {}
        workflow_name = "argo-job"
        metadata_dict = dict(name="", generate_name=f"{workflow_name}-")
        props["metadata"] = metadata_dict
        spec_dict["entrypoint"] = "main"
        spec_dict["pod_gc"] = {'strategy':'OnWorkflowSuccess'}
        # spec_dict['auth_templates']  = auth  # will  pre-load all the authorized containers templates
        templates_dict["name"] = "main"
        templates_dict["dag"] = []

        props["spec"] = spec_dict
        klass = super().__new__(cls, name, bases, props)
        cls.__compile(klass, name, bases, props)
        return klass

    @classmethod
    def __compile(cls, klass, name, bases, props: Dict[str, Any]):
        metadata: Dict[str, Any] = {}
        for key, prop in props.items():
            if isinstance(prop, dict) or isinstance(prop, list):
                metadata[key] = prop
        klass.metadata = metadata


class workflow(metaclass=workflowMeta):
    def __init__(self, name: str, data: List, authpath: str = "./config.yaml"):
        self.resources = [
            pos.get("resources")
            for pos in data
            if pos.get("resources", None) is not None
        ]
        self.wf = [
            pos.get("workflow") for pos in data if pos.get("workflow", None) is not None
        ]
        self.name = name
        self.template: V1alpha1Workflow = None
        self.dags = V1alpha1DAGTemplate(tasks=self.wf)
        # self.metadata["spec"]["volumes"] = [V1Volume(name='hive-pv-storage',
        #      persistent_volume_claim=(V1PersistentVolumeClaimVolumeSource(
        #          claim_name='hive-pv-claim')))]
        self.metadata["metadata"]["generate_name"] = (
            dasherize(underscore(self.name)) + "-"
        )
        self.metadata["spec"]["templates"] = (
            [{"name": "main", "dag": self.dags}]
            + authContainers(authpath).getContainers()
            + authContainers(authpath).getResources(self.resources)
        )

    def raw_dict(self):
        return self.metadata

    @classmethod
    def getClient(self):
        config = Configuration(host="https://localhost:2746")
        client = ApiClient(configuration=config)
        service = WorkflowServiceApi(api_client=client)
        return (service, client)

    def generate_template(self) -> V1alpha1Workflow:
        self.template = V1alpha1Workflow(
            api_version="argoproj.io/v1alpha1",
            kind="Workflow",
            metadata=V1ObjectMeta(**self.metadata["metadata"]),
            status={},
            spec=V1alpha1WorkflowSpec(**self.metadata["spec"]),
        )
        return self.template

    def get_dict(self) -> Dict:
        result = V1alpha1Workflow.to_dict(self.generate_template())
        return remove_none(result)

    def get_yaml(self) -> str:
        service, client = self.getClient()
        if self.template is None:
            self.generate_template()
        d: Dict[str, Any] = client.sanitize_for_serialization(self.template)
        serialized = yaml.dump(d, Dumper=BlockDumper)
        return serialized

    def submit(self):
        if self.template is None:
            self.generate_template()
        service, client = self.getClient()
        body = client.sanitize_for_serialization(self.template)
        # print(yaml.dump(body, Dumper=BlockDumper))
        name_s = ""
        try:
            x = service.create_workflow(
                "argo", V1alpha1WorkflowCreateRequest(workflow=body)
            )
        except Exception as e:
            print(e)
        else:
            x = x.to_dict()
            name_s = x["metadata"]["name"]
            print(name_s)
        print(self.name + " Submmitted")
        return name_s

    def get_metadata(self, name_submitted):
        service, client = self.getClient()
        status = service.get_workflow("argo", name_submitted).status
        return status.to_dict()
