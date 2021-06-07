from argo.workflows.client import (
    V1alpha1Arguments,
    V1Container,
    V1VolumeMount,
    V1alpha1ResourceTemplate,
)
from pathlib import Path
from typing import List, Any, Dict
import yaml
from itertools import zip_longest


class authContainers:
    def __init__(self, path: str = "./config.yaml"):
        self.path = Path(path)
        try:
            with open(self.path) as file:
                self.data = yaml.load(file, Loader=yaml.FullLoader)
        except Exception as e:
            print(y)

    def getData(self) -> Dict:
        return self.data["Resources"]

    def getResources(self, spark_manifest: List) -> List:
        res = []
        data = [
            {**u, **v}
            for u, v in zip_longest(
                self.data["Resources"], spark_manifest, fillvalue={}
            )
        ]
        for rsc in data:
            res.append(
                {
                    "name": rsc["name"],
                    "resource": V1alpha1ResourceTemplate(
                        action=rsc["action"],
                        success_condition=rsc["successCondition"],
                        failure_condition=rsc["failureCondition"],
                        manifest=rsc["manifest"],
                    ),
                }
            )
        return res

    def getContainers(self) -> List:
        auth = []
        for container in self.data["Containers"]:
            auth.append(
                {
                    "name": container["name"],
                    "inputs": V1alpha1Arguments(
                        parameters=container.get("parameters", None)
                    ),
                    "container": V1Container(
                        image=container["image"],
                        # image_pull_policy=container["image_pull_policy"],
                        # volume_mounts=[V1VolumeMount(name='hive-pv-storage', mount_path='/profile_data')],
                        command=container["command"],
                        # args=container["args"]
                    ),
                }
            )
        return auth
