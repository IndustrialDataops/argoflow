import yaml
import re

try:
    from yaml import CDumper as Dumper
except ImportError:
    from yaml import Dumper


def remove_none(obj):
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(remove_none(x) for x in obj if x is not None)
    elif isinstance(obj, dict):
        return type(obj)(
            (remove_none(k), remove_none(v))
            for k, v in obj.items()
            if k is not None and v is not None
        )
    else:
        return obj


class BlockDumper(Dumper):
    def represent_scalar(self, tag, value, style=None):
        if re.search("\n", value):
            style = "|"
            # remove trailing spaces and newlines which are not allowed in YAML blocks
            value = re.sub(" +\n", "\n", value).strip()

        return super().represent_scalar(tag, value, style)
