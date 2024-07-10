from tsdat.tstring import TEMPLATE_REGISTRY, Template


def get_datastream(**global_attrs: str) -> str:
    datastream_template = Template(TEMPLATE_REGISTRY["datastream"])
    return datastream_template.substitute(global_attrs)
