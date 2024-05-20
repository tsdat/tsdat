from ._adi_base_transformer import _ADIBaseTransformer


class Automatic(_ADIBaseTransformer):
    transformation_type: str = "TRANS_AUTO"
