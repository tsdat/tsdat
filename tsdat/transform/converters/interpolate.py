from ._adi_base_transformer import _ADIBaseTransformer


class Interpolate(_ADIBaseTransformer):
    transformation_type: str = "TRANS_INTERPOLATE"
