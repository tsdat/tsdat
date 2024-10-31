from ._base_transformer import _baseTransformer


class Interpolate(_baseTransformer):
    transformation_type: str = "TRANS_INTERPOLATE"
    method: str = "linear"
