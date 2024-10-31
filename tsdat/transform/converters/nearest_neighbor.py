from ._base_transformer import _baseTransformer


class NearestNeighbor(_baseTransformer):
    transformation_type: str = "TRANS_SUBSAMPLE"
    method: str = "nearest"
