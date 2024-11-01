from ._adi_base_transformer import _ADIBaseTransformer


class NearestNeighbor(_ADIBaseTransformer):
    transformation_type: str = "TRANS_SUBSAMPLE"
