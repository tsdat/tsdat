from ._adi_base_transformer import _ADIBaseTransformer


class BinAverage(_ADIBaseTransformer):
    transformation_type: str = "TRANS_BIN_AVERAGE"
