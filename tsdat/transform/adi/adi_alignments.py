class ADIAlignments:
    LEFT = "LEFT"
    CENTER = "CENTER"
    RIGHT = "RIGHT"

    label_to_int = {LEFT: 0, CENTER: 0.5, RIGHT: 1}

    @staticmethod
    def get_adi_value(parameter_value: str):
        return ADIAlignments.label_to_int.get(parameter_value)
