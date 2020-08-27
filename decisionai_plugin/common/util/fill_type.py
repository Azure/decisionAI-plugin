import enum


# Using enum class create enumerations
class Fill(enum.Enum):
    Previous = 1
    Subsequent = 2
    Linear = 3
    Zero = 4
    Pad = 5
    NotFill = 6
