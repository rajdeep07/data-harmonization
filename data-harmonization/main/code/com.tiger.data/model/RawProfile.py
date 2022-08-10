from dataclasses import dataclass
from typing import Optional

@dataclass(unsafe_hash=True)
class RawProfile:
    """[Summary]
    :param [ParamName]: [ParamDescription], defaults to [DefaultParamVal]
    :type [ParamName]: [ParamType](, optional)
    ...
    :raises [ErrorType]: [ErrorDescription]
    ...
    :return: [ReturnDescription]
    :rtype: [ReturnType]
    """
    id: str
    name: Optional[str]
    city: Optional[str]
    address: Optional[str]
    state: Optional[str]
    zipcode: Optional[str]
    source: Optional[str]

