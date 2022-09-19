import re
from typing import Tuple
from data_harmonization.main.code.tiger.features.Distance import Distance
import numpy as np

from data_harmonization.main.code.tiger.model.datamodel import RawEntity

class Train():

    # TODO: Get clustering output from CSV [Postive Examples]

    # TODO: Create negative examples [Sligtly Tricky]

    # TODO: Concat both with appropriate labels

    # TODO: sklearn pipeline => Feature => Feature Engineering (Distance.py) => Grid Search => model fit => predictions

    # TODO: predict on all pair within that cluster