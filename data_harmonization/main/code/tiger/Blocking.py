import itertools
import os
import random
import re
import sys
from os import listdir
from typing import Any, Optional

import numpy as np
import pandas as pd

from data_harmonization.main.code.tiger.model.datamodel import *
from data_harmonization.main.code.tiger.model.GeocodedAddress import GeocodedAddress
from data_harmonization.main.code.tiger.model.PostalAddress import PostalAddress
from data_harmonization.main.code.tiger.model.SemiMergedProfile import SemiMergedProfile
from data_harmonization.main.code.tiger.Sanitizer import Sanitizer
from data_harmonization.main.code.tiger.transformer import (
    CityTransformer,
    NameTransformer,
    PostalAddressTransformer,
    StateTransformer,
)
from data_harmonization.main.code.tiger.transformer.utils import StringSupport
from data_harmonization.main.code.tiger.clustering.minLSH import MinLSH


class Blocking:
    """create cluster pairs using minLSH alogorithm"""

    def prepare_data(
        self, n_docs: Optional[int] = 1000, data: Optional[pd.DataFrame] = None
    ) -> dict:
        """Flatten raw_profiles to fields which are only string / int / float

        :return: flat raw profile
        """
        flattened_raw_profile = {}
        raw_profiles = data
        if not data:
            filenames = listdir(os.getcwd() + "/data_harmonization/main/data/")
            csv_filenames = [
                filename for filename in filenames if filename.endswith(".csv")
            ]
            for csv_file in csv_filenames:
                raw_profiles = pd.concat(
                    [
                        raw_profiles,
                        pd.read_csv(
                            os.getcwd() + f"/data_harmonization/main/data/{csv_file}"
                        ),
                    ],
                    axis="index",
                )
        raw_profiles_with_tokens = raw_profiles.apply(
            lambda r: Sanitizer().toRawEntity(r), axis=1
        )  # .filter(lambda p: p.id._isNotEmpty)
        id = 0
        for raw_ent in raw_profiles_with_tokens.sample(n_docs):
            semiflatten_raw_profile = {}
            raw_dict = raw_ent.__dict__
            for k, v in raw_dict.items():
                if not isinstance(v, (int, str, float)) and v:
                    for k1, v1 in v.__dict__.items():
                        semiflatten_raw_profile[k1] = v1
                else:
                    semiflatten_raw_profile[k] = v
            flattened_raw_profile[str(id)] = semiflatten_raw_profile
            id = id + 1
        return flattened_raw_profile

    def do_blocking(self, docs: dict):
        """Block datasets"""
        block_ = MinLSH()
        return block_.block_datasets(docs)

    def get_statistics(self, docs, n_similar_docs):
        """Get blocking statistics

        :parameter docs: output from blocking
        :parameter n_similar_docs: number of similar docs

        :return calculated statistics
        """
        block_ = MinLSH()
        return block_.compute_statistics(docs, n_similar_docs)


if __name__ == "__main__":

    n_similar_docs = 10

    clus = Blocking()
    prepared_data = clus.prepare_data(n_docs=300)
    similar_docs = clus.do_blocking(docs=prepared_data)

    print(clus.get_statistics(similar_docs, n_similar_docs))
    df_dict = {}
    for pair1, pair2 in similar_docs:
        for k, v in pair1.items():
            key_var = f"{k}" + "_pair1"
            if not df_dict.get(key_var, None):
                df_dict[key_var] = []
                df_dict[key_var].append(v)
            else:
                df_dict[key_var].append(v)

        for k, v in pair2.items():
            key_var = f"{k}" + "_pair2"
            if not df_dict.get(key_var, None):
                df_dict[key_var] = []
                df_dict[key_var].append(v)
            else:
                df_dict[key_var].append(v)
    df = pd.DataFrame(df_dict)
    df.to_csv(os.getcwd() + "/similiar.csv")
