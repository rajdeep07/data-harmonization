import itertools
from operator import ipow
import os
import random
import re
import sys
from os import listdir
from typing import Any, Optional
from unittest import result

import numpy as np
import pandas as pd
from data_harmonization.main.code.tiger.clustering.utils import (
    _flatten_list,
    _isNotEmpty,
)

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
from data_harmonization.main.resources.config import (
    minlsh_band_size,
    minlsh_max_doc_length,
    minlsh_n_docs,
    minlsh_n_hashes,
    minlsh_n_similar_docs,
    minlsh_shingle_size,
    minlsh_collect_indexes,
)


class MinLSH:
    def __init__(self):
        self.n_hashes = minlsh_n_hashes
        self.band_size = minlsh_band_size
        self.shingle_size = minlsh_shingle_size
        self.collect_indexes = minlsh_collect_indexes
        self.flat_raw_profile = {}

    # Step 1 : Create shingles
    def _create_shingles(
        self, input: Optional[str], shingle_size
    ) -> Optional[list[str]]:
        """Create shingles of provided string

        :return: list of shingles
        """

        def _shingles(x: str) -> list[str]:
            i = (
                x.lower()
            )  # TODO: remove extra unnecessary characters when creating shingles
            if len(i) > shingle_size:
                return list(
                    map(
                        lambda j: i[j : j + shingle_size],
                        range(0, len(i) - shingle_size + 1),
                    )
                )
            else:
                return [i]

        return _flatten_list(
            list(
                map(
                    _shingles,
                    list(filter(_isNotEmpty, re.split("[-\s\\\\,]s*", input))),
                )
            )
        )

    # Step 2: Tokenization
    def _create_tokens(self, profile: dict, shingle_size: int) -> str:
        """Create shingles of values of provided dictionary if they are string type

        :return: list of all shingles
        """
        output = []
        for v in profile.values():
            if isinstance(v, str):
                output.extend(self._create_shingles(v, shingle_size))
        # return output.flatten.filter(lambda x: (x is not None) and x._isNotEmpty)
        return output

    # Step 3: Hash
    def _get_minhash(self, tot_shingle, n_hashes, random_strings) -> list:
        """Calculate minHash signature

        :return: minHash signatures"""
        minhash_row = []
        for i in range(n_hashes):
            minhash = sys.maxsize
            for shingle in tot_shingle:
                hash_candidate = abs(hash(shingle + random_strings[i]))
                if hash_candidate < minhash:
                    minhash = hash_candidate
            minhash_row.append(minhash)
        return minhash_row

    # LSH ==> MinLSH [More reading]
    def _get_band_hashes(self, minhash_row, band_size) -> list:
        band_hashes = []
        for i in range(len(minhash_row)):
            if i % band_size == 0:
                if i > 0:
                    band_hashes.append(band_hash)
                band_hash = 0
            band_hash += hash(minhash_row[i])
        return band_hashes

    # Similar documents : LSH
    def block_datasets(self, docs: dict):
        """Block datasets using minLSH

        :return: similar docs or matched values
        """
        hash_bands = {}
        random_strings = [str(random.random()) for _ in range(self.n_hashes)]
        doc_num = 0
        for doc in docs.values():
            shingles = self._create_tokens(doc, self.shingle_size)
            minhash_row = self._get_minhash(shingles, self.n_hashes, random_strings)
            band_hashes = self._get_band_hashes(minhash_row, self.band_size)

            doc_member = doc_num if self.collect_indexes else doc
            for i in range(len(band_hashes)):
                if i not in hash_bands:
                    hash_bands[i] = {}
                if band_hashes[i] not in hash_bands[i]:
                    hash_bands[i][band_hashes[i]] = [doc_member]
                else:
                    hash_bands[i][band_hashes[i]].append(doc_member)
            doc_num += 1

        similar_docs = []
        for i in hash_bands:
            for hash_num in hash_bands[i]:
                if len(hash_bands[i][hash_num]) > 1:
                    for pair in itertools.combinations(hash_bands[i][hash_num], r=2):
                        similar_docs.append(pair)
        return similar_docs

    def compute_statistics(self, docs, n_similar_docs):
        """Calculate blocking statistics

        :parameter docs: output from blocking
        :parameter n_similar_docs: number of similar docs

        :return: dicttionary of calculated statistics
        """
        r = float(minlsh_n_hashes / minlsh_band_size)
        similarity = (1 / r) ** (1 / float(minlsh_band_size))
        result = {
            "Similarity": f"{similarity}%",
            "Similar Pairs": len(docs),
            "Test": "Passed: All similar pairs found."
            if len(docs) == n_similar_docs
            else "Failed.",
        }
        return result
