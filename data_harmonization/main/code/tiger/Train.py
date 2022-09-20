import re
from typing import Tuple
from data_harmonization.main.code.tiger.features.Distance import Distance
import numpy as np
from Cluster import Cluster

from data_harmonization.main.code.tiger.model.datamodel import RawEntity

class Train():
    cluster_pairs = None

    # TODO: Get clustering output from CSV [Postive Examples]
    def createClusterPairs(self):
        n_hashes = 200
        band_size = 5
        shingle_size = 5
        n_docs = 200
        cluster = Cluster()
        flatten_rawprofile = cluster.createflattenRawprofile(n_docs)
        self.cluster_pairs = cluster.get_similar_docs(docs=flatten_rawprofile, n_hashes=n_hashes, \
            band_size = band_size, shingle_size= shingle_size, collectIndexes=False)
        return self

    # TODO: Create negative examples [Sligtly Tricky]

    # TODO: Concat both with appropriate labels

    # TODO: sklearn pipeline => Feature => Feature Engineering (Distance.py) => Grid Search => model fit => predictions

    # TODO: predict on all pair within that cluster

if __name__ == "__main__":
    train = Train().createClusterPairs()
    print(train.cluster_pairs)