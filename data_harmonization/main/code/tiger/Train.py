import random  
import re
from shutil import ignore_patterns
from typing import Tuple
from data_harmonization.main.code.tiger.features.Distance import Distance
import numpy as np
from Cluster import Cluster
from data_harmonization.main.code.tiger.Features import Features
from data_harmonization.main.code.tiger.model.datamodel import RawEntity
import tensorflow as tf
import pandas as pd
import time

tf.compat.v1.disable_v2_behavior()

class Train():
    flatten_rawprofile = None
    cluster_pairs = None
    _positive_df = pd.DataFrame()
    _negative_df = pd.DataFrame()

    # TODO: Get clustering output [Postive Examples]
    def createClusterPairs(self):
        n_hashes = 200
        band_size = 5
        shingle_size = 5
        n_docs = 400
        cluster = Cluster()
        self.flatten_rawprofile = cluster.createflattenRawprofile(n_docs)
        # print("Current Flatten raw profiles", self.flatten_rawprofile)
        self.cluster_pairs = cluster.get_similar_docs(docs=self.flatten_rawprofile, n_hashes=n_hashes, \
            band_size = band_size, shingle_size= shingle_size, collectIndexes=False)
        # print("Intial clusters",self.cluster_pairs)
        return self

    def _getPositiveExamples(self):
        for pairs in self.cluster_pairs:
            _positive = Features().get(pairs)
            row = pd.DataFrame(data=[[pairs[0]["cluster_id"],pairs[1]["cluster_id"],_positive, 1]], columns=("cluster_id1","cluster_id2","feature", "target"))
            self._positive_df = pd.concat([self._positive_df, row], axis=0, ignore_index=True)            
        # print(_positive_df.head())
        return self._positive_df

    # TODO: Create negative examples [Slightly Tricky]
    def _getNegativeExamples(self):
        total_length = len(self.flatten_rawprofile)
        negativePair_set = set()
        negativeDfSize = 5*(self._positive_df.shape[0])
        prev_size = -1
        while len(negativePair_set) < negativeDfSize:
            pair1_row = random.randint(0, total_length-1)
            pair2_row = random.randint(0, total_length-1)
            pair1 = self.flatten_rawprofile[str(pair1_row)]
            pair2 = self.flatten_rawprofile[str(pair2_row)]
            
            row1 = (self._positive_df["cluster_id1"] == pair1["cluster_id"]).any() \
                and (self._positive_df["cluster_id2"] == pair2["cluster_id"]).any()
            row2 = (self._positive_df["cluster_id1"] == pair2["cluster_id"]).any() \
                and (self._positive_df["cluster_id2"] == pair1["cluster_id"]).any()

            if not row1 and not row2:
                negativePair_set.add((pair1["cluster_id"], pair2["cluster_id"]))

            if len(negativePair_set) > prev_size:
                prev_size = len(negativePair_set)
                _negative = Features().get((pair1, pair2))
                row = pd.DataFrame(data=[[pair1["cluster_id"],pair2["cluster_id"],_negative, 0]], columns=("cluster_id1","cluster_id2","feature", "target"))
                self._negative_df = pd.concat([self._negative_df, row], axis=0, ignore_index=True)

            

        # duplicate_id = set()
        # for pair1, pair2 in cluster_pairs:
        #     duplicate_id.add(pair1["cluster_id"])
        #     duplicate_id.add(pair2["cluster_id"])
        # _number_of_negative_examples = 5*len(duplicate_id)

        # id = 0
        # unique_ids = set()
        # for profile in self.flatten_rawprofile.values():
        #     while id <= _number_of_negative_examples:
        #         if profile["cluster_id"] not in duplicate_id:
        #             unique_ids.add(profile["cluster_id"])
        #         id += 1

        # _negative_df = pd.DataFrame()
        # feature_list = ["Name", "City", "Zip", "Address"]
        # # TODO: Find a better way
        # unique_ids = [j for j in
        #               [i for i in self.flatten_rawprofile if self.flatten_rawprofile[i]["cluster_id"]
        #               not in duplicate_id].values()]

        # p_id = 0
        # _negative_df = pd.DataFrame()
        # for id1["cluster_id"] in unique_ids:
        #     for id2["cluster_id"] in unique_ids:
        #         if id1["cluster_id"] != id2["cluster_id"]:
        #             while p_id <= _number_of_negative_examples:
        #                 _negative = np.darray()
        #                 for feature in feature_list:
        #                     _negative += Features.engineerFeatures(id1["".format(feature)], id2["".format(feature)])
        #                 _negative_df["target"] = 0
        #                 _negative_df["features"] = _negative.flatten()
        #                 p_id += 1

        return self._negative_df

    # TODO: Concat both with appropriate labels
    def concat_examples(self, _positive_df, _negative_df):
        _positive_df['feature'] = _positive_df['feature'].to_numpy().flatten()
        _negative_df['feature'] = _negative_df['feature'].to_numpy().flatten()
        return pd.concat([_positive_df, _negative_df])

    # Function to run an input tensor through the 3 layers and output a tensor that will give us a match/non match result
    # Each layer uses a different function to fit lines through the data and predict whether a given input tensor will \
    #   result in a match or non match profiles
    def network(self, input_tensor):
        # Sigmoid fits modified data well
        layer1 = tf.nn.sigmoid(tf.matmul(input_tensor, weight_1_node) + biases_1_node)
        # Dropout prevents model from becoming lazy and over confident
        layer2 = tf.nn.dropout(tf.nn.sigmoid(tf.matmul(layer1, weight_2_node) + biases_2_node), 0.85)
        # Softmax works very well with one hot encoding which is how results are outputted
        layer3 = tf.nn.softmax(tf.matmul(layer2, weight_3_node) + biases_3_node)
        return layer3

    # Function to calculate the accuracy of the actual result vs the predicted result
    def calculate_accuracy(self, actual, predicted):
        actual = np.argmax(actual, 1)
        predicted = np.argmax(predicted, 1)
        return (100 * np.sum(np.equal(predicted, actual)) / predicted.shape[0])
    # TODO: predict on all pair within that cluster

if __name__ == "__main__":
    train = Train().createClusterPairs()
    print("Training dataset",train.cluster_pairs)

    _positive_df = train._getPositiveExamples()
    # print(_positive_df)
    _negative_df = train._getNegativeExamples()
    # print(_negative_df)
    data = train.concat_examples(_positive_df, _negative_df)

    # Change Class column into target_0 ([1 0] for No Match data) and target_1 ([0 1] for Match data)
    one_hot_data = pd.get_dummies(data, prefix=['target'], columns=["target"])

    # split
    df_X = one_hot_data.drop(['target_0', 'target_1', "cluster_id1", "cluster_id2"], axis=1)
    df_y = one_hot_data[['target_0', 'target_1']]
    print(df_X.shape)
    # print(df_X["feature"].head())
    # print(df_X["feature"].values[:5])
    # print(df_X["feature"].values[:5].shape)
    # print(df_X.values, df_X.values.shape)
    # df_X["feature"] = df_X["feature"].apply(lambda x: np.asarray(x))
    print(np.stack(df_X["feature"].values).shape)
    # print(np.asarray(df_X["feature"].values))

    # Convert both data_frames into np arrays of float32
    ar_y = np.stack(df_y.values).astype(dtype='float32')
    ar_X = np.stack(df_X["feature"].values).astype(dtype='float32')
    # ar_X = np.asarray(df_X['feature']).astype('float32')
    # Allocate first 80% of data into training data and remaining 20% into testing data
    train_size = int(0.8 * len(ar_X))
    (raw_X_train, raw_y_train) = (ar_X[:train_size], ar_y[:train_size])
    (raw_X_test, raw_y_test) = (ar_X[train_size:], ar_y[train_size:])

    # Gets a percent of match vs no match (6% of data are match?)
    count_match, count_no_match = np.unique(data['target'], return_counts=True)[1]
    match_ratio = float(count_match / (count_match + count_no_match))
    print('Percent of match ratios: ', match_ratio)

    # Applies a logit weighting to match profiles to cause model to pay more attention to them
    weighting = 1 / match_ratio
    raw_y_train[:, 1] = raw_y_train[:, 1] * weighting

    # 30 cells for the input
    input_dimensions = ar_X.shape[1]
    # 2 cells for the output
    output_dimensions = ar_y.shape[1]
    # 100 cells for the 1st layer
    num_layer_1_cells = 100
    # 150 cells for the second layer
    num_layer_2_cells = 150

    # We will use these as inputs to the model when it comes time to train it (assign values at run time)
    X_train_node = tf.compat.v1.placeholder(tf.float32, [None, input_dimensions], name='X_train')
    y_train_node = tf.compat.v1.placeholder(tf.float32, [None, output_dimensions], name='y_train')

    # We will use these as inputs to the model once it comes time to test it
    X_test_node = tf.constant(raw_X_test, name='X_test')
    y_test_node = tf.constant(raw_y_test, name='y_test')

    # First layer takes in input and passes output to 2nd layer
    weight_1_node = tf.Variable(tf.zeros([input_dimensions, num_layer_1_cells]), name='weight_1')
    biases_1_node = tf.Variable(tf.zeros([num_layer_1_cells]), name='biases_1')

    # Second layer takes in input from 1st layer and passes output to 3rd layer
    weight_2_node = tf.Variable(tf.zeros([num_layer_1_cells, num_layer_2_cells]), name='weight_2')
    biases_2_node = tf.Variable(tf.zeros([num_layer_2_cells]), name='biases_2')

    # Third layer takes in input from 2nd layer and outputs [1 0] or [0 1] depending on match vs non match
    weight_3_node = tf.Variable(tf.zeros([num_layer_2_cells, output_dimensions]), name='weight_3')
    biases_3_node = tf.Variable(tf.zeros([output_dimensions]), name='biases_3')

    num_epochs = 100

    # Used to predict what results will be given training or testing input data
    # Remember, X_train_node is just a placeholder for now. We will enter values at run time
    y_train_prediction = train.network(X_train_node)
    y_test_prediction = train.network(X_test_node)

    # Cross entropy loss function measures differences between actual output and predicted output
    cross_entropy = tf.compat.v1.losses.softmax_cross_entropy(y_train_node, y_train_prediction)

    # Adam optimizer function will try to minimize loss (cross_entropy) but changing the 3 layers' variable values at a
    #   learning rate of 0.005
    optimizer = tf.compat.v1.train.AdamOptimizer(0.005).minimize(cross_entropy)

    with tf.compat.v1.Session() as session:
        tf.compat.v1.global_variables_initializer().run()
        for epoch in range(num_epochs):

            start_time = time.time()

            _, cross_entropy_score = session.run([optimizer, cross_entropy],
                                                 feed_dict={X_train_node: raw_X_train, y_train_node: raw_y_train})

            if epoch % 10 == 0:
                timer = time.time() - start_time

                print('Epoch: {}'.format(epoch), 'Current loss: {0:.4f}'.format(cross_entropy_score),
                      'Elapsed time: {0:.2f} seconds'.format(timer))

                final_y_test = y_test_node.eval()
                final_y_test_prediction = y_test_prediction.eval()
                final_accuracy = train.calculate_accuracy(final_y_test, final_y_test_prediction)
                print("Current accuracy: {0:.2f}%".format(final_accuracy))

        final_y_test = y_test_node.eval()
        final_y_test_prediction = y_test_prediction.eval()
        final_accuracy = train.calculate_accuracy(final_y_test, final_y_test_prediction)
        print("Final accuracy: {0:.2f}%".format(final_accuracy))

    final_match_y_test = final_y_test[final_y_test[:, 1] == 1]
    final_match_y_test_prediction = final_y_test_prediction[final_y_test[:, 1] == 1]
    final_match_accuracy = train.calculate_accuracy(final_match_y_test, final_match_y_test_prediction)
    print('Final match specific accuracy: {0:.2f}%'.format(final_match_accuracy))