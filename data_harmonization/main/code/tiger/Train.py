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


class Train():
    cluster_pairs = None

    # TODO: Get clustering output [Postive Examples]
    def createClusterPairs(self):
        n_hashes = 200
        band_size = 5
        shingle_size = 5
        n_docs = 200
        cluster = Cluster()
        flatten_rawprofile = cluster.createflattenRawprofile(n_docs)
        print("Current Flatten raw profiles", flatten_rawprofile)
        self.cluster_pairs = cluster.get_similar_docs(docs=flatten_rawprofile, n_hashes=n_hashes, \
            band_size = band_size, shingle_size= shingle_size, collectIndexes=False)
        print("Intial clusters",self.cluster_pairs)
        return self

    def _getPositiveExamples(self):
        _positive_df = pd.DataFrame()


        for pairs in self.cluster_pairs:
            _positive = Features().get(pairs)
            _positive["target"] = 1
            for key, value in _positive.items():
                _positive_df[key] = value

        return _positive_df

    # TODO: Create negative examples [Slightly Tricky]
    def _getNegativeExamples(self, cluster_pairs):
        duplicate_ids = set()
        for pair1, pair2 in cluster_pairs:
            duplicate_ids.add(pair1["cluster_id"])
            duplicate_ids.add(pair2["cluster_id"])
        _number_of_negative_examples = 5*len(duplicate_ids)

        id = 0
        unique_ids = set()
        for profile in flatten_rawprofile.values():
            while id <= _number_of_negative_examples:
                if profile["cluster_id"] not in duplicate_id:
                    unique_ids.add(profile["cluster_id"])
                id += 1

        _negative_df = pd.DataFrame()
        feature_list = ["Name", "City", "Zip", "Address"]
        # TODO: Find a better way
        unique_ids = [j for j in
                      [i for i in flatten_rawprofile if flatten_rawprofile[i]["cluster_id"]
                      not in duplicate_ids].values()]

        p_id = 0
        _negative_df = pd.DataFrame()
        for id1["cluster_id"] in unique_ids:
            for id2["cluster_id"] in unique_ids:
                if id1["cluster_id"] != id2["cluster_id"]:
                    while p_id <= _number_of_negative_examples:
                        _negative = np.darray()
                        for feature in feature_list:
                            _negative += Features.engineerFeatures(id1["".format(feature)], id2["".format(feature)])
                        _negative_df["target"] = 0
                        _negative_df["features"] = _negative.flatten()
                        p_id += 1

        return _negative_df

    # TODO: Concat both with appropriate labels
    def concat_examples(self, _positive_df, _negative_df):
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
    print(_positive_df)
    _negative_df = train._getNegativeExamples(train.cluster_pairs)
    data = train.concat_examples(_positive_df, _negative_df)

    # Change Class column into target_0 ([1 0] for No Match data) and target_1 ([0 1] for Match data)
    one_hot_data = pd.get_dummies(data, columns=['target'])

    # split
    df_X = one_hot_data.drop(['target_0', 'target_1'], axis=1)
    df_y = one_hot_data[['target_0', 'target_1']]

    # Convert both data_frames into np arrays of float32
    ar_X, ar_y = np.asarray(df_X.values, dtype='float32'), np.asarray(df_y.values, dtype='float32')

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
    X_train_node = tf.placeholder(tf.float32, [None, input_dimensions], name='X_train')
    y_train_node = tf.placeholder(tf.float32, [None, output_dimensions], name='y_train')

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
    y_train_prediction = network(X_train_node)
    y_test_prediction = network(X_test_node)

    # Cross entropy loss function measures differences between actual output and predicted output
    cross_entropy = tf.losses.softmax_cross_entropy(y_train_node, y_train_prediction)

    # Adam optimizer function will try to minimize loss (cross_entropy) but changing the 3 layers' variable values at a
    #   learning rate of 0.005
    optimizer = tf.train.AdamOptimizer(0.005).minimize(cross_entropy)

    with tf.Session() as session:
        tf.global_variables_initializer().run()
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