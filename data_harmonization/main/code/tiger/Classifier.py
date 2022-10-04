from pickletools import optimize
import random
import tensorflow as tf
import numpy as np
import pandas as pd
import itertools
import time

from sklearn.model_selection import StratifiedShuffleSplit
from data_harmonization.main.code.tiger.Features import Features

tf.compat.v1.disable_v2_behavior()

class Classifier():
    def __init__(self) -> None:
        self.input_dim : int
        self.output_dim : int
        self.optimizer
        self.cross_entropy

    def _feature_data(self, features:pd.DataFrame, target:pd.DataFrame) -> pd.DataFrame:
        features = Features().get(features)
        return features, pd.get_dummies(target, prefix=["target"], columns=["target"]).values

    def _extract_postive_data(self, data:pd.DataFrame, threshold:float = 0.70) -> pd.DataFrame:
        high_thresh_data = data[data['confidence'] >= threshold]
        joined_data = pd.merge(high_thresh_data, high_thresh_data, how='inner', on='cluster_id')
        joined_data.drop_duplicates(inplace=True)
        joined_data = joined_data[~(joined_data['id_x'] == joined_data['id_y'])]
        return joined_data

    def _extract_negative_data(self, data:pd.DataFrame, match_ratio:float=0.5) -> pd.DataFrame:
        cluster_ids = data['cluster_id']
        master_set = set()
        while len(master_set) < data.shape[0] * match_ratio:
            clus_id1, clus_id2 = random.sample(cluster_ids, 2)
            right_ids = set(data[data['cluster_id'] == clus_id1]['id'])
            left_ids = set(data[data['cluster_id'] == clus_id2]['id'])
            right_ids.difference_update(left_ids)
            left_ids.difference_update(right_ids)
            master_set.add(set(itertools.product(right_ids, left_ids)))

    
    def _preprocess_data(self, data:pd.DataFrame) -> pd.DataFrame:
        positive_df = self._extract_postive_data(data)
        negative_df = self._extract_negative_data(data)
        data = pd.concat([positive_df, negative_df])
        data['feature'], data['target'] = self._feature_data(data.drop('target', axis=1), data['feature'])
     
    def _train_test_split(self, feature : np.array, target : np.array, n_splits : int = 1, test_size : float = 0.2,
                        random_state : int = 42) -> np.array:

        split = StratifiedShuffleSplit(n_splits, test_size, random_state)
        for train_index, validation_index in split.split(feature, target):
            raw_X_train, raw_X_test = feature[train_index], feature[validation_index]
            raw_y_train, raw_y_test = target[train_index], target[validation_index]
        
        return raw_X_train, raw_X_test, raw_y_train, raw_y_test

    def _network(self, input_tensor):
        # Sigmoid fits modified data well
        layer1 = tf.nn.sigmoid(tf.matmul(input_tensor, self.weight_1_node) + self.biases_1_node)
        # Dropout prevents model from becoming lazy and over confident
        layer2 = tf.nn.dropout(
            tf.nn.sigmoid(tf.matmul(layer1, self.weight_2_node) + self.biases_2_node), 0.85
        )
        # Softmax works very well with one hot encoding which is how results are outputted
        layer3 = tf.nn.softmax(tf.matmul(layer2, self.weight_3_node) + self.biases_3_node)
        return layer3

    def _calculate_accuracy(self, actual, predicted):
        actual = np.argmax(actual, 1)
        predicted = np.argmax(predicted, 1)
        print(actual, predicted)
        return 100 * np.sum(np.equal(predicted, actual)) / predicted.shape[0]

    def _initialize_model(self) -> None:
        # First layer takes in input and passes output to 2nd layer
        self.weight_1_node = tf.Variable(
            tf.zeros([self.input_dim, self.num_layer_1_cells]), name="weight_1"
        )
        self.biases_1_node = tf.Variable(tf.zeros([self.num_layer_1_cells]), name="biases_1")

        # Second layer takes in input from 1st layer and passes output to 3rd layer
        self.weight_2_node = tf.Variable(
            tf.zeros([self.num_layer_1_cells, self.num_layer_2_cells]), name="weight_2"
        )
        self.biases_2_node = tf.Variable(tf.zeros([self.num_layer_2_cells]), name="biases_2")

        # Third layer takes in input from 2nd layer and outputs [1 0] or [0 1] depending on match vs non match
        self.weight_3_node = tf.Variable(
            tf.zeros([self.num_layer_2_cells, self.output_dim]), name="weight_3"
        )
        self.biases_3_node = tf.Variable(tf.zeros([self.output_dim]), name="biases_3")

        

    def train(self, data:pd.DataFrame, num_epochs, cross_entropy, optimizer):
        feature, target = self._preprocess_data(data)
        raw_X_train, raw_y_train, raw_X_test, raw_y_test = self._train_test_split(feature, target)
        X_train_node = tf.compat.v1.placeholder(
            tf.float32, [None, self.input_dim], name="X_train"
        )
        y_train_node = tf.compat.v1.placeholder(
            tf.float32, [None, self.output_dim], name="y_train"
        )
        X_test_node = tf.constant(raw_X_test, name="X_test")
        y_test_node = tf.constant(raw_y_test, name="y_test")
        y_train_prediction = self.network(self.X_train_node)
        y_test_prediction = self.network(self.X_test_node)
        self.cross_entropy = cross_entropy(self.y_train_node, y_train_prediction)
        self.optimizer = optimizer(0.005).minimize(self.cross_entropy)
        with tf.compat.v1.Session() as session:
            tf.compat.v1.global_variables_initializer().run()
            for epoch in range(num_epochs):
                start_time = time.time()
                _, cross_entropy_score = session.run(
                    [self.optimizer, self.cross_entropy],
                    feed_dict={self.X_train_node: self.raw_X_train, self.y_train_node: self.raw_y_train},)
                if epoch % 10 == 0:
                    timer = time.time() - start_time
                    print("Epoch: {}".format(epoch),
                        "Current loss: {0:.4f}".format(cross_entropy_score),
                        "Elapsed time: {0:.2f} seconds".format(timer),)
                    final_y_test = self.y_test_node.eval()
                    final_y_test_prediction = self.y_test_prediction.eval()
                    final_accuracy = self.calculate_accuracy(final_y_test, final_y_test_prediction)
                    print("Current accuracy: {0:.2f}%".format(final_accuracy))
            final_y_test = self.y_test_node.eval()
            final_y_test_prediction = self.y_test_prediction.eval()
            final_accuracy = self.calculate_accuracy(final_y_test, final_y_test_prediction)
            print("Final accuracy: {0:.2f}%".format(final_accuracy))


    def save_model(self, path:str):
        pass


if __name__ == "__main__":
    data = pd.read_csv('/home/navazdeens/data-harmonization/data_harmonization/main/data/benchmark.csv')

    Classifier().train(data=data)