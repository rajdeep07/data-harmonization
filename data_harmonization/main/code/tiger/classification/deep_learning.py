import random
import time

import numpy as np
import pandas as pd

# import tensorflow as tf
import tensorflow as tf
from sklearn.model_selection import StratifiedShuffleSplit

from data_harmonization.main.code.tiger.Features import Features

# tf.compat.v1.disable_v2_behavior()
tf = tf.compat.v1
tf.disable_v2_behavior()


class DeepLearning:
    """Classification using Deep Learning"""

    def __init__(self):
        self.flat_rawprofile = None
        self.cluster_pairs = None
        self._positive_df = pd.DataFrame()
        self._negative_df = pd.DataFrame()
        self.model_path = "data_harmonization/main/code/tiger/classification/models/"
        self.model_name = "classification_deep_learing_model"

    def _get_positive_examples(self):
        """Create positive examples (similar values) for classification

        :return: positive examples
        """
        for pairs in self.cluster_pairs:
            _positive = Features().get(pairs)
            row = pd.DataFrame(
                data=[[pairs[0]["id"], pairs[1]["id"], _positive, 1]],
                columns=("rid", "lid", "feature", "target"),
            )
            self._positive_df = pd.concat(
                [self._positive_df, row], axis=0, ignore_index=True
            )
        return self._positive_df

    def _get_negative_examples(self):
        """Create negative examples (nonsimilar values) for Classification.

        :return: negative examples
        """
        total_length = len(self.flat_rawprofile)
        negative_pair_set = set()
        negative_df_size = 1 * (self._positive_df.shape[0])
        prev_size = -1
        while len(negative_pair_set) < negative_df_size:
            pair1_row = random.randint(0, total_length - 1)
            pair2_row = random.randint(0, total_length - 1)
            pair1 = self.flat_rawprofile[str(pair1_row)]
            pair2 = self.flat_rawprofile[str(pair2_row)]

            row1 = (self._positive_df["rid"] == pair1["id"]).any() and (
                self._positive_df["lid"] == pair2["id"]
            ).any()
            row2 = (self._positive_df["lid"] == pair2["id"]).any() and (
                self._positive_df["rid"] == pair1["id"]
            ).any()

            if not row1 and not row2:
                negative_pair_set.add((pair1["id"], pair2["id"]))

            if len(negative_pair_set) > prev_size:
                prev_size = len(negative_pair_set)
                _negative = Features().get((pair1, pair2))
                row = pd.DataFrame(
                    data=[[pair1["id"], pair2["id"], _negative, 0]],
                    columns=("rid", "lid", "feature", "target"),
                )
                self._negative_df = pd.concat(
                    [self._negative_df, row], axis=0, ignore_index=True
                )
        return self._negative_df

    def _concat_examples(self, _positive_df, _negative_df):
        """Concatinate positive and negative examples.

        :return: concatinated dataset
        """
        _positive_df["feature"] = _positive_df["feature"].to_numpy().flatten()
        _negative_df["feature"] = _negative_df["feature"].to_numpy().flatten()
        return pd.concat([_positive_df, _negative_df])

    def _network(self, input_tensor):
        """Function to run an input tensor through the 3 layers and output a tensor
        that will give us a match/non match result.
        Each layer uses a different function to fit lines through the data and
        predict whether a given input tensor will result in a match or non match profiles.

        :return: match or non match profile"""
        # Sigmoid fits modified data well
        layer1 = tf.nn.sigmoid(
            tf.matmul(input_tensor, self.weight_1_node) + self.biases_1_node
        )
        # Dropout prevents model from becoming lazy and over confident
        layer2 = tf.nn.dropout(
            tf.nn.sigmoid(tf.matmul(layer1, self.weight_2_node) + self.biases_2_node),
            0.85,
        )
        # Softmax works very well with one hot encoding which is how results are outputted
        layer3 = tf.nn.softmax(
            tf.matmul(layer2, self.weight_3_node) + self.biases_3_node
        )
        return layer3

    def _calculate_accuracy(self, actual, predicted):
        """Calculate the accuracy of the actual result vs the predicted result

        :result: accuracy in percentage"""
        actual = np.argmax(actual, 1)
        predicted = np.argmax(predicted, 1)
        print(actual, predicted)
        return 100 * np.sum(np.equal(predicted, actual)) / predicted.shape[0]

    def train(self, flat_rawprofile, cluster_pairs):
        """Train the model"""
        self.flat_rawprofile = flat_rawprofile
        self.cluster_pairs = cluster_pairs
        _positive_df = self._get_positive_examples()
        # print(_positive_df)
        _negative_df = self._get_negative_examples()
        # print(_negative_df)
        data = self._concat_examples(_positive_df, _negative_df)

        # Change Class column into target_0 ([1 0] for No Match data) and target_1 ([0 1] for Match data)
        one_hot_data = pd.get_dummies(data, prefix=["target"], columns=["target"])

        # split
        df_X = one_hot_data.drop(["target_0", "target_1", "rid", "lid"], axis=1)
        df_y = one_hot_data[["target_0", "target_1"]]

        print(np.stack(df_X["feature"].values).shape)

        # Convert both data_frames into np arrays of float32
        ar_y = np.asarray(df_y.values, dtype="float32")
        ar_X = np.stack(df_X["feature"].values).astype(dtype="float32")

        # Allocate first 80% of data into training data and remaining 20% into testing data

        # ----------------------------------------------------------------------
        # train_size = int(0.8 * len(ar_X))
        # (raw_X_train, raw_y_train) = (ar_X[:train_size], ar_y[:train_size])
        # (raw_X_test, raw_y_test) = (ar_X[train_size:], ar_y[train_size:])
        # ----------------------------------------------------------------------
        split = StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
        for train_index, validation_index in split.split(ar_X, ar_y):
            raw_X_train, raw_X_test = ar_X[train_index], ar_X[validation_index]
            raw_y_train, raw_y_test = ar_y[train_index], ar_y[validation_index]
        # ------------------------------------------------------------------------

        # Gets a percent of match vs no match (6% of data are match?)
        count_match, count_no_match = np.unique(data["target"], return_counts=True)[1]
        match_ratio = float(count_match / (count_match + count_no_match))
        print("Percent of match ratios: ", match_ratio)

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
        X_train_node = tf.compat.v1.placeholder(
            tf.float32, [None, input_dimensions], name="X_train"
        )
        y_train_node = tf.compat.v1.placeholder(
            tf.float32, [None, output_dimensions], name="y_train"
        )

        # We will use these as inputs to the model once it comes time to test it
        X_test_node = tf.constant(raw_X_test, name="X_test")
        y_test_node = tf.constant(raw_y_test, name="y_test")

        # First layer takes in input and passes output to 2nd layer
        self.weight_1_node = tf.Variable(
            tf.zeros([input_dimensions, num_layer_1_cells]), name="weight_1"
        )
        self.biases_1_node = tf.Variable(tf.zeros([num_layer_1_cells]), name="biases_1")

        # Second layer takes in input from 1st layer and passes output to 3rd layer
        self.weight_2_node = tf.Variable(
            tf.zeros([num_layer_1_cells, num_layer_2_cells]), name="weight_2"
        )
        self.biases_2_node = tf.Variable(tf.zeros([num_layer_2_cells]), name="biases_2")

        # Third layer takes in input from 2nd layer and outputs [1 0] or [0 1] depending on match vs non match
        self.weight_3_node = tf.Variable(
            tf.zeros([num_layer_2_cells, output_dimensions]), name="weight_3"
        )
        self.biases_3_node = tf.Variable(tf.zeros([output_dimensions]), name="biases_3")

        num_epochs = 100

        # Used to predict what results will be given training or testing input data
        # Remember, X_train_node is just a placeholder for now. We will enter values at run time

        y_train_prediction = self._network(X_train_node)
        y_test_prediction = self._network(X_test_node)

        # Cross entropy loss function measures differences between actual output and predicted output
        cross_entropy = tf.compat.v1.losses.softmax_cross_entropy(
            y_train_node, y_train_prediction
        )

        # Adam optimizer function will try to minimize loss (cross_entropy) but changing the 3 layers' variable values at a
        #   learning rate of 0.005
        optimizer = tf.compat.v1.train.AdamOptimizer(0.005).minimize(cross_entropy)
        saver = tf.train.Saver()
        with tf.compat.v1.Session() as session:
            tf.compat.v1.global_variables_initializer().run()
            for epoch in range(num_epochs):

                start_time = time.time()

                operation_ = [optimizer, cross_entropy]
                _, cross_entropy_score = session.run(
                    operation_,
                    feed_dict={X_train_node: raw_X_train, y_train_node: raw_y_train},
                )

                if epoch % 10 == 0:
                    timer = time.time() - start_time

                    print(
                        "Epoch: {}".format(epoch),
                        "Current loss: {0:.4f}".format(cross_entropy_score),
                        "Elapsed time: {0:.2f} seconds".format(timer),
                    )

                    final_y_test = y_test_node.eval()
                    final_y_test_prediction = y_test_prediction.eval()
                    final_accuracy = self._calculate_accuracy(
                        final_y_test, final_y_test_prediction
                    )
                    print("Current accuracy: {0:.2f}%".format(final_accuracy))

            final_y_test = y_test_node.eval()
            final_y_test_prediction = y_test_prediction.eval()
            final_accuracy = self._calculate_accuracy(
                final_y_test, final_y_test_prediction
            )
            print("Final accuracy: {0:.2f}%".format(final_accuracy))
            self.save_model({"saver": saver, "session": session})
            # saver.save(session, "my_test_model")
        final_match_y_test = final_y_test[final_y_test[:, 1] == 1]
        final_match_y_test_prediction = final_y_test_prediction[final_y_test[:, 1] == 1]
        final_match_accuracy = self._calculate_accuracy(
            final_match_y_test, final_match_y_test_prediction
        )
        print("Final match specific accuracy: {0:.2f}%".format(final_match_accuracy))

    def predict(self, flat_rawprofile, cluster_pairs):
        """Predict using the previously trained model"""
        self.flat_rawprofile = flat_rawprofile
        self.cluster_pairs = cluster_pairs
        model_name = self.model_name + ".meta"

        _positive_df = self._get_positive_examples()
        _negative_df = self._get_negative_examples()
        data = self._concat_examples(_positive_df, _negative_df)

        # Change Class column into target_0 ([1 0] for No Match data) and target_1 ([0 1] for Match data)
        one_hot_data = pd.get_dummies(data, prefix=["target"], columns=["target"])

        # split
        df_X = one_hot_data.drop(["target_0", "target_1", "rid", "lid"], axis=1)
        df_y = one_hot_data[["target_0", "target_1"]]

        # print(np.stack(df_X["feature"].values).shape)

        # Convert both data_frames into np arrays of float32
        ar_y = np.asarray(df_y.values, dtype="float32")
        ar_X = np.stack(df_X["feature"].values).astype(dtype="float32")

        # Gets a percent of match vs no match (6% of data are match?)
        count_match, count_no_match = np.unique(data["target"], return_counts=True)[1]
        match_ratio = float(count_match / (count_match + count_no_match))
        print("Percent of match ratios: ", match_ratio)

        # Applies a logit weighting to match profiles to cause model to pay more attention to them
        weighting = 1 / match_ratio
        ar_y[:, 1] = ar_y[:, 1] * weighting

        final_y_test_prediction = None
        # load model
        with self.load_model(model_name) as session:
            # Access the graph
            graph = tf.get_default_graph()
            # Now, let's access and create placeholders variables and
            # create feed-dict to feed new data

            # X_train_node = graph.get_tensor_by_name("X_train_node:0")
            # y_train_node = graph.get_tensor_by_name("y_train_node:0")
            # feed_dict = {X_train_node: ar_X, y_train_node: ar_y}
            # feed_dict={X_train_node: raw_X_train, y_train_node: raw_y_train}

            self.weight_1_node = graph.get_tensor_by_name("weight_1:0")
            self.biases_1_node = graph.get_tensor_by_name("biases_1:0")
            self.weight_2_node = graph.get_tensor_by_name("weight_2:0")
            self.biases_2_node = graph.get_tensor_by_name("biases_2:0")
            self.weight_3_node = graph.get_tensor_by_name("weight_3:0")
            self.biases_3_node = graph.get_tensor_by_name("biases_3:0")

            y_test_prediction = self._network(ar_X)

            # # Now, access the op that you want to run.
            # operation_ = graph.get_tensor_by_name("operation_:0")

            # session.run(operation_, feed_dict=feed_dict)
            final_y_test_prediction = y_test_prediction.eval(session=session)
        return final_y_test_prediction

    def save_model(self, model_config: dict):
        """This will save following files in Tensorflow
        classification_deep_learing_model.data-00000-of-00001
        classification_deep_learing_model.index
        classification_deep_learing_model.meta
        checkpoint
        """
        model_saver = model_config.get("saver")
        session = model_config.get("session")
        model_saver.save(
            session,
            self.model_path + self.model_name,
        )

    def load_model(self, model_name):
        """This will load the model from saved model meta file

        :return: tensorflow session with restored model"""
        session = tf.Session()
        saver = tf.train.import_meta_graph(self.model_path + model_name)
        saver.restore(
            session,
            tf.train.latest_checkpoint(self.model_path),
        )
        return session

if __name__ == "__main__":
    cluster = Blocking()
    flat_rawprofile = cluster.prepare_data(n_docs=config.minlsh_n_docs)
    cluster_pairs = cluster.do_blocking(docs=flat_rawprofile)
    dl = DeepLearning()
    # fitted = dl.fit(train.flat_rawprofile, train.cluster_pairs)
    # output = dl.network(fitted)
    dl.train(flat_rawprofile, cluster_pairs)
    output = dl.predict(flat_rawprofile, cluster_pairs)
    print(output)
