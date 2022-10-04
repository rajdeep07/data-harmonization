import pandas as pd
import tensorflow as tf

from data_harmonization.main.code.tiger.Blocking import Blocking
from data_harmonization.main.code.tiger.classification.deep_learning import DeepLearning
import data_harmonization.main.resources.config as config

tf.compat.v1.disable_v2_behavior()


class Train:
    """Classification"""

    def __init__(self):
        self.flat_rawprofile = None
        self.cluster_pairs = None
        self._positive_df = pd.DataFrame()
        self._negative_df = pd.DataFrame()

    def get_data(self):
        """Block datasets and create clusters

        :return: class object with prepared input data and clusters
        """
        cluster = Blocking()
        self.flat_rawprofile = cluster.prepare_data(n_docs=config.minlsh_n_docs)
        self.cluster_pairs = cluster.do_blocking(docs=self.flat_rawprofile)
        return self

    # def _get_data_from_db(self, app_name, table_name):
    #     app_name = "data_harmonization"
    #     table_name = "Raw_Entity"
    #     mysql = MySQL(
    #         config.mysqlLocalHost, app_name, config.mysqlUser, config.mysqlPassword
    #     )

    #     mysql.mycursor.execute(f"SELECT * FROM {table_name};")
    #     result_set = mysql.get_result()
    #     return result_set

    # def get_data(self):
    #     app_name = "data_harmonization"
    #     table_name = "Raw_Entity"
    #     # format flat_rawprofile
    #     result_set = self._get_data_from_db(app_name, table_name)
    #     id = 0
    #     self.flat_rawprofile = {}
    #     for row in result_set:
    #         self.flat_rawprofile[str(id)] = row
    #         id = id + 1

    #     # get cluster pairs
    #     table_name = "Cluster_pairs"
    #     self.cluster_pairs = self._get_data_from_db(app_name, table_name)

    #     return self

    def train(self, flat_rawprofile=None, cluster_pairs=None):
        """It will call the train method of specific classification module"""
        if not flat_rawprofile:
            flat_rawprofile = self.flat_rawprofile
        if not cluster_pairs:
            cluster_pairs = self.cluster_pairs

        classification_obj = DeepLearning()
        return classification_obj.train(self, flat_rawprofile, cluster_pairs)

    def predict(self, flat_rawprofile=None, cluster_pairs=None):
        """It will call the predict method of specific classification module to predict output"""
        if not flat_rawprofile:
            flat_rawprofile = self.flat_rawprofile
        if not cluster_pairs:
            cluster_pairs = self.cluster_pairs

        classification_obj = DeepLearning()
        return classification_obj.predict(self, flat_rawprofile, cluster_pairs)


if __name__ == "__main__":
    dta = Train().get_data()
    train = Train().get_data()
    dl = DeepLearning()
    # fitted = dl.fit(train.flat_rawprofile, train.cluster_pairs)
    # output = dl.network(fitted)
    dl.train(train.flat_rawprofile, train.cluster_pairs)
    output = dl.predict(train.flat_rawprofile, train.cluster_pairs)
    print(output)

    """ train = Train().get_data()
    print("Training dataset", train.cluster_pairs)

    _positive_df = train._get_positive_examples()
    # print(_positive_df)
    _negative_df = train._get_negative_examples()
    # print(_negative_df)
    data = train._concat_examples(_positive_df, _negative_df)

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
    weight_1_node = tf.Variable(
        tf.zeros([input_dimensions, num_layer_1_cells]), name="weight_1"
    )
    biases_1_node = tf.Variable(tf.zeros([num_layer_1_cells]), name="biases_1")

    # Second layer takes in input from 1st layer and passes output to 3rd layer
    weight_2_node = tf.Variable(
        tf.zeros([num_layer_1_cells, num_layer_2_cells]), name="weight_2"
    )
    biases_2_node = tf.Variable(tf.zeros([num_layer_2_cells]), name="biases_2")

    # Third layer takes in input from 2nd layer and outputs [1 0] or [0 1] depending on match vs non match
    weight_3_node = tf.Variable(
        tf.zeros([num_layer_2_cells, output_dimensions]), name="weight_3"
    )
    biases_3_node = tf.Variable(tf.zeros([output_dimensions]), name="biases_3")

    num_epochs = 100

    # Used to predict what results will be given training or testing input data
    # Remember, X_train_node is just a placeholder for now. We will enter values at run time
    y_train_prediction = train.predict(X_train_node)
    y_test_prediction = train.predict(X_test_node)

    # Cross entropy loss function measures differences between actual output and predicted output
    cross_entropy = tf.compat.v1.losses.softmax_cross_entropy(
        y_train_node, y_train_prediction
    )

    # Adam optimizer function will try to minimize loss (cross_entropy) but changing the 3 layers' variable values at a
    #   learning rate of 0.005
    optimizer = tf.compat.v1.train.AdamOptimizer(0.005).minimize(cross_entropy)

    with tf.compat.v1.Session() as session:
        tf.compat.v1.global_variables_initializer().run()
        for epoch in range(num_epochs):

            start_time = time.time()

            _, cross_entropy_score = session.run(
                [optimizer, cross_entropy],
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
                final_accuracy = train.calculate_accuracy(
                    final_y_test, final_y_test_prediction
                )
                print("Current accuracy: {0:.2f}%".format(final_accuracy))

        final_y_test = y_test_node.eval()
        final_y_test_prediction = y_test_prediction.eval()
        final_accuracy = train.calculate_accuracy(final_y_test, final_y_test_prediction)
        print("Final accuracy: {0:.2f}%".format(final_accuracy))

    final_match_y_test = final_y_test[final_y_test[:, 1] == 1]
    final_match_y_test_prediction = final_y_test_prediction[final_y_test[:, 1] == 1]
    final_match_accuracy = train.calculate_accuracy(
        final_match_y_test, final_match_y_test_prediction
    )
    print("Final match specific accuracy: {0:.2f}%".format(final_match_accuracy))
 """
