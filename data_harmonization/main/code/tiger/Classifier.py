import random
from typing import Any, Optional
import tensorflow as tf
import numpy as np
import pandas as pd
import time

from sklearn.model_selection import StratifiedShuffleSplit
from data_harmonization.main.code.tiger.Features import Features
from data_harmonization.main.code.tiger.spark import SparkClass
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from data_harmonization.main.resources import config

tf.compat.v1.disable_v2_behavior()


class Classifier():
    def __init__(self) -> None:
        self.spark = SparkClass()
        self.sparksession = self.spark.get_sparkSession()
        self.rawentity_df = self.spark.read_from_database_to_dataframe(
            'rawentity')
        self.rawentity_df_can = self.rawentity_df.rdd.toDF(
            ["canonical_"+col for col in self.rawentity_df.columns])
        self.input_dim: int
        self.output_dim: int

    def _feature_data(self, features: pd.DataFrame, target: Optional[pd.DataFrame] = pd.DataFrame([])) -> pd.DataFrame:
        feature_df = features.copy()
        feature_df['features'] = feature_df.apply(
            lambda x: Features().get(x), axis=1)
        if target.empty:
            return feature_df
        target_df = pd.get_dummies(target, prefix='target', columns=[
            'target'], drop_first=False)
        return pd.concat([feature_df, target_df], axis=1)

    def create_df_from_id_pairs(self, id_pair: DataFrame) -> pd.DataFrame:
        full_df = (id_pair.alias("a")
                   .join(
                   self.rawentity_df.alias("b"),
                   (col("a.id") == col("b.id")),
                   "inner"
                   )).drop(col("b.id"))
        full_df = (full_df.alias("a")
                   .join(
            self.rawentity_df_can.alias("b"),
                  (col("a.canonical_id") == col("b.canonical_id")),
            "inner"
        )).drop(col("b.canonical_id"))

        return full_df.toPandas()

    def _extract_postive_data(self, data: DataFrame, threshold: float = 0.80) -> pd.DataFrame:
        positive_df_id = data.filter(
            f"confidence > {threshold}").select("id", "canonical_id")
        positive_df = self.create_df_from_id_pairs(id_pair=positive_df_id)
        positive_df['target'] = pd.Series(
            np.ones(positive_df.shape[0])).astype('int')
        return positive_df

    def _extract_negative_data(self, data: DataFrame, match_ratio: float = 0.8) -> pd.DataFrame:
        rawentity_df = self.rawentity_df.sample(match_ratio, seed=42)
        all_id = rawentity_df.select(
            col("id")).rdd.flatMap(lambda x: x).collect()
        master_set = set()
        while len(master_set) < (data.count()*match_ratio):
            clus_ida, clus_idb = random.sample(all_id, 2)
            is_match = data.filter((data.id == clus_ida) & (data.canonical_id == clus_idb)).collect(
            ) and data.filter((data.id == clus_idb) & (data.canonical_id == clus_ida)).collect()
            if is_match:
                continue
            master_set.add((clus_ida, clus_idb))
            master_set.add((clus_idb, clus_ida))

        id_df = self.sparksession.createDataFrame(
            list(master_set), ["id", "canonical_id"])  # [(ida, idb), (idc, idd)]
        negative_df = self.create_df_from_id_pairs(id_pair=id_df)
        negative_df['target'] = pd.Series(
            np.zeros(negative_df.shape[0])).astype('int')
        return negative_df

    def _preprocess_data(self, data: DataFrame) -> pd.DataFrame:
        print("Started preprocessing data......")
        print("extracting positive data......")
        positive_df = self._extract_postive_data(data)
        print("Positive data shape:", positive_df.shape)
        print("extracting negative data......")
        negative_df = self._extract_negative_data(data)
        print("Negative data shape:", negative_df.shape)
        print("done extracting data......")
        data = pd.concat([positive_df, negative_df])
        print(
            f"Extracting features from the data.......\nTotal Rows : {data.shape[0]}")
        data = self._feature_data(data.drop('target', axis=1), data['target'])
        print("Done preprocessing data......")
        return data

    def _train_test_split(self, feature: np.array, target: np.array, n_splits: int = 2, test_size: float = 0.2,
                          random_state: int = 42) -> np.array:

        split = StratifiedShuffleSplit(
            n_splits=n_splits, test_size=test_size, random_state=random_state)
        for train_index, validation_index in split.split(feature, target):
            raw_X_train, raw_X_test = feature[train_index], feature[validation_index]
            raw_y_train, raw_y_test = target[train_index], target[validation_index]

        return raw_X_train, raw_X_test, raw_y_train, raw_y_test

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
            tf.nn.sigmoid(
                tf.matmul(layer1, self.weight_2_node) + self.biases_2_node),
            0.85,
        )
        # Softmax works very well with one hot encoding which is how results are outputted
        layer3 = tf.nn.softmax(
            tf.matmul(layer2, self.weight_3_node) + self.biases_3_node
        )
        return layer3

    def _calculate_accuracy(self, actual, predicted):
        TP = tf.math.count_nonzero(predicted * actual)
        TN = tf.math.count_nonzero((predicted - 1) * (actual - 1))
        FP = tf.math.count_nonzero(predicted * (actual - 1))
        FN = tf.math.count_nonzero((predicted - 1) * actual)
        precision = TP / (TP + FP)
        recall = TP / (TP + FN)
        f1 = 2 * precision * recall / (precision + recall)
        # print(classification_report(actual, predicted))
        return precision, recall, f1

    def _initialize_model(self) -> None:
        # First layer takes in input and passes output to 2nd layer
        self.weight_1_node = tf.Variable(
            tf.zeros([self.input_dim, self.num_layer_1_cells]), name="weight_1"
        )
        self.biases_1_node = tf.Variable(
            tf.zeros([self.num_layer_1_cells]), name="biases_1")

        # Second layer takes in input from 1st layer and passes output to 3rd layer
        self.weight_2_node = tf.Variable(
            tf.zeros([self.num_layer_1_cells, self.num_layer_2_cells]), name="weight_2"
        )
        self.biases_2_node = tf.Variable(
            tf.zeros([self.num_layer_2_cells]), name="biases_2")

        # Third layer takes in input from 2nd layer and outputs [1 0] or [0 1] depending on match vs non match
        self.weight_3_node = tf.Variable(
            tf.zeros([self.num_layer_2_cells, self.output_dim]), name="weight_3"
        )
        self.biases_3_node = tf.Variable(
            tf.zeros([self.output_dim]), name="biases_3")

    def train(self, table_name: str, num_epochs: int = 500):
        data_df = self.spark.read_from_database_to_dataframe(table=table_name)
        final_df = self._preprocess_data(data_df)
        raw_X_train, raw_X_test, raw_y_train, raw_y_test = self._train_test_split(
            final_df['features'].values,
            final_df[['target_0', 'target_1']].values)

        # stacking data
        raw_X_train = np.stack(raw_X_train, axis=0).astype(dtype='float32')
        raw_X_test = np.stack(raw_X_test, axis=0).astype(dtype='float32')
        print("Input shape:", raw_X_train.shape,
              "Output shape", raw_y_train.shape)

        # Gets a percent of match vs no match (6% of data are match?)
        count_match, count_no_match = final_df[['target_1']].value_counts()
        match_ratio = float(count_match / (count_match + count_no_match))
        print("Percent of match ratios: ", match_ratio)

        # Applies a logit weighting to match profiles to cause model to pay more attention to them
        weighting = 1 / match_ratio
        raw_y_train[:, 1] = raw_y_train[:, 1] * weighting

        # 30 cells for the input
        self.input_dim = input_dimensions = raw_X_train.shape[1]
        # 2 cells for the output
        self.output_dim = output_dimensions = raw_y_train.shape[1]
        # 100 cells for the 1st layer
        self.num_layer_1_cells = 100
        # 150 cells for the second layer
        self.num_layer_2_cells = 150

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

        self._initialize_model()

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
        optimizer = tf.compat.v1.train.AdamOptimizer(
            0.005).minimize(cross_entropy)
        saver = tf.compat.v1.train.Saver()

        with tf.compat.v1.Session() as session:
            tf.compat.v1.global_variables_initializer().run()
            for epoch in range(num_epochs):

                start_time = time.time()

                operation_ = [optimizer, cross_entropy]
                _, cross_entropy_score = session.run(
                    operation_,
                    feed_dict={X_train_node: raw_X_train,
                               y_train_node: raw_y_train},
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
                    precision, recall, f1 = self._calculate_accuracy(
                        final_y_test, final_y_test_prediction
                    )
                    print("Precision: {}\nrecall: {}\nf1: {}".format(precision.eval(session=session),
                                                                     recall.eval(
                        session=session),
                        f1.eval(
                        session=session)
                    ))

            final_y_test = y_test_node.eval()
            final_y_test_prediction = y_test_prediction.eval()
            precision, recall, f1 = self._calculate_accuracy(
                final_y_test, final_y_test_prediction
            )
            print("Precision: {}\nrecall: {}\nf1: {}".format(precision.eval(session=session),
                                                             recall.eval(
                session=session),
                f1.eval(
                session=session)
            ))
            self.save_model(model_config={"saver": saver, "session": session})
            # saver.save(session, "my_test_model")
        final_match_y_test = final_y_test[final_y_test[:, 1] == 1]
        final_match_y_test_prediction = final_y_test_prediction[final_y_test[:, 1] == 1]
        precision, recall, f1 = self._calculate_accuracy(
            final_match_y_test, final_match_y_test_prediction
        )
        print("Final Score:\nPrecision: {}\n recall: {}\n f1: {}".format(precision,
                                                                         recall,
                                                                         f1))

    def predict(self, table_name=config.blocking_table):
        session = self.load_model(model_path="data_harmonization/main/code/tiger/classification/models/",
                                  model_name="classification_deep_learing_model.meta")

        semi_merged_data = self.spark.read_from_database_to_dataframe(
            table=table_name)
        data = self.create_df_from_id_pairs(id_pair=semi_merged_data)
        processed_data = self._feature_data(data)

        data_X = processed_data['features'].values
        data_X = np.stack(data_X, axis=0).astype(dtype='float32')
        # raw_X_train = np.stack(raw_X_train, axis=0).astype(dtype='float32')

        with session as sess:
            # Access the graph
            graph = tf.compat.v1.get_default_graph()
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

            predict_node = tf.constant(data_X, name="data_X")

            prediction = self._network(predict_node)

            # # Now, access the op that you want to run.
            # operation_ = graph.get_tensor_by_name("operation_:0")

            # session.run(operation_, feed_dict=feed_dict)
            predicted = prediction.eval(session=sess)

        predicted = np.argmax(predicted, axis=1)
        semi_merged_data = semi_merged_data.withColumn("predicted", predicted)
        self.spark.write_to_database_from_df(config.classification_table)

    def save_model(self, model_config: dict,
                   model_path: str = "data_harmonization/main/code/tiger/classification/models/",
                   model_name: str = "classification_deep_learing_model"):
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
            model_path + model_name
        )

    def load_model(self, model_name: str, model_path: str) -> tf.compat.v1.Session:
        """This will load the model from saved model meta file

        :return: tensorflow session with restored model"""
        session = tf.compat.v1.Session()
        saver = tf.compat.v1.train.import_meta_graph(model_path + model_name)
        saver.restore(
            session,
            tf.train.latest_checkpoint(model_path),
        )
        return session


if __name__ == "__main__":
    # data = pd.read_csv('/home/navazdeens/data-harmonization/data_harmonization/main/data/benchmark.csv')

    Classifier().train(table_name='benchmark')
    # Classifier().predict()
