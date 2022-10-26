from typing import Any, Optional

import numpy as np
import pandas as pd
import tensorflow as tf
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from data_harmonization.main.code.tiger.Features import Features
from data_harmonization.main.code.tiger.spark import SparkClass
from data_harmonization.main.resources import config as config_

tf.compat.v1.disable_v2_behavior()
tf.compat.v1.disable_eager_execution()


class Classifier:
    def __init__(self) -> None:
        self.spark = SparkClass()
        self.sparksession = self.spark.get_sparkSession()
        self.rawentity_df = self.spark.read_from_database_to_dataframe("rawentity")
        self.rawentity_df_can = self.rawentity_df.rdd.toDF(
            ["canonical_" + col for col in self.rawentity_df.columns]
        )

    def _feature_data(
        self, features: pd.DataFrame, target: Optional[pd.DataFrame] = pd.DataFrame([])
    ) -> pd.DataFrame:
        feature_df = features.copy()
        feature_df["features"] = feature_df.apply(lambda x: Features().get(x), axis=1)
        if target.empty:
            return feature_df
        target_df = pd.get_dummies(
            target, prefix="target", columns=["target"], drop_first=False
        )
        return pd.concat([feature_df, target_df], axis=1)

    def create_df_from_id_pairs(self, id_pair: DataFrame) -> pd.DataFrame:
        full_df = (
            id_pair.alias("a").join(
                self.rawentity_df.alias("b"), (col("a.id") == col("b.id")), "inner"
            )
        ).drop(col("b.id"))
        full_df = (
            full_df.alias("a").join(
                self.rawentity_df_can.alias("b"),
                (col("a.canonical_id") == col("b.canonical_id")),
                "inner",
            )
        ).drop(col("b.canonical_id"))

        return full_df.toPandas()

    def _network(self, input_tensor) -> tf.Tensor:
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

    def _calculate_accuracy(self, actual, predicted) -> list[tf.Tensor]:
        TP = tf.math.count_nonzero(predicted * actual)
        TN = tf.math.count_nonzero((predicted - 1) * (actual - 1))
        FP = tf.math.count_nonzero(predicted * (actual - 1))
        FN = tf.math.count_nonzero((predicted - 1) * actual)
        precision = TP / (TP + FP)
        recall = TP / (TP + FN)
        f1 = 2 * precision * recall / (precision + recall)
        # print(classification_report(actual, predicted))
        return precision, recall, f1

    def predict(self, table_name: str = config_.blocking_table) -> None:
        session = self.load_model(
            model_path="data_harmonization/main/code/tiger/classification/models/",
            model_name="classification_deep_learing_model.meta",
        )

        semi_merged_data = self.spark.read_from_database_to_dataframe(table=table_name)
        data = self.create_df_from_id_pairs(id_pair=semi_merged_data)
        processed_data = self._feature_data(data)

        data_X = processed_data["features"].values
        data_X = np.stack(data_X, axis=0).astype(dtype="float32")

        # Access the graph
        # graph = tf.compat.v1.get_default_graph()
        # Now, let's access and create placeholders variables and
        # create feed-dict to feed new data

        a = tf.compat.v1.trainable_variables()
        var_dict = {}
        print("Trainable Params:")
        for var in a:
            var_dict[var.name] = var
            print(var.name)

        self.weight_1_node = session.run(var_dict.get("weight_1:0"))
        self.biases_1_node = session.run(var_dict.get("biases_1:0"))
        self.weight_2_node = session.run(var_dict.get("weight_2:0"))
        self.biases_2_node = session.run(var_dict.get("biases_2:0"))
        self.weight_3_node = session.run(var_dict.get("weight_3:0"))
        self.biases_3_node = session.run(var_dict.get("biases_3:0"))

        predict_node = tf.constant(data_X, name="data_X")

        prediction = self._network(predict_node)

        predicted = prediction.eval(session=session)
        session.close()
        predicted = np.argmax(predicted, axis=1).astype("int32")
        semi_merged_data_pd = semi_merged_data.toPandas()
        semi_merged_data_pd["isMatch"] = predicted
        semi_merged_data_pd = semi_merged_data_pd.drop(
            columns="JaccardDistance", axis=1
        )
        semi_merged_data_pd = semi_merged_data_pd.rename(
            columns={"id": "leftId", "canonical_id": "rightId"}
        )
        semi_merged_data = self.sparksession.createDataFrame(semi_merged_data_pd)
        self.spark.write_to_database_from_df(
            config_.classification_table, df=semi_merged_data, mode="overwrite"
        )

    def load_model(self, model_name: str, model_path: str) -> tf.compat.v1.Session:
        """This will load the model from saved model meta file

        :return: tensorflow session with restored model"""
        session = tf.compat.v1.Session()
        init = tf.compat.v1.global_variables_initializer()
        session.run(init)
        saver = tf.compat.v1.train.import_meta_graph(model_path + model_name)
        saver.restore(
            session,
            tf.train.latest_checkpoint(model_path),
        )
        return session


if __name__ == "__main__":
    Classifier().predict()
