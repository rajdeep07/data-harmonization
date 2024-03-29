import numpy as np
import pandas as pd
import tensorflow as tf
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from data_harmonization.main.code.tiger.Features import Features
from data_harmonization.main.code.tiger.spark import SparkClass
from data_harmonization.main.resources import config as config_
from data_harmonization.main.resources.log4j import Logger

tf.compat.v1.disable_v2_behavior()
tf.compat.v1.disable_eager_execution()


class Synthesis:
    """Classification using Deep Learning.
    Determines whether 2 profile are match or not"""

    def __init__(self) -> None:
        """Setting up initial variables"""
        self.spark = SparkClass()
        self.sparksession = self.spark.get_sparkSession()
        self.rawentity_df = self.spark.read_from_database_to_dataframe(
            config_.raw_entity_table
        )
        self.rawentity_df_can = self.rawentity_df.rdd.toDF(
            ["canonical_" + col for col in self.rawentity_df.columns]
        )
        self.weight_1_node = None
        self.biases_1_node = None
        self.weight_2_node = None
        self.biases_2_node = None
        self.weight_3_node = None
        self.biases_3_node = None
        self.logger = Logger(name="synthesis")

    def _feature_data(
        self,
        features: pd.DataFrame,
        target: pd.DataFrame = pd.DataFrame([]),
    ) -> pd.DataFrame:
        """
        Feature extracts text data into numerical by using `Features` module

        Parameters
        -----------
        features
                  DataFrame for which features to be extracted
        target
                target dataframe features to be extracted

        Returns
        -------
        pd.DataFrame
            Feature extracted dataframe
        """
        self.logger.log(level="INFO", msg="Processing data")
        feature_df = features.copy()
        feature_df["features"] = feature_df.apply(
            lambda x: Features().get(x), axis=1
        )
        if target.empty:
            return feature_df
        target_df = pd.get_dummies(target, prefix="target", columns=["target"], drop_first=False)
        return pd.concat([feature_df, target_df], axis=1)

    def create_df_from_id_pairs(self, id_pair: DataFrame) -> pd.DataFrame:
        """Create a dataframe object from id pair dataframe

        Parameters
        ----------
        id_pair
            dataframe with id pairs. It should have id
            and canonical_id columns

        Returns
        -------
        pd.DataFrame
            returns dataframe containing all the
            rows and columns from left and right id pair
        """
        self.logger.log(
            level="INFO",
            msg="Fetching all atributes from raw entites matching with ids"
        )
        full_df = (
            id_pair.alias("a").join(
                self.rawentity_df.alias("b"),
                (col("a.id") == col("b.id")),
                "inner",
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

    def _network(self, input_tensor: tf.Tensor) -> tf.Tensor:
        """Function to run an input tensor through the 3 layers and output
        a tensor that will give us a match/non match result.
        Each layer uses a different function to fit lines through the data and
        predict whether a given input tensor will result in a match
        or non match profiles.

        Parameters
        ----------
        input_tensor: tf.Tensor
            input data for creating graph

        Returns
        -------
        tf.Tensor
            match or non match profile. It is the output of the last layer
        """
        self.logger.log(level="INFO", msg="Running all layers")
        # Sigmoid fits modified data well
        layer1 = tf.nn.sigmoid(tf.matmul(input_tensor, self.weight_1_node) + self.biases_1_node)
        # Dropout prevents model from becoming lazy and over confident
        layer2 = tf.nn.dropout(
            tf.nn.sigmoid(tf.matmul(
                layer1, self.weight_2_node) + self.biases_2_node
            ),
            0.85,
        )
        # Softmax works very well with one hot encoding
        # which is how results are outputted
        layer3 = tf.nn.softmax(
            tf.matmul(layer2, self.weight_3_node) + self.biases_3_node
        )
        return layer3

    def predict(self, table_name: str = config_.blocking_table) -> None:
        """Predict using the previously trained model.

        Parameters
        ----------
        table_name
            blocking table name
        """
        # load the model
        self.logger.log(
            level="INFO",
            msg="Loading the previously trained model"
        )
        session = self.load_model(
            model_path="data_harmonization/main/code"
            + "/tiger/classification/models/",
            model_name="classification_deep_learing_model.meta",
        )

        semi_merged_data = self.spark.read_from_database_to_dataframe(
            table=table_name
        )
        data = self.create_df_from_id_pairs(id_pair=semi_merged_data)
        processed_data = self._feature_data(data)

        data_X = processed_data["features"].values
        data_X = np.stack(data_X, axis=0).astype(dtype="float32")

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
        semi_merged_data_pd = semi_merged_data_pd.drop(columns="JaccardDistance", axis=1)
        semi_merged_data_pd = semi_merged_data_pd.rename(
            columns={"id": "leftId", "canonical_id": "rightId"}
        )
        semi_merged_data = self.sparksession.createDataFrame(
            semi_merged_data_pd
        )
        self.logger.log(
            level="INFO",
            msg=f"Writing all merged data in {config_.classification_table} "
            + "table in database"
        )
        self.spark.write_to_database_from_df(
            config_.classification_table, df=semi_merged_data, mode="overwrite"
        )

    def load_model(
        self, model_name: str, model_path: str
    ) -> tf.compat.v1.Session:
        """This will load the model from saved model meta file

        Parameters
        ----------
        model_name
            name of the saved model
        model_path
            relative path of the saved model

        Returns
        -------
        tf.compat.v1.Session
            tensorflow session with restored model
        """
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
    Synthesis().predict()
