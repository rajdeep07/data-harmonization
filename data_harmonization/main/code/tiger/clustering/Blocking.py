import argparse
import os
import re

from data_harmonization.main.code.tiger.spark import SparkClass
from data_harmonization.main.resources import config as config_
from data_harmonization.main.resources.log4j import Logger
from pyspark.sql import Row
from pyspark.ml.feature import (
    BucketedRandomProjectionLSH,
    CountVectorizer,
    CountVectorizerModel,
    MinHashLSH,
    RegexTokenizer,
    StopWordsRemover,
    Tokenizer,
    Word2Vec,
    Word2VecModel,
)
from pyspark.sql.functions import col, concat_ws


class Blocking:
    """create cluster pairs using minLSH alogorithm"""

    def __init__(self) -> None:
        """Setting up initial variables"""
        self.spark = SparkClass()
        self.logger = Logger(name="blocking")

    def _createShingles(self, input: str) -> list[str]:
        """Return shingles from string

        Parameters
        ----------
        input
            text from where shingles will be created

        Returns
        -------
        Optional[list[str]]
            list of shingles
        """

        def _shingle(x: str) -> list[str]:
            """Create shingles from string

            Parameters
            ----------
            x
                text from where shingles will be created

            Returns
            -------
            list[str]
                created shingles
            """
            inp = x.lower()
            shingle_size = 5
            if len(inp) >= shingle_size:
                return list(
                    map(
                        lambda i: inp[i : i + shingle_size],
                        range(0, len(inp) - shingle_size + 1),
                    )
                )
            else:
                return [inp]

        output = "".join(re.split("[-\\W,]", input))
        return _shingle(output)

    def _createTokens(self, profile: dict, features_to_deduplicate=None):

        output: list[str] = []
        if features_to_deduplicate:
            for feature in features_to_deduplicate:
                output.extend(self._createShingles(profile[feature]))
        else:
            # Map this operations to all attribute of RawEntity class
            for value in profile.values():
                if isinstance(value, str):
                    output.extend(self._createShingles(value))

        tokens = " ".join(
            map(
                lambda x: x.strip(),
                filter(lambda e: e and not e.isspace(), output),
            )
        )
        return Row(id=profile["id"], shingles=tokens)

    def do_blocking(
        self,
        is_train: bool = True,
        if_word_2_vec: bool = False,
        if_min_lsh: bool = True,
    ) -> None:
        """Block datasets

        Parameters
        -----------
        is_train
            whether to train the vector model
        if_word_2_vec
            whether to use word_2_vec model or count vector will be used
        if_min_lsh
            whether to use minLSH algorithm or
            Bucketed random projection LSH will be used
        """
        # Read from MySQL + RawEntity
        df = self.spark.read_from_database_to_dataframe(
            config_.raw_entity_table
        )
        # cleanse the dataframe <id, features [shingles]>
        cleansed_df = df.rdd.map(
            lambda r: self._createTokens(r.asDict())
        ).toDF(["id", "shingles"])
        cleansed_df.show()
        # Space Tokenizer
        tokenizer = Tokenizer(inputCol="shingles", outputCol="tokens")
        tokensDF = tokenizer.transform(cleansed_df).select("id", "tokens")
        tokensDF = tokensDF.withColumn("tokens", concat_ws(" ", col("tokens")))
        # Regex Tokenizer
        regexTokenizer = RegexTokenizer(
            inputCol="tokens",
            outputCol="reg_tokens",
            pattern="\\W",
            toLowercase=True,
        )
        regexTokensDF = regexTokenizer.transform(tokensDF).select(
            "id", "reg_tokens"
        )
        # remove stop words
        remover = StopWordsRemover(
            inputCol="reg_tokens", outputCol="clean_tokens"
        )
        cleansedTokensDF = remover.transform(regexTokensDF).select(
            "id", "clean_tokens"
        )

        if if_word_2_vec:
            if is_train:
                # word2Vec
                w2v_model = Word2Vec(
                    vectorSize=1000,
                    inputCol="clean_tokens",
                    outputCol="vector",
                    minCount=3,
                )
                model = w2v_model.fit(cleansedTokensDF)
                model.write().overwrite().save(
                    "/data_harmonization/main/model/model.word2vec"
                )
                resultsDF = model.transform(cleansedTokensDF).select(
                    "id", "vector"
                )
            else:
                model = Word2VecModel.load(
                    "/data_harmonization/main/model/model.word2vec"
                )
                resultsDF = model.transform(cleansedTokensDF).select(
                    "id", "vector"
                )
        else:
            if is_train:
                cv = CountVectorizer(
                    inputCol="clean_tokens",
                    outputCol="vector",
                    vocabSize=200 * 10000,
                    minDF=1.0,
                )
                cv_model = cv.fit(cleansedTokensDF)
                cv_model.write().overwrite().save(
                    os.path.abspath(__file__)
                    + "/../../../../../../data_harmonization/main/model"
                    + "/model.countvec"
                )
                resultsDF = cv_model.transform(cleansedTokensDF).select(
                    "id", "vector"
                )
            else:
                cv_model = CountVectorizerModel.load(
                    os.path.abspath(__file__)
                    + "/../../../../../../data_harmonization/main/model"
                    + "/model.countvec"
                )
                resultsDF = cv_model.transform(cleansedTokensDF).select(
                    "id", "vector"
                )

        if if_min_lsh:
            # 1.MinHashLSH
            brp = MinHashLSH(
                inputCol="vector", outputCol="hashes", numHashTables=4
            )
            model = brp.fit(resultsDF)
        else:
            # 2.BucketedRandomProjectionLSH
            brp = BucketedRandomProjectionLSH(
                inputCol="vector",
                outputCol="hashes",
                numHashTables=4,
                bucketLength=10.0,
            )
            model = brp.fit(resultsDF)

        model.transform(resultsDF).show()
        similarDF = (
            model.approxSimilarityJoin(
                resultsDF,
                resultsDF,
                config_.blocking_threshold,
                distCol="JaccardDistance",
            )
            .filter("JaccardDistance != 0")
            .select(
                col("datasetA.id").alias("id"),
                col("datasetB.id").alias("canonical_id"),
                col("JaccardDistance"),
            )
            .sort(col("JaccardDistance").desc())
        )
        self.spark.write_to_database_from_df(
            table=config_.blocking_table, df=similarDF, mode="overwrite"
        )


if __name__ == "__main__":
    # word2vec + minLSH
    # CountVectorizer + minLSH
    # Blocking(is_train=True, if_word_2_vec=False, if_min_lsh=True)
    parser = argparse.ArgumentParser(
        description="Blocking alogrithm creates a cluster pair"
    )
    parser.add_argument(
        "-t",
        "--train",
        help="train the model",
        default=True,
        action="store_true",
    )
    parser.add_argument(
        "-p",
        "--predict",
        help="Predict from the model",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--word2vec",
        "-w2v",
        action="store_true",
        default=False,
        help="Use Word2Vec algorithm, defaults to use CountVectorizer",
    )
    parser.add_argument(
        "--min-lsh",
        "-lsh",
        action="store_true",
        default=True,
        help="Use MinHashLSH algorithm, defaults "
        + "to use BucketedRandomProjectionLSH",
    )
    arg = parser.parse_args()

    Blocking().do_blocking(
        is_train=not (arg.train and arg.predict),
        if_word_2_vec=arg.word2vec,
        if_min_lsh=arg.min_lsh,
    )
