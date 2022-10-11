import os
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from data_harmonization.main.code.tiger.spark.SparkClass import SparkClass
from data_harmonization.main.code.tiger.model.ingester.Rawentity import Rawentity
from typing import Optional
from pyspark.ml.feature import Word2Vec, Word2VecModel, CountVectorizer, HashingTF, IDF, \
    Tokenizer, StopWordsRemover,RegexTokenizer, CountVectorizerModel
from pyspark.ml.feature import BucketedRandomProjectionLSH, MinHashLSH
from pyspark.sql.functions import col



def Blocking(is_train=True, if_word_2vec=False, if_min_lsh=True):
    # initializing spark
    spark = SparkClass().get_sparkSession()

    # Read from MySQL + RawEntity
    df = spark.read_from_database_to_dataframe("Rawentity")

    def createShingles(input: Optional[str]) -> Optional[list[str]]:
        def shingle(x: str) -> list[str]:
            i = x.lower()
            if len(i) >= 5:
                range(0, len(i) - 5 + 1).map(lambda j: i.substring(j, j + 5))
            else:
                list(i)

        input.map(lambda i: i.split("[-\\s,]").filter(lambda s: not s.isEmpty).flatMap(shingle).list())

    def createTokens(profile: Rawentity, features_to_deduplicate=None):

        if features_to_deduplicate:
            output = []
            for feature in features_to_deduplicate:
                output.append(createShingles(feature))
        else:
            # Map this operations to all attribute of RawEntity class
            output = createShingles(profile.Name) + createShingles(profile.Address) + createShingles(profile.City)
            output.flatten.filter(lambda e: not e.isNull() and not e.isEmpty).mkString(" ")

    # cleanse the dataframe <id, features [shingles]>
    cleansed_df = df.rdd.map(lambda r: createTokens(r)).filter(lambda p: not p.isEmpty).toDF(['id', 'shingles'])

    # Space Tokenizer
    tokenizer = Tokenizer(inputCol='shingles', outputCol='tokens')
    tokensDF = tokenizer.transform(cleansed_df).select("id", "tokens")

    # Regex Tokenizer
    regexTokenizer = RegexTokenizer(inputCol="tokens", outputCol="reg_tokens", pattern="\\W", toLowercase=True)
    regexTokensDF = regexTokenizer.transform(tokensDF).select("id", "tokens")

    # remove stop words
    remover = StopWordsRemover(inputCol="reg_tokens", outputCol="clean_tokens")
    cleansedTokensDF = remover.transform(regexTokensDF).select("id", "clean_tokens")

    if if_word_2vec:
        if is_train:
            # word2Vec
            w2v_model = Word2Vec(vectorSize=10000, inputCol='clean_tokens', outputCol='vector', minCount=3)
            model = w2v_model.fit(cleansedTokensDF)
            model.write().overwrite().save("/data_harmonization/main/model/model.word2vec")
            # model = Word2VecModel.load("/data_harmonization/main/model/model.word2vec")
            resultsDF = model.transform(cleansedTokensDF).select("id", "vector")
    else:
        if is_train:
            cv = CountVectorizer(inputCol="clean_tokens", outputCol="vector", vocabSize=200 * 10000, minDF=1.0)
            cv_model = cv.fit(cleansedTokensDF)
            cv_model.write().overwrite().save("/data_harmonization/main/model/model.countvec")
            # cv_model = CountVectorizerModel.load("models/CV.model")
            resultsDF = cv_model.transform(cleansedTokensDF).select("id", "vector")

    if if_min_lsh:
        # 1.MinHashLSH
        brp = MinHashLSH(inputCol='vector', outputCol='hashes', numHashTables=4.0)
        model = brp.fit(resultsDF)
    else:
        # 2.BucketedRandomProjectionLSH
        brp = BucketedRandomProjectionLSH(inputCol='vector', outputCol='hashes', numHashTables=4.0,
                                      bucketLength=10.0)
        model = brp.fit(resultsDF)

    # approx threshold score
    threshold = 0.80

    similarDF = model.approxSimilarityJoin(resultsDF, resultsDF, threshold, distCol='JaccardDistance').filter("distCol != 0")

    finalResultsDF = similarDF.withColumnRenamed(col("datasetA.id"), "id1")\
        .withColumnRenamed(col("datasetB.id"), "id2").select("id1", "id2", "distcol").sort(col("distcol").desc())

    spark.write_to_database_from_df(db="data_harmonization", table="semi_merged", df=finalResultsDF,
                                         mode='overwrite')

if __name__ == "__main__":
    # word2vec + minLSH
    Blocking(is_train=True, if_word_2vec=True, if_min_lsh=True)
    # CountVectorizer + minLSH
    Blocking(is_train=True, if_word_2vec=False, if_min_lsh=True)
