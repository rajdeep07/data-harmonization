import os
import re
from pyspark import SparkContext
from pyspark import Row
from pyspark.sql.types import *
from data_harmonization.main.code.tiger.spark import SparkClass
from data_harmonization.main.code.tiger.model.ingester.Rawentity import Rawentity
from typing import Optional
from pyspark.ml.feature import Word2Vec, Word2VecModel, CountVectorizer, HashingTF, IDF, \
    Tokenizer, StopWordsRemover,RegexTokenizer, CountVectorizerModel
from pyspark.ml.feature import BucketedRandomProjectionLSH, MinHashLSH
from pyspark.sql.functions import col, concat_ws



def Blocking(is_train=True, if_word_2vec=False, if_min_lsh=True):
    # initializing spark
    spark = SparkClass()
    sparksession = spark.get_sparkSession()

    # Read from MySQL + RawEntity
    df = spark.read_from_database_to_dataframe("rawentity")

    def createShingles(input: Optional[str]) -> Optional[list[str]]:
        def shingle(x: str) -> list[str]:
            inp = x.lower()
            shingle_size = 5
            if len(inp) >= shingle_size:
                # range(0, len(i) - 5 + 1).map(lambda j: i.substring(j, j + 5))
                return list(map(lambda i:inp[i:i+shingle_size], range(0, len(inp)-shingle_size+1)))
            else:
                return inp

        # input.map(lambda i: i.split("[-\\s,]").filter(lambda s: not s.isEmpty).flatMap(shingle).list())
        output = "".join(re.split("[-\\W,]", input))
        return shingle(output) 

    def createTokens(profile: dict, features_to_deduplicate=None):

        output : list[str] = []
        if features_to_deduplicate:
            for feature in features_to_deduplicate:
                output.extend(createShingles(profile[feature]))
        else:
            # Map this operations to all attribute of RawEntity class
            for value in profile.values():
                if isinstance(value, str):
                    output.extend(createShingles(value))
                
        # output.filter(lambda e: not e.isNull() and not e.isEmpty).mkString(" ")
        output = " ".join(map(lambda x:x.strip(), filter(lambda e: e and not e.isspace(), output)))
        return Row(id=profile['id'],shingles=output)

    # cleanse the dataframe <id, features [shingles]>
    cleansed_df = df.rdd.map(lambda r: createTokens(r.asDict())).toDF(['id', 'shingles'])
    cleansed_df.show()
    # Space Tokenizer
    tokenizer = Tokenizer(inputCol='shingles', outputCol='tokens')
    tokensDF = tokenizer.transform(cleansed_df).select("id", "tokens")
    tokensDF = tokensDF.withColumn('tokens', concat_ws(" ",col('tokens')))
    tokensDF.show()
    # Regex Tokenizer
    regexTokenizer = RegexTokenizer(inputCol="tokens", outputCol="reg_tokens", pattern="\\W", toLowercase=True)
    regexTokensDF = regexTokenizer.transform(tokensDF).select("id", "reg_tokens")
    regexTokensDF.show()
    # remove stop words
    remover = StopWordsRemover(inputCol="reg_tokens", outputCol="clean_tokens")
    cleansedTokensDF = remover.transform(regexTokensDF).select("id", "clean_tokens")

    cleansedTokensDF.show()
    if if_word_2vec:
        if is_train:
            # word2Vec
            w2v_model = Word2Vec(vectorSize=1000, inputCol='clean_tokens', outputCol='vector', minCount=3)
            model = w2v_model.fit(cleansedTokensDF)
            model.write().overwrite().save("/data_harmonization/main/model/model.word2vec")
            resultsDF = model.transform(cleansedTokensDF).select("id", "vector")
        else:
            model = Word2VecModel.load("/data_harmonization/main/model/model.word2vec")
            resultsDF = model.transform(cleansedTokensDF).select("id", "vector")
    else:
        if is_train:
            cv = CountVectorizer(inputCol="clean_tokens", outputCol="vector", vocabSize=200 * 10000, minDF=1.0)
            cv_model = cv.fit(cleansedTokensDF)
            cv_model.write().overwrite().save(os.path.abspath(__file__)+"/../../../../../../data_harmonization/main/model/model.countvec")
            resultsDF = cv_model.transform(cleansedTokensDF).select("id", "vector")
        else:
            cv_model = CountVectorizerModel.load(os.path.abspath(__file__)+"/../../../../../../data_harmonization/main/model/model.countvec")
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
    model.transform(resultsDF).show()
    similarDF = model.approxSimilarityJoin(resultsDF, resultsDF, threshold, distCol="JaccardDistance").filter("JaccardDistance != 0")\
        .select(col("datasetA.id").alias("idA"),
                col("datasetB.id").alias("idB"),
                col("JaccardDistance")).sort(col("JaccardDistance").desc())
    similarDF.show(n=20)
    # finalResultsDF = similarDF.withColumnRenamed(col("datasetA.id"), "id1")\
    #     .withColumnRenamed(col("datasetB.id"), "id2").select("id1", "id2", "distcol").sort(col("distcol").desc())

    spark.write_to_database_from_df(table="semi_merged", df=similarDF,
                                         mode='overwrite')

if __name__ == "__main__":
    # word2vec + minLSH
    Blocking(is_train=False, if_word_2vec=False, if_min_lsh=True)
    # CountVectorizer + minLSH
    # Blocking(is_train=True, if_word_2vec=False, if_min_lsh=True)