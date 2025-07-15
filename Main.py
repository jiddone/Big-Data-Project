import datetime
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.clustering import LDA
from pyspark.ml import Pipeline
import os
import re
from pyspark.ml.linalg import Vectors
import random
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import pandas as pd
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

#lessico utilizzato da VADER
nltk.download('vader_lexicon')

#SESSIONE
#metodo per la creazione della sessione Spark
def create_spark_session(app_name="Tweet Processing"):
    return SparkSession.builder \
    .appName(app_name) \
    .config("spark.executorEnv.WINUTILS", "C:/Users/gianl/Desktop/spark-3.5.5-bin-hadoop3/bin/winutils.exe") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.hadoop.native.lib", "false") \
    .master("local[*]") \
    .config("spark.local.dir", "C:/Users/gianl/Desktop/Spark Cestino") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "150") \
    .config("spark.storage.memoryFraction", 0.5) \
    .config("spark.shuffle.spill", True) \
    .getOrCreate()

#metodo per la definizione dei tipi degli attributi nel dataset
def get_schema():
    return StructType([
    StructField("tweet_id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("user_id_str", StringType(), True),
    StructField("text", StringType(), True),
    StructField("hashtags", StringType(), True),
    StructField("retweet_count", IntegerType(), True),
    StructField("favorite_count", IntegerType(), True),
    StructField("in_reply_to_screen_name", StringType(), True),
    StructField("source", StringType(), True),
    StructField("retweeted", BooleanType(), True),
    StructField("lang", StringType(), True),
    StructField("location", StringType(), True),
    StructField("place_name", StringType(), True),
    StructField("place_lat", StringType(), True),
    StructField("place_lon", StringType(), True),
    StructField("screen_name", StringType(), True)
])


#CARICAMENTO DATI
#Directory di riferimento del dataset
csv_directory = 'C:/Users/gianl/Desktop/Progetto Big Data/Dataset'

#metodo per verificare se la colonna 'created_at' ha un formato valido
def is_valid_date(date_col):
    #Confronta la data con il formato 'yyyy-MM-dd HH:mm:ss'
    return to_timestamp(col(date_col), 'yyyy-MM-dd HH:mm:ss').isNotNull()

#metodo per il filling dei campi created_at vuoti o invalidi
def fill_missing_created_at(df, filepath):
    filename = os.path.basename(filepath)
    try:
        day = filename.split("_")[2].zfill(2)
    except IndexError:
        raise ValueError("Il nome del file non segue il formato atteso")
    
    # Genera orario casuale
    hour = str(random.randint(0, 23)).zfill(2)
    minute = str(random.randint(0, 59)).zfill(2)
    second = str(random.randint(0, 59)).zfill(2)

    # Crea una data base (yyyy-MM-dd 00:00:00)
    date_str = f"2020-10-{day} {hour}:{minute}:{second}"

    #Si utilizza la data creata solo per i campi created_at vuoti o non validi
    df = df.withColumn(
        "created_at",
        when(
            (col("created_at").isNull()) | (~is_valid_date("created_at")),
            lit(date_str)
        ).otherwise(col("created_at"))
    )

    return df


#metodo per la gestione dei campi retweet_count e/o favorite_count nullo
def fill_missing_retweets_and_favorites(df):
    #In entrambi i casi, il campo, se vuoto, viene impostato a 0
    df = df.withColumn(
        "retweet_count", 
        when(col("retweet_count").isNull(), lit(0)).otherwise(col("retweet_count"))
    )
    
    df = df.withColumn(
        "favorite_count", 
        when(col("favorite_count").isNull(), lit(0)).otherwise(col("favorite_count"))
    )
    
    return df

#metodo per la gestione dei campi retweeted malformati
def fix_retweeted_column(df):
    #se il tweet contiene 'RT @' significa che è un retweet
    return df.withColumn(
        "retweeted",  
        when(col("text").contains("RT @"), lit(True))  #Imposta a True se la sottostringa "RT @" è presente
        .otherwise(col("retweeted"))  #Altrimenti lascia invariato il valore di 'retweeted'
    )

#metodo per il caricamento e pulizia dati
def load_and_clean_data(spark, csv_directory):
    schema = get_schema()
    #tutti i file CSV nella directory
    csv_files = [os.path.join(csv_directory, f) for f in os.listdir(csv_directory) if f.endswith('.csv')]

    df_spark_cleaned_list = []

    #caricamento di ciascun file del dataset
    for file_path in csv_files:
        df = spark.read.option("header", "true") \
               .option("multiLine", "true") \
               .option("quote", "\"") \
               .option("escape", "\"") \
               .option("mode", "PERMISSIVE") \
               .schema(schema) \
               .csv(file_path)
        
        #pulizia e gestione degli attributi mancanti
        df_cleaned = fill_missing_created_at(df, file_path)
        
        #Aggiunta alla lista dei DataFrame puliti
        df_spark_cleaned_list.append(df_cleaned)

    #Unione di tutti i DataFrame in un unico DataFrame
    df_spark = df_spark_cleaned_list[0]
    for df in df_spark_cleaned_list[1:]:
        df_spark = df_spark.union(df)

    #numero di partizioni del dataset
    df_spark = df_spark.repartition(150)
    print(f"Numero di partizioni: {df_spark.rdd.getNumPartitions()}")

    
    #Numero di righe prima della pulizia
    num_righe_originali = df_spark.count()
    print(f"Numero di righe originali: {num_righe_originali}")

    #Stampa lo schema del DataFrame
    df_spark.printSchema()

    #PULIZIA DATAFRAME
    #Rimozione delle colonne location, place_lat e place_lon dal DataFrame
    df_spark_cleaned = df_spark.drop("location", "place_lat", "place_lon")

    #Rimozione delle righe con 'lang' di lunghezza maggiore di 2 (causa righe malformate)
    df_spark_cleaned = df_spark_cleaned.filter(col("lang").rlike("^[a-z]{2}$"))

    #Rimozione delle righe con valore nullo nella colonna 'text'
    df_spark_cleaned = df_spark_cleaned.filter(col("text").isNotNull())

    #gestione campi
    df_spark_cleaned = fix_retweeted_column(df_spark_cleaned)

    df_spark_cleaned = fill_missing_retweets_and_favorites(df_spark_cleaned)

    #Rimozione delle righe con 'place_name' nullo
    df_spark_cleaned = df_spark_cleaned.filter(col("place_name").isNotNull())

    #Verifica del numero di righe dopo la pulizia
    print(f"Numero di righe dopo la pulizia: {df_spark_cleaned.count()}")

    return df_spark_cleaned

#metodo veloce per la stampa del dataframe
def print_first_n_rows(df, n):
    for i, row in enumerate(df.limit(n).collect()):
        print(f"Riga {i + 1}:")
        for col_name, value in row.asDict().items():
            print(f"{col_name}: {value}")
        print("\n" + "-"*50 + "\n")

#METRICHE
#numero totale di tweet
def total_tweets(df):
    return df.count()

#numero totale di utenti unici
def total_unique_users(df):
    return df.select("user_id_str").distinct().count()

#numero totale di hashtag distinti
def total_unique_hashtags(df):
    return df.withColumn("hashtag", explode(split(col("hashtags"), " "))) \
             .select("hashtag").distinct().count()

#numero totale di tweet per lingua distinta
def total_tweets_per_language(df):
    #Calcolo del numero di tweet per ogni lingua
    language_count = df.groupBy("lang").count().collect()

    #Creazione di un dizionario dove la chiave è la lingua e il valore è il numero di tweet
    language_dict = {row['lang']: row['count'] for row in language_count}
    
    #Ordinamento del dizionario in ordine decrescente e selezione dei primi 5 elementi
    top_5_languages = dict(sorted(language_dict.items(), key=lambda item: item[1], reverse=True)[:5])
    
    return top_5_languages

#numero totale di posti distinti
def total_unique_place_names(df):
    return df.select("place_name").distinct().count()

#numero totale di tweet giornalieri
def total_tweets_per_day(df, date_str):
    #Estrazione della parte della data (yyyy-MM-dd) da 'created_at'
    df_filtered = df.filter(substring(col('created_at'), 1, 10) == date_str)
    
    #Conteggio del numero di tweet per quel giorno
    total_tweets = df_filtered.count()
    
    return total_tweets

#numero totale di tweet in un range di giorni
def total_tweets_in_range(df, start_date, end_date):
    #Estrazione della parte della data (yyyy-MM-dd) da 'created_at'
    df_filtered = df.filter(
        (substring(col('created_at'), 1, 10) >= start_date) & 
        (substring(col('created_at'), 1, 10) <= end_date)
    )
    
    #Conteggio del numero di tweet nell'intervallo di date
    total_tweets = df_filtered.count()
    
    return total_tweets

#metodo per generare la lista di tutte le date di ottobre 2020
def get_october_dates():
    start_date = datetime(2020, 10, 1)
    end_date = datetime(2020, 10, 31)
    
    delta = timedelta(days=1)
    dates = []
    
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += delta
    
    return dates

#distribuzione dei tweet giornalieri
def get_daily_tweet_counts(df):
    return df.withColumn("date", substring(col("created_at"), 1, 10)) \
             .groupBy("date").count() \
             .withColumnRenamed("count", "total_tweets") \
             .orderBy("date") \
             .toPandas()

#QUERY

#top 10 utenti che hanno postato di più (retweet esclusi)
def most_tweets(df):
    #Filtraggio dei retweet
    df_filtered = df.filter(df['retweeted'] == False)

    user_tweet_count = df_filtered.groupBy('screen_name').count()

    #Ordinamento per numero di tweet in ordine decrescente
    df_sorted = user_tweet_count.orderBy('count', ascending=False)
    
    #Conversione a dizionario {nome: conteggio}
    most_active_users_dict = {row['screen_name']: row['count'] for row in df_sorted.limit(10).collect()}
    
    return most_active_users_dict

#top 10 utenti che retweettano di più
def most_retweeters(df):
    #Filtraggio dei retweet
    df_retweets = df.filter(df['retweeted'] == True)

    user_retweet_count = df_retweets.groupBy('screen_name').count()

    #Ordinamento per numero di tweet in ordine decrescente
    df_sorted = user_retweet_count.orderBy('count', ascending=False)

    #Conversione a dizionario {nome: conteggio}
    most_retweeters_dict = {row['screen_name']: row['count'] for row in df_sorted.limit(10).collect()}
    
    return most_retweeters_dict

#top 10 utenti per engagement (tweet + retweet)
def most_engagement(df):
    #Raggruppamento di tutti i post (tweet e retweet) per utente
    user_total_count = df.groupBy('screen_name').count()

    #Ordinamento per numero totale in ordine decrescente
    df_sorted = user_total_count.orderBy('count', ascending=False)

    #Conversione a dizionario {nome: conteggio}
    most_engagement_dict = {row['screen_name']: row['count'] for row in df_sorted.limit(10).collect()}

    return most_engagement_dict

#tweet di un singolo giorno
def tweets_of_day(df, date_str):
    df_filtered = df.filter(substring(col('created_at'), 1, 10) == date_str)
    return df_filtered

#tweet in un range di giorni
def tweets_in_range(df, start_date, end_date):
    #Estrazione della parte della data (yyyy-MM-dd) da 'created_at'
    df_filtered = df.filter(
        (substring(col('created_at'), 1, 10) >= start_date) & 
        (substring(col('created_at'), 1, 10) <= end_date)
    )
    
    return df_filtered

#tweet con più like 
def tweets_sorted_by_likes(df):
    df_original = df.filter(col('retweeted') == False)
    df_sorted = df_original.orderBy(col('favorite_count'), ascending=False).limit(10)
    return df_sorted

#tweet più retweettati
def tweets_sorted_by_retweets(df):
    df_original = df.filter(col('retweeted') == False)
    df_sorted = df_original.orderBy(col('retweet_count'), ascending=False).limit(10)
    return df_sorted

#tweet con più interazioni (like+repost)
def tweets_sorted_by_engagement(df):
    df_original = df.filter(col('retweeted') == False)
    #Ordinamento in base alla somma di like e retweet in ordine decrescente
    df_sorted = df_original.orderBy((col('favorite_count') + col('retweet_count')).desc()).limit(10)
    
    return df_sorted

#utenti con più like
def top_users_by_likes(df):
    #Raggruppamento per 'screen_name' e somma dei like ricevuti
    top_users_likes = df.groupBy("screen_name").agg(
        sum("favorite_count").alias("total_likes")
    )

    #Ordinamento dei risultati in ordine decrescente di total_likes
    top_users_likes = top_users_likes.orderBy(col("total_likes"), ascending=False).limit(10)

    #Conversione in un dizionario
    users_likes_dict = {row['screen_name']: row['total_likes'] for row in top_users_likes.collect()}

    return users_likes_dict

#utenti con più retweet
def top_users_by_reposts(df):
    #Raggruppamento per 'screen_name' e somma 'retweet_count' per ogni utente
    top_reposts = df.groupBy("screen_name").agg(sum("retweet_count").alias("total_reposts"))
    
    #Ordinamento per numero di retweet in ordine decrescente
    top_reposts = top_reposts.orderBy(desc("total_reposts")).limit(10)
    
    #Conversione in un dizionario
    reposts_dict = {row['screen_name']: row['total_reposts'] for row in top_reposts.collect()}
    
    return reposts_dict

#hashtag in trend
def top_hashtags(df):
    #Estrazione degli hashtag dalla colonna 'hashtags'
    hashtags_df = df.withColumn("hashtag", explode(split(col("hashtags"), " ")))

    #Filtragio degli hashtag non vuoti e non uguali a 'NA'
    hashtags_df = hashtags_df.filter((col("hashtag") != "") & (col("hashtag") != "NA"))

    #Rimozione di eventuali parentesi quadre e virgolette
    hashtags_df = hashtags_df.withColumn("hashtag", regexp_replace(col("hashtag"), r'["\[\],]', ''))

    #Aggiunta di '#' all'inizio dell'hashtag
    hashtags_df = hashtags_df.withColumn("hashtag", concat(lit('#'), col("hashtag")))

    #Raggruppamento per hashtag e conteggio delle occorrenze
    top_hashtags = hashtags_df.groupBy("hashtag").agg(count("hashtag").alias("count"))

    #Ordinamento per frequenza in ordine decrescente
    top_hashtags = top_hashtags.orderBy(desc("count")).limit(10)

    #Conversione in dizionario
    hashtags_dict = {row['hashtag']: row['count'] for row in top_hashtags.collect()}
    
    return hashtags_dict

#utenti più menzionati
def top_mentioned_users(df):
    #Estrazione di tutte le menzioni con regex: cerca @ seguito da caratteri alfanumerici o underscore
    mention_pattern = r'@(\w+)'
    
    #Creazione colonna 'mention' con la prima menzione trovata, poi usa split per prendere tutte
    df_mentions = df.withColumn("mention", regexp_extract(col("text"), mention_pattern, 0))
    df_mentions = df.withColumn("mentions", split(col("text"), " "))
    df_mentions = df_mentions.withColumn("mention", explode(col("mentions")))
    df_mentions = df_mentions.filter(col("mention").startswith("@"))

    #Rimozione di eventuali punteggiature accodate (es. "@user.")
    df_mentions = df_mentions.withColumn("mention", regexp_extract(col("mention"), mention_pattern, 1))

    #Conteggio
    top_mentions = df_mentions.groupBy("mention").agg(count("*").alias("count")).orderBy(col("count").desc()).limit(10)
    
    return {row["mention"]: row["count"] for row in top_mentions.collect()}

#RICERCA
#ricerca utente
def get_user_tweets(df, username):
    # Rimozione del simbolo '@' all'inizio, se presente
    clean_username = username.lstrip("@")

    return df.filter(col("screen_name") == clean_username)

#ricerca per parola
def get_tweets_by_keyword(df, keyword):
    return df.filter(col("text").contains(keyword))

#ricerca per hashtag
def get_tweets_by_hashtag(df, hashtag):
    #rimozione del simbolo '#' all'inizio
    clean_hashtag = hashtag.replace("#", "").lower()
    return df.filter(lower(col("hashtags")).contains(clean_hashtag))

#SENTIMENT ANALISYS
#preprocessing del dataframe per sentiment analisys
def preprocess_text_sentiment(df):
    #Rimozione dei retweet
    df = df.filter(df["retweeted"] == False)

    #Rimozione delle menzioni (@username)
    df = df.withColumn("text", regexp_replace(col("text"), r'@\w+', ''))
    
    #Conversione del testo in minuscolo
    df = df.withColumn("text", lower(col("text")))
    
    #Rimozione degli URL
    df = df.withColumn("text", regexp_replace(col("text"), r'http\S+|www\S+', ''))

    return df

#metodo per ottenere il punteggio di sentiment
def get_sentiment(text, analyzer):
    sentiment = analyzer.polarity_scores(text)
    return sentiment['compound'] 

#metodo di sentiment analisys
def sentiment_analysis(df):
    #Creazione di un'istanza di SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()

    sentiment_udf = udf(lambda text: get_sentiment(text, analyzer), FloatType())

    #Applicazione della UDF al dataframe, creazione di una nuova colonna "sentiment"
    df = df.withColumn("sentiment", sentiment_udf(col("text")))

    return df

# Metodo per ottenere la top 10 dei commenti più positivi
def get_top_positive_comments(df):
    return df.filter(col("sentiment") <= 0.9).orderBy(col("sentiment"), ascending=False).limit(10)

# Metodo per ottenere la top 10 dei commenti più negativi
def get_top_negative_comments(df):
    return df.filter(col("sentiment") >= -0.9).orderBy(col("sentiment"), ascending=True).limit(10)

# Metodo per ottenere 10 commenti neutri
def get_neutral_comments(df):
    return df.filter((col("sentiment") > -0.1) & (col("sentiment") < 0.1)).limit(10)

#metodo che restituisce il numero di commenti positivi, neutrali e negativi
def sentiment_distribution(df):
    negative_count = df.filter(col("sentiment") < -0.3333).count()
    neutral_count = df.filter((col("sentiment") >= -0.3333) & (col("sentiment") <= 0.3333)).count()
    positive_count = df.filter(col("sentiment") > 0.3333).count()
    return negative_count, neutral_count, positive_count

#TOPIC MODELING
#metodo di preprocessing del dataframe per Topic Modeling
def preprocess_text_LDA(df):
    #Considero solo i tweet in inglese
    df = df.filter(df["lang"] == "en")

    #Rimozione dei retweet
    df = df.filter(df["retweeted"] == False)

    #Rimozione delle menzioni (@username)
    df = df.withColumn("text", regexp_replace(col("text"), r'@\w+', ''))
    
    #Conversione del testo in minuscolo
    df = df.withColumn("text", lower(col("text")))
    
    #Rimozione degli URL
    df = df.withColumn("text", regexp_replace(col("text"), r'http\S+|www\S+', ''))
    
    #Rimozione della punteggiatura
    df = df.withColumn("text", regexp_replace(col("text"), r'[^\w\s]', ''))
    
    #Rimozione di numeri
    df = df.withColumn("text", regexp_replace(col("text"), r'\d+', ''))

    #Tokenizzazione
    tokenizer = udf(lambda x: x.split(), ArrayType(StringType()))
    df = df.withColumn("tokens", tokenizer(col("text")))

    #Rimozione stopwords
    remover = StopWordsRemover(inputCol="tokens", outputCol="filtered_tokens")
    df = remover.transform(df)

    #Rimozione di tweet troppo corti (meno di 5 parole significative)
    df = df.withColumn("filtered_len", size(col("filtered_tokens"))) \
           .filter(col("filtered_len") >= 5) \
           .drop("filtered_len")
    
    return df

#metodo per creazione del vocabolario e applicazione di LDA
def create_vocabulary_and_apply_LDA(df, num_topics, max_iter):
    #Creazione di un CountVectorizer per creare il vocabolario
    vectorizer = CountVectorizer(inputCol="filtered_tokens", outputCol="features", vocabSize=134892, minDF=10)

    #Creaazione della pipeline
    pipeline = Pipeline(stages=[vectorizer])

    #Applicazione della pipeline ai dati
    model = pipeline.fit(df)
    df_transformed = model.transform(df)

    #Estrazione del vocabolario dal CountVectorizer
    vocabulary = model.stages[0].vocabulary
    
    #Mapping degli indici ai termini del vocabolario
    def map_indices_to_terms(indices):
        return [vocabulary[i] for i in indices]

    #metodo per la creazione della colonna terms che contiene le stringhe (non gli indici) dei termini
    def add_terms_column_to_topics(topics, vocabulary):
        # Creiamo una funzione UDF per applicare la mappatura
        map_indices_to_terms_udf = udf(map_indices_to_terms, ArrayType(StringType()))

        # Aggiungiamo la nuova colonna 'terms' al DataFrame
        topics_with_terms = topics.withColumn("terms", map_indices_to_terms_udf(topics["termIndices"]))

        return topics_with_terms

    #Esecuzione di LDA sul dataset trasformato
    lda = LDA(k=num_topics, maxIter=max_iter, featuresCol="features", seed=1234)
    
    #Allenamento del modello LDA
    lda_model = lda.fit(df_transformed)
    
    #topic e i termini associati a ciascun topic
    topics = lda_model.describeTopics(maxTermsPerTopic=10)

    #Aggiunta della colonna 'terms' ai topics
    topics_with_terms = add_terms_column_to_topics(topics, vocabulary)
    
    #distribuzione dei topic per ogni documento
    topic_distribution = lda_model.transform(df_transformed)
    
    return lda_model, topics_with_terms, topic_distribution, model

#metodo che restituisce la top 10 dei topic all'interno del dataset e 10 tweet del topic più rilevante
def get_top_10_topics(topic_distribution_df, topic_df):

    #metodo per convertire il vector in un array
    def vector_to_array(vettore):
        return vettore.toArray().tolist()

    vector_to_array_udf = udf(vector_to_array, ArrayType(DoubleType()))

    #Aggiunta della colonna 'topic_distribution_array' convertendo 'topicDistribution' in un array
    topic_distribution_df = topic_distribution_df.withColumn("topic_distribution_array", vector_to_array_udf(col("topicDistribution")))

    #metodo per ottenere l'indice del massimo valore in topic_distribution_array
    def get_max_index(arr):
        if not arr:
            return None
        max_value = arr[0]
        max_index = 0
        for i in range(1, len(arr)):
            if arr[i] > max_value:
                max_value = arr[i]
                max_index = i
        return max_index
    
    get_max_index_udf = udf(get_max_index, IntegerType())

    #Aggiunta della colonna 'main_topic' applicando la funzione get_max_index
    topic_distribution_df = topic_distribution_df.withColumn("main_topic", get_max_index_udf(col("topic_distribution_array")))

    #metodo che conta il numero di occorrenze per ogni indice in 'main_topic'
    def count_occurrences(df):
        count_df = topic_distribution_df.groupBy("main_topic") \
                                .count() \
                                .withColumnRenamed("count", "occurrences") \
                                .orderBy(desc("occurrences"))
        return count_df
    

    count_df = count_occurrences(topic_distribution_df)

    count_df = count_df.withColumnRenamed("main_topic", "topic")

    #Unione con TopicDF per aggiungere la colonna 'terms'
    count_df_with_terms = count_df.join(topic_df, "topic", "left") \
                              .select(count_df.topic, count_df["occurrences"], topic_df.terms)
    
    #Ordinamento in ordine decrescente rispetto a "occurrences"
    count_df_with_terms = count_df_with_terms.orderBy(desc("occurrences"))

    #topic più rilevante
    top_topic = count_df_with_terms.first()["topic"]

    #Selezione di 10 esempi da topic_distribution_df con quel topic
    example_rows = topic_distribution_df.filter(col("main_topic") == top_topic).limit(10)

    return count_df_with_terms.limit(10), example_rows




#TEST
os.environ["PYSPARK_PYTHON"] = "python"
spark=create_spark_session(app_name="Tweet Processing")
spark.conf.set("spark.hadoop.fs.local", "true")
df_cleaned = load_and_clean_data(spark, csv_directory)

#REGISTRO I DATI DEI DIVERSI METODI IN FILE
tweet_count = total_tweets(df_cleaned)
with open("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/total_tweets.txt", "w") as file:
    file.write(f"{tweet_count}\n")

unique_users_count = total_unique_users(df_cleaned)
with open("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/total_unique_users.txt", "w") as file:
    file.write(f"{unique_users_count}\n")

unique_hashtags_count = total_unique_hashtags(df_cleaned)
with open("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/total_unique_hashtags.txt", "w") as file:
    file.write(f"{unique_hashtags_count}\n")

unique_places_count = total_unique_place_names(df_cleaned)
with open("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/total_locations.txt", "w") as file:
    file.write(f"{unique_places_count}\n")

tweets_per_language = total_tweets_per_language(df_cleaned)
with open("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/total_tweets_per_language.json", "w") as file:
    json.dump(tweets_per_language, file, indent=4)

daily_tweet_counts = get_daily_tweet_counts(df_cleaned)
daily_tweet_counts.to_csv("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/daily_tweets.csv", index=False)

most_active_users = most_tweets(df_cleaned)
with open("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/most_posters.json", "w") as file:
    json.dump(most_active_users, file, indent=4)

most_retweeters = most_retweeters(df_cleaned)
with open("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/most_retweeters.json", "w") as file:
    json.dump(most_retweeters, file, indent=4)

most_engagement = most_engagement(df_cleaned)
with open("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/most_engagement_users.json", "w") as file:
    json.dump(most_engagement, file, indent=4)

tweets_by_likes = tweets_sorted_by_likes(df_cleaned)
tweets_by_likes.toPandas().to_csv("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/top_tweet_likes.csv", index=False)

tweets_by_retweets = tweets_sorted_by_retweets(df_cleaned)
tweets_by_retweets.toPandas().to_csv("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/top_tweet_retweet.csv", index=False)

tweets_by_engagement = tweets_sorted_by_engagement(df_cleaned)
tweets_by_engagement.toPandas().to_csv("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/top_tweet_engagement.csv", index=False)


likes_dict = top_users_by_likes(df_cleaned)
reposts_dict = top_users_by_reposts(df_cleaned)
hashtags_dict = top_hashtags(df_cleaned)
mentions_dict = top_mentioned_users(df_cleaned)

with open("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/top_users_by_likes.json", "w") as f:
    json.dump(likes_dict, f)

with open("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/top_users_by_repost.json", "w") as f:
    json.dump(reposts_dict, f)

with open("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/top_hashtags.json", "w") as f:
    json.dump(hashtags_dict, f)

with open("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/top_mentioned_users.json", "w") as f:
    json.dump(mentions_dict, f)


df_preprocessed = preprocess_text_sentiment(df_cleaned)

df_with_sentiment = sentiment_analysis(df_preprocessed)

top_pos = get_top_positive_comments(df_with_sentiment)
top_neg = get_top_negative_comments(df_with_sentiment)
neutral = get_neutral_comments(df_with_sentiment)

get_top_positive_comments(df_with_sentiment).toPandas().to_csv("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/top_positive_comments.csv", index=False)
get_top_negative_comments(df_with_sentiment).toPandas().to_csv("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/top_negative_comments.csv", index=False)
get_neutral_comments(df_with_sentiment).toPandas().to_csv("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/neutral_comments.csv", index=False)

negative, neutral, positive = sentiment_distribution(df_with_sentiment)

with open("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/negative_comments_count.txt", "w") as f:
    f.write(f"{negative}\n")

with open("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/neutral_comments_count.txt", "w") as f:
    f.write(f"{neutral}\n")

with open("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/positive_comments_count.txt", "w") as f:
    f.write(f"{positive}\n")

df_preprocessed = preprocess_text_LDA(df_cleaned)
print(f"Numero di righe nel dataset preprocessato: {df_preprocessed.count()}")
df_preprocessed = df_preprocessed.orderBy(rand()).limit(500000)
lda_model, topics_with_terms, topic_distribution, vectorizer_model = create_vocabulary_and_apply_LDA(df_preprocessed, num_topics=25, max_iter=10)
top_10_topics_df, top_examples_df = get_top_10_topics(topic_distribution, topics_with_terms)

top_10_topics_df.toPandas().to_csv("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/top_10_topics.csv", index=False)
top_examples_df.toPandas().to_csv("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/top_topic_examples.csv", index=False)
