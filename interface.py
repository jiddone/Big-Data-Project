import streamlit as st
import json
import pandas as pd
import matplotlib.pyplot as plt


def create_interface():
    #TITOLO
    st.set_page_config(page_title="Twitter Tracker", layout="wide")
    #divisione del display in tre colonne
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        #Centra il titolo all'interno della colonna centrale
        st.markdown("<h1 style='text-align: center;'>Twitter Tracker</h1>", unsafe_allow_html=True)
        st.markdown("<h2 style='text-align: center;'>Analisi di Tweet del periodo pre-elezioni USA 2020</h2>", unsafe_allow_html=True)
    
    
    #SEZIONE INFORMAZIONI GENERICHE
    #################################################
    # Lettura dati
    with open("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/total_tweets.txt", "r") as file:
        total_tweets = file.readline().strip()

    with open("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/total_unique_users.txt", "r") as file:
        total_unique_users = file.readline().strip()

    with open("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/total_unique_hashtags.txt", "r") as file:
        total_unique_hashtags = file.readline().strip()

    with open("C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/total_locations.txt", "r") as file:
        total_locations = file.readline().strip()

    #dataframe contenente i dati
    df_info = pd.DataFrame({
        "Informazione": ["Numero totale di tweet", "Numero totale di utenti univoci", "Numero totale di hashtags univoci", "Numero totale di locations"],
        "Valore": [total_tweets, total_unique_users, total_unique_hashtags, total_locations]
    })

    with col2:
        # Titolo della sezione
        st.header("Informazioni generiche sul dataset")
        st.markdown("<div style='text-align: center;'>", unsafe_allow_html=True)
        st.dataframe(df_info, width=900, use_container_width=False, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)

    #################################################

    #SEZIONE LINGUE
    #################################################
    # Percorso del file JSON
    tweets_per_language = "C:/Users/gianl/Desktop/Progetto Big Data/Intermediate Data/total_tweets_per_language.json"

    #Caricamento e lettura del file JSON
    with open(tweets_per_language, "r") as file:
        language_data = json.load(file)

    #Dataframe contenente i dati
    df = pd.DataFrame(list(language_data.items()), columns=["Lingua", "Numero di Tweet"])

    with col2:
        st.header("Lingue più utilizzate nei tweet")
        df = pd.DataFrame(list(language_data.items()), columns=["Lingua", "Numero di Tweet"])
        st.markdown(
        """
        <style>
        .dataframe-container {
            max-width: 900px;
            margin: 0 auto;
        }
        </style>
        """, unsafe_allow_html=True
        )
        with st.container():
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(df, width=900, use_container_width=False, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)
    #################################################

    #SEZIONE TWEET GIORNALIERI
    #################################################
    # Markdown per personalizzare la larghezza della sezione
    st.markdown(
        """
        <style>
        .content-container {
            max-width: 900px;
            margin: 0 auto;
        }
        </style>
        """, unsafe_allow_html=True
    )

    with col2:
        st.header("Distribuzione Giornaliera dei Tweet")
        st.image(r"C:\Users\gianl\Desktop\Progetto Big Data\Intermediate Data\daily_tweets_plot.png", width=900)
    #################################################

    #SEZIONE TOP POSTERS
    #################################################
    #Caricamento dati
    with open(r"C:\Users\gianl\Desktop\Progetto Big Data\Intermediate Data\most_posters.json") as f:
        most_posters = json.load(f)

    #Dataframe contenente i dati
    df_most_posters = pd.DataFrame(list(most_posters.items()), columns=['Utente', 'Numero di post'])

    #Markdown per personalizzare la larghezza della sezione
    st.markdown(
        """
        <style>
        .content-container {
            max-width: 900px;
            margin: 0 auto;
        }
        .dataframe-container {
            width: 500px;
        }
        </style>
        """, unsafe_allow_html=True
    )

    with col2:
        st.header("Utenti con più post")
        with st.container():
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(df_most_posters, width=900, use_container_width=False, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)
    #################################################

    #SEZIONE TOP RETWEETERS 
    #################################################
    #Caricamento dati
    with open(r"C:\Users\gianl\Desktop\Progetto Big Data\Intermediate Data\most_retweeters.json") as f:
        most_posters = json.load(f)

    #Dataframe contenente i dati
    df_most_retweeters = pd.DataFrame(list(most_posters.items()), columns=['Utente', 'Numero di retweet'])

    #Markdown per personalizzare la larghezza della sezione
    st.markdown(
        """
        <style>
        .content-container {
            max-width: 900px;
            margin: 0 auto;
        }
        .dataframe-container {
            width: 500px;
        }
        </style>
        """, unsafe_allow_html=True
    )

    with col2:
        st.header("Utenti con più retweet")
        with st.container():
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(df_most_retweeters, width=900, use_container_width=False, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)
    #################################################

    #SEZIONE TOP ENGAGEMENT 
    #################################################
    #Caricamento dati
    with open(r"C:\Users\gianl\Desktop\Progetto Big Data\Intermediate Data\most_engagement_userS.json") as f:
        most_posters = json.load(f)

    #Dataframe contenente i dati
    df_most_engagement = pd.DataFrame(list(most_posters.items()), columns=['Utente', 'Engagement'])

    #Markdown per personalizzare la larghezza della sezione
    st.markdown(
        """
        <style>
        .content-container {
            max-width: 900px;
            margin: 0 auto;
        }
        .dataframe-container {
            width: 500px;
        }
        </style>
        """, unsafe_allow_html=True
    )

    with col2:
        st.header("Utenti con più engagement (POST+RETWEET)")
        with st.container():
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(df_most_engagement, width=900, use_container_width=False, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)
    #################################################

    #SEZIONE TWEET CON PIù LIKE
    #################################################

    #Caricamento dati
    df_likes = pd.read_csv(r"C:\Users\gianl\Desktop\Progetto Big Data\Intermediate Data\top_tweet_likes.csv")

    #Rimozione delle colonne specificate
    df_likes = df_likes.drop(columns=[
        "tweet_id", "created_at", "user_id_str", "hashtags",
        "retweet_count", "in_reply_to_screen_name", "source",
        "retweeted", "lang", "place_name"
    ])

    df_likes.columns = ["Tweet", "Numero like", "Nome utente"]

    #Markdown per la larghezza
    st.markdown(
        """
        <style>
        .content-container {
            max-width: 900px;
            margin: 0 auto;
        }
        .dataframe-container {
            width: 500px;
        }
        </style>
        """, unsafe_allow_html=True
    )

    with col2:
        st.header("Tweet con più like")
        with st.container():
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(df_likes, width=900, use_container_width=False, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)
    #################################################
    
    #SEZIONE TWEET CON PIù RETWEET
    #################################################

    #Caricamento dati
    df_ret = pd.read_csv(r"C:\Users\gianl\Desktop\Progetto Big Data\Intermediate Data\top_tweet_retweet.csv")

    #Rimozione delle colonne specificate
    df_ret = df_ret.drop(columns=[
        "tweet_id", "created_at", "user_id_str", "hashtags",
        "favorite_count", "in_reply_to_screen_name", "source",
        "retweeted", "lang", "place_name"
    ])

    df_ret.columns = ["Tweet", "Numero retweet", "Nome utente"]

    #Markdown per la larghezza
    st.markdown(
        """
        <style>
        .content-container {
            max-width: 900px;
            margin: 0 auto;
        }
        .dataframe-container {
            width: 500px;
        }
        </style>
        """, unsafe_allow_html=True
    )

    with col2:
        st.header("Tweet con più retweet")
        with st.container():
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(df_ret, width=900, use_container_width=False, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)
    #################################################

    #SEZIONE TWEET CON PIù ENGAGEMENT
    #################################################

    #Caricamento dati
    df_eng = pd.read_csv(r"C:\Users\gianl\Desktop\Progetto Big Data\Intermediate Data\top_tweet_retweet.csv")

    #Rimozione delle colonne specificate
    df_eng = df_eng.drop(columns=[
        "tweet_id", "created_at", "user_id_str", "hashtags",
        "in_reply_to_screen_name", "source",
        "retweeted", "lang", "place_name"
    ])

    df_eng.columns = ["Tweet", "Numero retweet","Numero like", "Nome utente"]

    #Markdown per la larghezza
    st.markdown(
        """
        <style>
        .content-container {
            max-width: 900px;
            margin: 0 auto;
        }
        .dataframe-container {
            width: 500px;
        }
        </style>
        """, unsafe_allow_html=True
    )

    with col2:
        st.header("Tweet con più engagement")
        with st.container():
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(df_eng, width=900, use_container_width=False, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)
    #################################################

    #SEZIONE TOP HASHTAGS
    #################################################
    #Caricamento dati
    with open(r"C:\Users\gianl\Desktop\Progetto Big Data\Intermediate Data\top_hashtags.json", "r") as file:
        data = json.load(file)

    #Dataframe contenente i dati
    df_hashtags = pd.DataFrame(list(data.items()), columns=["Hashtag", "Numero"])

    with col2:
        st.header("Top Hashtags")
        with st.container():
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(df_hashtags, width=900, use_container_width=False, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)

    #################################################

    #SEZIONE TOP UTENTI MENZIONATI
    #################################################
    #Caricamento dati
    with open(r"C:\Users\gianl\Desktop\Progetto Big Data\Intermediate Data\top_mentioned_users.json", "r") as file:
        data = json.load(file)

    #Dataframe contenente i dati
    df_mentions = pd.DataFrame(list(data.items()), columns=["Nome Utente", "Numero menzioni"])

    with col2:
        st.header("Top Utenti Menzionati")
        with st.container():
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(df_mentions, width=900, use_container_width=False, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)
    #################################################

    #SEZIONE UTENTI CHE HANNO RICEVUTO PIù LIKE
    #################################################
    #Caricamento dati
    with open(r"C:\Users\gianl\Desktop\Progetto Big Data\Intermediate Data\top_users_by_likes.json", "r") as file:
        data_likes = json.load(file)

    #Dataframe contenente i dati
    df_likes = pd.DataFrame(list(data_likes.items()), columns=["Nome utente", "Numero totale di like"])

    with col2:
        st.header("Utenti che hanno ricevuto più like")
        with st.container():
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(df_likes, width=900, use_container_width=False, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)

    #################################################


    #SEZIONE UTENTI CHE HANNO RICEVUTO PIù RETWEET
    #################################################
    #Caricamento dati
    with open(r"C:\Users\gianl\Desktop\Progetto Big Data\Intermediate Data\top_users_by_repost.json", "r") as file:
        data_reposts = json.load(file)

    #Dataframe contenente i dati
    df_reposts = pd.DataFrame(list(data_reposts.items()), columns=["Nome utente", "Numero totale di retweet"])

    with col2:
        st.header("Utenti che hanno ricevuto più retweet")
        with st.container():
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(df_reposts, width=900, use_container_width=False, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)

    #################################################


    #SEZIONE SENTIMENT ANALISYS
    #################################################

    #Caricamento dati
    negative_comments = open(r"C:\Users\gianl\Desktop\Progetto Big Data\Intermediate Data\negative_comments_count.txt", "r").read().strip()
    neutral_comments = open(r"C:\Users\gianl\Desktop\Progetto Big Data\Intermediate Data\neutral_comments_count.txt", "r").read().strip()
    positive_comments = open(r"C:\Users\gianl\Desktop\Progetto Big Data\Intermediate Data\positive_comments_count.txt", "r").read().strip()

    #Creazione di un dizionario per i conteggi
    data = {
        "Negative Comments": negative_comments,
        "Neutral Comments": neutral_comments,
        "Positive Comments": positive_comments
    }

    #Dataframe contenente i conteggi
    df_counts = pd.DataFrame(list(data.items()), columns=["Classe", "Numero"])

    with col2:
        st.header("Sentiment Analisys")
        st.subheader("Distribuzione dei commenti in base al sentiment")
        with st.container():
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(df_counts, width=900, use_container_width=False, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)

    #Dataframe contenente i commenti positivi
    df_positive = pd.read_csv(r"C:\Users\gianl\Desktop\Progetto Big Data\Intermediate Data\top_positive_comments.csv")

    #Rimozione delle colonne specificate
    df_positive = df_positive.drop(columns=["tweet_id", "created_at", "user_id_str", "hashtags", "retweet_count", "in_reply_to_screen_name", "source", "retweeted", "lang", "place_name","favorite_count"])

    df_positive.columns = ["Tweet","Nome utente","Valore Sentiment"]

    #Visualizzazione in markdown e dataframe con larghezza personalizzata
    with col2:
        with st.container():
            st.subheader("Commenti positivi")
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(df_positive, width=900, use_container_width=False, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)

    #Dataframe contenente i commenti negativi
    df_negative = pd.read_csv(r"C:\Users\gianl\Desktop\Progetto Big Data\Intermediate Data\top_negative_comments.csv")

    #Rimozione delle colonne specificate
    df_negative = df_negative.drop(columns=["tweet_id", "created_at", "user_id_str", "hashtags", "retweet_count", "in_reply_to_screen_name", "source", "retweeted", "lang", "place_name","favorite_count"])

    df_negative.columns = ["Tweet","Nome utente","Valore Sentiment"]

    with col2:
        st.subheader("Commenti negativi")
        with st.container():
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(df_negative, width=900, use_container_width=False, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)

    #Dataframe contenente i commenti neutrali
    df_neutral = pd.read_csv(r"C:\Users\gianl\Desktop\Progetto Big Data\Intermediate Data\neutral_comments.csv")

    #Rimozione delle colonne specificate
    df_neutral = df_neutral.drop(columns=["tweet_id", "created_at", "user_id_str", "hashtags", "retweet_count", "in_reply_to_screen_name", "source", "retweeted", "lang", "place_name","favorite_count"])

    df_neutral.columns = ["Tweet","Nome utente","Valore Sentiment"]

    with col2:
        st.subheader("Commenti neutri")
        with st.container():
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(df_neutral, width=900, use_container_width=False, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)
    #################################################

    #SEZIONE TOPIC MODELING
    #################################################

    #Caricamento dati
    df_topics = pd.read_csv(r"C:\Users\gianl\Desktop\Progetto Big Data\Intermediate Data\top_10_topics.csv")
    df_examples = pd.read_csv(r"C:\Users\gianl\Desktop\Progetto Big Data\Intermediate Data\top_topic_examples.csv")

    #Rimozione colonne
    df_examples = df_examples.drop(columns=[
        "tweet_id", "created_at", "user_id_str", "hashtags", "retweet_count", "favorite_count",
        "in_reply_to_screen_name", "source", "retweeted", "lang", "place_name", "tokens",
        "filtered_tokens", "features", "topicDistribution", "topic_distribution_array"
    ])

    df_topics.columns = ["Indice Topic", "Numero", "Termini caratterizanti"]
    df_examples.columns = ["Tweet", "Nome Utente", "Topic Dominante"]

    # Stile per contenitore e larghezza
    st.markdown(
        """
        <style>
        .content-container {
            max-width: 900px;
            margin: 0 auto;
            text-align: center;
        }
        .dataframe-container {
            width: 500px;
        }
        </style>
        """, unsafe_allow_html=True
    )

    with col2:
        st.header("TOPIC MODELING")
        st.subheader("Top 10 Topic")
        with st.container():
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(df_topics, width=900, use_container_width=False, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.subheader("Esempi per Topic")
        with st.container():
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(df_examples, width=900, use_container_width=False, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)
    #################################################


create_interface()