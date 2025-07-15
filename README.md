
# 🐦 Twitter Tracker

**Analisi dei Tweet nel periodo pre-elezioni USA 2020**  
Progetto sviluppato da: **Gianluca Ferrari**  

## 📄 Descrizione

Twitter Tracker è un progetto di analisi dei dati pubblicati su Twitter durante il mese di ottobre 2020, in prossimità delle elezioni presidenziali USA. L’obiettivo è estrarre insight dal comportamento degli utenti attraverso query, sentiment analysis, topic modeling e visualizzazioni interattive.

---

## 📊 Dataset

- **Dimensione**: 31,7 GB
- **Formato**: 31 file `.csv` (uno per ogni giorno di ottobre 2020)
- **Totale Tweet iniziali**: ~88 milioni  
- **Tweet dopo la pulizia**: ~83 milioni (rimozione del 4,81%)

### ✨ Attributi principali:
- `tweet_id`, `created_at`, `user_id_str`, `text`, `hashtags`, `retweet_count`, `favorite_count`, `retweeted`, `lang`, `place_name`, `screen_name`, ...

> Alcuni attributi (es. `location`, `place_lat`, `place_lon`) sono stati rimossi per inconsistenza o valore nullo.

---

## ⚙️ Stack Tecnologico

- **Python**
- **Apache Spark + PySpark**
- **NLTK + VADER** (Sentiment Analysis)
- **MLlib (Spark)** (Topic Modeling con LDA)
- **Streamlit** (Interfaccia utente)

---

## 🔍 Funzionalità

### 📈 Query e Metriche
- Numero totale di tweet, utenti unici, hashtag
- Top 10 utenti per tweet, like, retweet, engagement
- Tweet per giorno, lingua, luogo
- Estrazione tweet per utente, parola chiave o hashtag

### 😄 Sentiment Analysis
- Preprocessing: rimozione retweet, menzioni, URL, lowercasing
- Modello: `SentimentIntensityAnalyzer` di VADER
- Output: classificazione tweet come positivi, neutrali o negativi

### 🧠 Topic Modeling
- Preprocessing: filtraggio per lingua inglese, rimozione stopword, tokenizzazione
- Modello: LDA con 25 topic e 500.000 tweet randomici
- Output: distribuzione dei topic + top tweet rappresentativi

### 💻 Interfaccia (Streamlit)
- Visualizzazione risultati di query, sentiment e topic
- I dati sono pre-processati e salvati in file intermedi

---

## 🚀 Avvio del progetto

1. Clona il repository  
   ```bash
   git clone https://github.com/tuo-username/twitter-tracker.git
   cd twitter-tracker
   ```

2. Installa le dipendenze  
   ```bash
   pip install -r requirements.txt
   ```

3. Avvia l'interfaccia Streamlit  
   ```bash
   streamlit run app.py
   ```

> ⚠️ Il dataset completo non è incluso nel repository per motivi di spazio. Può essere richiesto separatamente o simulato tramite subset.

---

## 📁 Struttura del progetto

```text
twitter-tracker/
│
├── data/                  # File CSV intermedi
├── notebooks/             # Notebook di analisi
├── scripts/               # Script PySpark e NLP
├── app.py                 # Applicazione Streamlit
├── requirements.txt       # Dipendenze Python
└── README.md              # Questo file
```

---

## 📜 Licenza

Questo progetto è distribuito con licenza **MIT**. Vedi il file `LICENSE` per maggiori dettagli.

---

## 🤝 Ringraziamenti

- NLTK & VADER team per gli strumenti NLP
- Apache Spark per l’elaborazione distribuita
- Streamlit per la semplicità dell’interfaccia

---

## 📬 Contatti

Per domande o suggerimenti:

**Gianluca Ferrari**  
📧 Email: gianlucaferrari2000@gmail.com
