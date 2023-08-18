import datetime
import psycopg2
import pandas as pd
from airflow.models import DAG
from nltk.tokenize import WordPunctTokenizer
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

def addAuthor(name):
    conn = psycopg2.connect(database="postgres", user="airflow",
                            password="airflow", host="postgres", port="5432")
    cur = conn.cursor()
    cur.execute(
        f"SELECT idAuth FROM author WHERE name = '{name}'")
    id = cur.fetchone()
    if(id == None):
        sql = "INSERT INTO author (name) VALUES (%s) RETURNING idAuth"
        cur.execute(sql, (name,))
        id = cur.fetchone()[0]
        conn.commit()
    else:
        id = id[0]
    cur.close()
    conn.close()
    return id


def addArticle(title, content, idAuth):
    conn = psycopg2.connect(database="postgres", user="airflow",
                            password="airflow", host="postgres", port="5432")
    cur = conn.cursor()
    sql = "INSERT INTO article (title,content,idAuth) VALUES (%s,%s,%s) RETURNING idArt"
    val = (title, content, idAuth)
    cur.execute(sql, val)
    id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return id


def addTime(jour, mois, annee):
    conn = psycopg2.connect(database="postgres", user="airflow",
                            password="airflow", host="postgres", port="5432")
    cur = conn.cursor()
    id = 0
    cur.execute(
        f"SELECT idTemps FROM temps WHERE jour= {jour} AND mois = '{mois}' AND annee = {annee}")
    id = cur.fetchone()
    if(id == None):
        sql = "INSERT INTO temps (jour,mois,annee) VALUES (%s,%s,%s) RETURNING idTemps"
        val = (jour, mois, annee)
        cur.execute(sql, val)
        id = cur.fetchone()[0]
        conn.commit()
    else:
        id = id[0]
    cur.close()
    conn.close()
    return id


def addKeyword(idart, idtime, tf):
    conn = psycopg2.connect(database="postgres", user="airflow",
                            password="airflow", host="postgres", port="5432")
    cur = conn.cursor()
    cur.execute(
        f"SELECT * FROM keyword WHERE idArt = {idart}")
    if(cur.fetchone() == None):
        sql = "INSERT INTO keyword (idArt,idTemps,tf) VALUES (%s,%s,%s) "
        val = (idart, idtime, tf)
        cur.execute(sql, val)
        conn.commit()
    else:
        pass
    cur.close()
    conn.close()
    return "ok"


def TF(document, word):
    N = len(document)
    oc = len([w for w in document if w == word])
    return oc/N


def get_data():
    data = pd.DataFrame(pd.read_excel("/datasets/dataSet.xlsx"))
    for i in range(len(data)):
        line = data.iloc[i]
        author_name = line["Author"]
        idAuth = addAuthor(author_name)
        artTile = line["Title"]
        artContent = line["Content"]
        idArt = addArticle(artTile, artContent, idAuth)
        date = line["Date"].split()
        idTemps = addTime(int(date[0]), date[1], int(date[2]))
        # charger les stop words
        sw = pd.DataFrame(pd.read_excel("/datasets/french.xlsx"))
        ls = []
        for i in range(len(sw)):
            ls.append(sw["words"].iloc[i])
        text = artContent.lower()
        tokens = WordPunctTokenizer().tokenize(text)
        tokens = [word for word in tokens if (word not in ls and not word.isdigit())]
        for j in range(len(tokens)):
            kw = tokens[j]
            tf = TF(tokens, kw)
            addKeyword(idArt, idTemps, tf)
    print("c'est fait !!!")
    return "ok"



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 9, 10),
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=2)
}


with DAG(
    dag_id = 'data_warehousing',
    default_args = default_args,
    schedule_interval = '@weekly',
) as dag:
    start = BashOperator(
    task_id = 'start_task',
    bash_command = 'echo start',
    )

    task1 = PythonOperator(
        task_id='ETLs_Data-warehousing',
        python_callable = get_data,
    )

    finish = BashOperator(
    task_id = 'finish_task',
    bash_command = 'echo finish'
    )
    start >> task1 >> finish
