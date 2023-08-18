import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
from airflow.models import DAG
from nltk.tokenize import WordPunctTokenizer
from lxml import etree
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# les articles du site: https://www.challenge.ma/category/economie/
def site1():
    url = "https://www.challenge.ma/category/economie/"
    max_pages = 6
    current_page = 1
    Liste = []
    while current_page <= max_pages:
        currentUrl = url+"/page/"+str(current_page)+"/"
        articlesLinks = requests.get(currentUrl)
        soup = BeautifulSoup(articlesLinks.content, "html.parser")
        soup_titles = soup.find_all('h3', {"class": "vw-post-box-title"})
        for x in range(len(soup_titles)):
            try:

                lt = soup_titles[x].a['href']
                article = requests.get(lt)
                soup1 = BeautifulSoup(article.content, "html.parser")
                articleTitle = soup1.find('h1').text
                articleContent = soup1.find_all('p')
                Date = soup1.find('time').text
                author = soup1.find(
                    'a', {"class", "author-name"}).text.replace(".", "")
                l = []
                Content = ""
                for x in range(len(articleContent)):
                    if (articleContent[x].strong == None and articleContent[x].i == None):
                        Content = Content+"" + \
                            articleContent[x].text.replace(
                                "\n", "").replace("\xa0", "")

                l.append(articleTitle)
                l.append(Date)
                l.append(author)
                l.append(Content)
                Liste.append(l)
            except:
                pass
        current_page = current_page+1
    for data in Liste:
        if(data[0]=="" or data[1]=="" or data[2]=="" or data[3]=="" ):
            Liste.remove(data)
    return Liste

# les articles du site : https://www.lavieeco.com/economie


def site2():
    url = "https://www.lavieeco.com/economie"
    max_pages = 50
    current_page = 1
    Liste = []
    while current_page <= max_pages:
        currentUrl = url+"/page/"+str(current_page)+"/"
        articlesLinks = requests.get(currentUrl)
        soup = BeautifulSoup(articlesLinks.content, "html.parser")
        soup_titles = soup.find_all('div', {"class": "item-inner"})
        for x in range(len(soup_titles)):
            try:
                if(soup_titles[x].div.div.span.a.text == "Économie"):
                    l1 = soup_titles[x].find('h2')
                    l2 = l1.find('a')['href']
                    article = requests.get(l2)
                    soup1 = BeautifulSoup(article.content, "html.parser")
                    articleTitle = soup1.find('h1').span.text
                    articleContent = soup1.find(
                        'div', {"class": "entry-content clearfix single-post-content"}).find_all('p')
                    articleDate = soup1.find('time').b.text.replace(",", "")
                    articleAuthor = soup1.find(
                        'span', {"class": "post-author-name"}).b.text.replace(".", "")
                    l = []
                    Content = ""
                    for x in range(len(articleContent)):
                        Content = Content+"" + \
                            articleContent[x].text.replace(
                                "\n", "").replace("\xa0", "")
                    l.append(articleTitle)
                    l.append(articleDate)
                    l.append(articleAuthor)
                    l.append(Content)
                    Liste.append(l)
            except:
                pass
                # print("article ", x+1, " is not scrapped from page : ", current_page)
        current_page = current_page+1
    for data in Liste:
        if(data[0]=="" or data[1]=="" or data[2]=="" or data[3]=="" ):
            Liste.remove(data)
    return Liste

# les articles du site: https://lematin.ma/journal/economie/

def site3():
    url = "https://lematin.ma/journal/economie/"
    max_pages = 10
    current_page = 1
    Liste = []
    mylist = []
    while current_page <= max_pages:
        currentUrl = url+str(current_page)+"/"
        articlesLinks = requests.get(currentUrl)
        soup = BeautifulSoup(articlesLinks.content, "html.parser")
        soup_titles = soup.find_all('div', {"class": "card h-100"})
        for x in range(len(soup_titles)):
            l = soup_titles[x].a['href']
            Liste.append(l)
        current_page = current_page+1
    Liste = list(dict.fromkeys(Liste))
    for link in Liste:
        try:
            article = requests.get(link)
            soup1 = BeautifulSoup(article.content, "html.parser")
            articleTitle = soup1.find('h1', {"id": "title"}).text
            articleContent = soup1.find(
                'div', {"class": "card-body p-2"}).find_all('p')
            dom = etree.HTML(str(soup1))
            Date = soup1.find('time').getText()[0:12]
            author = dom.xpath(
                '/html/body/div[4]/div[1]/div[1]/main/article/div[2]/div[1]/p/span[1]/a[2]/text()')[0]
            l = []
            l.append(articleTitle)
            l.append(Date)
            l.append(author)
            l.append(articleContent[0].text.replace(
                "\n", "").replace("\xa0", ""))
            mylist.append(l)
        except:
            pass
    for data in mylist:
        if(data[0]=="" or data[1]=="" or data[2]=="" or data[3]=="" ):
            mylist.remove(data)
    return mylist


def scrap_data():
    lis = site1()+site2()+site3()
    df = pd.DataFrame(lis, columns=["Title", "Date", "Author", "Content"])
    df.to_excel("/datasets/data.xlsx", index=False)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 9, 10),
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=2)
}


with DAG(
    dag_id = 'scrapping_data_economic',
    default_args = default_args,
    schedule_interval = '@weekly',
) as dag:
    start = BashOperator(
    task_id = 'start_task',
    bash_command = 'echo start',
    )

    task1 = PythonOperator(
        task_id='scrapping_data',
        python_callable = scrap_data,
    )

    finish = BashOperator(
    task_id = 'finish_task',
    bash_command = 'echo finish'
    )
    start >> task1 >> finish

