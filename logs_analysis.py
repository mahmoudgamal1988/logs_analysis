#!/usr/bin/env python3
import psycopg2


# What are the most popular three articles of all time?
q1_title = ("What are the most popular three articles of all time?")
q_1 = ("select title, count(*) as views from articles join\
    log on concat('/article/', articles.slug) = log.path\
    where log.status like '%200%'\
    group by log.path, articles.title order by views desc limit 3")

# Who are the most popular article authors of all time?
q2_title = ("Who are the most popular article authors of all time?")
q_2 = ("select authors.name, count(*) as views from articles join\
    authors on articles.author = authors.id join\
    log on concat('/article/', articles.slug) = log.path where\
    log.status like '%200%' group by authors.name order by views desc")

# On which days did more than 1% of requests lead to errors
q3_title = ("On which days did more than 1% of requests lead to errors?")
q_3 = ("select * from (\
    select a.day,\
    round(cast((100*b.hits) as numeric) / cast(a.hits as numeric), 2)\
    as perc from\
    (select date(time) as day, count(*) as hits from log group by day) as a\
    join\
    (select date(time) as day, count(*) as hits from log where status\
    like '%404%' group by day) as b\
    on a.day = b.day)\
    as t where perc >= 1.0")


def get_query(query):
    conn = psycopg2.connect("dbname='news'")
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    conn.close()
    return results

res1 = get_query(q_1)
res2 = get_query(q_2)
res3 = get_query(q_3)


def print_query_results(qres):
    for i in range(len(qres)):
        t = qres[i][0]
        res = qres[i][1]
        print("\t" + "%s - %d" % (t, res) + " views")
    print("\n")


def print_error_results(err_results):
    for i in range(len(err_results)):
        d=err_results[i][0]
        perc=err_results[i][1]
        print("%s-%.1f %%" %(d,perc))
    print("\n")



print (q1_title)
print_query_results(res1)
print(q2_title)
print_query_results(res2)
print(q3_title)
print_error_results(res3)
print("============================")
print("End Of Results")
