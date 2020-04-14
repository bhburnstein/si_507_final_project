import requests
from bs4 import BeautifulSoup
import json
import sqlite3
from tabulate import tabulate
import plotly.graph_objects as go 

DOG = 'http://www.animalplanet.com/breed-selector/dog-breeds/all-breeds-a-z.html'
DB_NAME = 'doginfo.sqlite'

def create_db():
    conn = sqlite3.connect("doginfo.sqlite")
    cur = conn.cursor()

    drop_dogs = '''
        DROP TABLE IF EXISTS "Dogs";
    '''
    create_dogs = '''
        CREATE TABLE IF NOT EXISTS "Dogs" (
            "Id"   INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
            "Name" TEXT NOT NULL,
            "Rank"  INTEGER,
            "OriginalPastime" TEXT NOT NULL,
            "CountryId"  INTEGER NOT NULL,
            "BreedGroupId" INTEGER NOT NULL,
            "Size" TEXT,
            "Barkiness" TEXT,
            "MinLifespan" INTEGER,
            "MaxLifespan" INTEGER
        );
    '''

    drop_countries = '''
        DROP TABLE IF EXISTS "Countries";
    '''
    create_countries = '''
        CREATE TABLE IF NOT EXISTS "Countries" (
            "Id"    INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
            "Country" TEXT NOT NULL
        );
    '''

    drop_groups = '''
        DROP TABLE IF EXISTS "Groups";
    '''
    create_groups = '''
        CREATE TABLE IF NOT EXISTS "Groups" (
            "Id"    INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
            "BreedGroup" TEXT NOT NULL
        );
    '''

    cur.execute(drop_countries)
    cur.execute(drop_groups)
    cur.execute(drop_dogs)
    cur.execute(create_countries)
    cur.execute(create_groups)
    cur.execute(create_dogs)
    conn.commit()
    conn.close()

def get_dogs():
    dogs_dict = {}
    url = requests.get(DOG)
    soup = BeautifulSoup(url.text, 'html.parser')
    all_dogs_first = soup.find_all('section', id='tabAtoZ')
    dogs = all_dogs_first[0].find_all('li')
    for dog in dogs:
        for rel_path in dog('a'):
            dogs_dict[dog.text.strip()] = rel_path['href']
    return dogs_dict

def get_dog_info(dictionary):
    dog_list = []
    for k,v in dictionary.items():
        info_list = []
        info_list.append(k)
        url = requests.get(v)
        soup = BeautifulSoup(url.text, 'html.parser')
        dog_info = soup.find_all('div', class_='stats clear')
        more_info = dog_info[0].find_all(class_='right')
        for info in more_info:
            x = info.text.lower().strip()
            info_list.append(x)
            info_list = info_list[:2]
        dog_list.append(info_list)
    return dog_list

def get_more_info(dictionary):
    dog_list = []
    for v in dictionary.values():
        url = requests.get(v)
        soup = BeautifulSoup(url.text, 'html.parser')
        all_dogs_first = soup.find_all('div', class_='body divider')
        l = []
        a = []
        for i in all_dogs_first:
            j = i.text.strip()
            fast_facts = j.find('FACTS')
            just_the_facts = j[fast_facts:]
            if ':' in just_the_facts:
                k=just_the_facts.partition(':')[2]
                l.append(k)
                for line in l:
                    m=line.split('\n')
                    for n in m:
                        z=n.partition(':')[2].strip()
                        a.append(z)
                if a[1] == 'Y':
                    a.pop(1)
                a=a[1:]
                if len(a) > 7:
                    a = a[:6]
                years = a.pop(3)
                years = years.strip(' years')
                min_max = years.split('-')
                a.extend(min_max)
                dog_list.append(a)
    return dog_list

def combine_dog_lists(list_1, list_2):
    list_of_dog_lists = []
    for i, dog in enumerate(list_1):
        doggy = list_2[i]
        total_dog_list = dog + doggy
        list_of_dog_lists.append(total_dog_list)
    return list_of_dog_lists

def add_info(list_of_info):
    select_country_id_sql = '''
        SELECT Id FROM Countries
        WHERE Country = ?
    '''

    select_group_id_sql = '''
        SELECT Id FROM Groups
        WHERE BreedGroup = ?
    '''

    insert_dogs = f'''
    INSERT INTO Dogs
    VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    for dog in list_of_info:
        cur.execute(select_country_id_sql, [dog[3]])
        res = cur.fetchone()
        country_id = None
        if res is not None:
            country_id = res[0]

        cur.execute(select_group_id_sql, [dog[4]])
        grp = cur.fetchone()
        group_id = None
        if grp is not None:
            group_id = grp[0]

        cur.execute(insert_dogs, [
            dog[0],
            dog[1],
            dog[2],
            country_id,
            group_id,
            dog[5],
            dog[6],
            dog[7],
            dog[8]
        ])
    conn.commit()
    conn.close()


def populate_countries(list_of_info):
    countries = {}
    counter = 0
    for list_item in list_of_info:
        country = list_item[3]
        if country not in countries:
            counter += 1
            countries[country] = counter
        else:
            country = countries[country]
    return countries
                
def country_table(dictionary):
    insert_countries = '''
    INSERT INTO Countries
    VALUES (NULL, ?)
    '''
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    countries = []
    for k in dictionary.keys():
        countries.append(k)
    for country in countries:
        y = []
        y.append(country)
        cur.execute(insert_countries, y)
        conn.commit()
    conn.close()

def populate_breed_groups(list_of_info):
    groups = {}
    counter = 0
    for list_item in list_of_info:
        group = list_item[4]
        if group not in groups:
            # if group == 'Working Dog':
            #     group = 'Working'
            counter += 1
            groups[group] = counter
        else:
            group = groups[group]
    return groups

def group_table(dictionary):
    insert_groups = '''
    INSERT INTO Groups
    VALUES (NULL, ?)
    '''
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    groups = []
    for k in dictionary.keys():
        groups.append(k)
    for group in groups:
        y = []
        y.append(group)
        cur.execute(insert_groups, y)
        conn.commit()
    conn.close()


def basic_query_sql(command):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    limit = 'LIMIT 10'
    # sort = 'DESC'
    query = f'''
    SELECT Name, Rank, C.Country, G.BreedGroup, Size, MinLifeSpan, MaxLifeSpan FROM DOGS AS D
    JOIN Countries AS C ON D.CountryId=C.Id JOIN Groups as G on D.BreedGroupId=G.Id
    {limit}
    '''
    cur.execute(query)
    results = cur.fetchall()
    return results

def more_query_sql(command, agg):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    limit = 'LIMIT 10'
    sort = 'DESC'
    if command == 'group':
        join_and_group = 'JOIN Groups AS G ON D.BreedGroupId=G.Id GROUP BY G.BreedGroup'
        select = 'G.BreedGroup'
    elif command == 'origin':
        join_and_group = 'JOIN Countries AS C ON D.BreedGroupId=C.Id GROUP BY C.Country'
        select = 'C.Country'
    if agg == 'rank':
        order_by = 'AVG(Rank)'
        sort = 'ASC'
    elif agg == 'max':
        order_by = 'AVG(MaxLifeSpan)'
    elif agg == 'min':
        order_by = 'AVG(MinLifeSpan)'
    query = f'''
    SELECT {select}, Avg(Rank), AVG(MinLifeSpan), AVG(MaxLifeSpan) FROM Dogs AS D
    {join_and_group} ORDER BY {order_by} {sort} {limit}
    '''
    cur.execute(query)
    results = cur.fetchall()
    return results

def plot_results(results, agg):
    xaxis = []
    yaxis = []
    for i in results:
        xaxis.append(i[0])
        if agg == 'rank':
            yaxis.append(i[1])
        elif agg == 'min':
            yaxis.append(i[2])
        elif agg == 'max':
            yaxis.append(i[3])
    bar_data = go.Bar(x=xaxis, y=yaxis)
    basic_layout = go.Layout(title="")
    fig = go.Figure(data=bar_data, layout=basic_layout)
    fig.write_html("bar.html", auto_open=True)

if __name__ == "__main__":
    print("Creating database of dogs...\nPlease sit for your treat...")
    create_db()
    doggydict = get_dogs()
    doggylist = get_dog_info(doggydict)
    more_list = get_more_info(doggydict)
    combined_info = combine_dog_lists(doggylist, more_list)
    countries = populate_countries(combined_info)
    country_table(countries)
    groups = populate_breed_groups(combined_info)
    group_table(groups)
    add_info(combined_info) # works
    while True:
        message = input("Please select 'dog', 'info', or 'exit': ").lower().strip()
        if message == 'exit':
            print("I hope to play with you soon!")
            exit()
        elif message == 'dog':
            pass
        elif message == 'info':
            search = input("You can search by 'origin', 'group', 'none': ").lower().strip()
            if search == 'origin' or search == 'group':
                rank = input("Order results by 'rank', 'min', or 'max': ").lower().strip()
                results = more_query_sql(search, rank)
                headers = [f'{search}'.capitalize(), 'AKC Rank', 'Min Life Span', 'Max Life Span']
                print(tabulate(results, headers, tablefmt="simple", floatfmt=".2f"))
                plot = input("Do you want to plot? 'y' or 'n': ").lower().strip()
                if plot == 'n':
                    continue
                elif plot == 'y':
                    plot_results(results, rank)
                else:
                    print("*cocks head* I didn't understand that command.")
            elif search == 'none':
                results = basic_query_sql(search)
                print(tabulate(results, tablefmt="simple"))
            else:
                print("*cocks head* I didn't understand that command.")
        else:
            print("*cocks head* I didn't understand that command.")
    
