import requests
from bs4 import BeautifulSoup
import json
import sqlite3
import plotly.graph_objects as go 
from flask import Flask, render_template, request

app = Flask(__name__)

DOG = 'http://www.animalplanet.com/breed-selector/dog-breeds/all-breeds-a-z.html'
DB_NAME = 'doginfo.sqlite'

CACHE_FILE_NAME = 'cache.json'
CACHE_DICT = {}

def create_db():
    '''Creates a SQL database and tables.
    
    Parameters
    ----------
    None
    
    Returns
    -------
    None
    '''
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
    '''Creates a dictionary of dogs from url
    and their associated url by scraping.
    
    Parameters
    ----------
    None
    
    Returns
    -------
    dict
        the results of the scraping with a dog breed
        as a key and the url as the value
    '''
    dogs_dict = {}
    dogs = make_url_request_using_cache(DOG, CACHE_DICT) # throwing stick
    soup = BeautifulSoup(dogs, 'html.parser')
    all_dogs_first = soup.find_all('section', id='tabAtoZ')
    dogs = all_dogs_first[0].find_all('li')
    for dog in dogs:
        for rel_path in dog('a'):
            dogs_dict[dog.text.strip()] = rel_path['href']
    return dogs_dict

def get_dog_info(dictionary):
    '''Creates a list of records associated with each dog.
    
    Parameters
    ----------
    dictionary: dict
        The dictionary containing the urls to scrape and crawl.
    
    Returns
    -------
    list
        Records from each dog in list format.
    '''
    dog_list = []
    for k,v in dictionary.items():
        info_list = []
        info_list.append(k)
        url = make_url_request_using_cache(v, CACHE_DICT) # retrieving stick (1)
        soup = BeautifulSoup(url, 'html.parser')
        dog_info = soup.find_all('div', class_='stats clear')
        more_info = dog_info[0].find_all(class_='right')
        for info in more_info:
            x = info.text.lower().strip()
            info_list.append(x)
            info_list = info_list[:2]
        dog_list.append(info_list)
    return dog_list

def get_more_info(dictionary):
    '''Creates a list of records associated with each dog.
    Uses a different html tag to find these records, so a 
    second function needed.
    
    Parameters
    ----------
    dictionary: dict
        The dictionary containing the urls to scrape and crawl.
    
    Returns
    -------
    list
        Records from each dog in list format.
    '''
    dog_list = []
    for v in dictionary.values():
        url = make_url_request_using_cache(v, CACHE_DICT) # retrieving stick(2)
        soup = BeautifulSoup(url, 'html.parser')
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
                if a[2] == 'Working Dog':
                    a[2] = 'Working' # clean up breed groups
                if a[1] == 'Herding':
                    a[1] = 'Hungary' # clean up origins
                punc = [',', '/', '&']
                for mark in punc:
                    if mark in a[1]:
                        country = a[1].split(mark)
                        a[1] = country[0].strip() # clean up origins
                if a[1] == 'Border of Scotland and England':
                    a[1] = 'Scotland'
                years = a.pop(3)
                years = years.strip(' years')
                min_max = years.split('-')
                a.extend(min_max)
                dog_list.append(a)
    return dog_list

def combine_dog_lists(list_1, list_2):
    '''Combines two lists of dog inforamtion.
    
    Parameters
    ----------
    list_1: list
        List of dog information.
    
    list_2: list
        List of dog information.
    
    Returns
    -------
    list
        Records from each dog in list format.
    '''
    list_of_dog_lists = []
    for i, dog in enumerate(list_1):
        doggy = list_2[i]
        total_dog_list = dog + doggy
        list_of_dog_lists.append(total_dog_list)
    return list_of_dog_lists

def add_info(list_of_info):
    '''Adds records to the dogs table in the SQL database.
    
    Parameters
    ----------
    list_of_info: list
        A list containing all the information for each dog.
    
    Returns
    -------
    None
    '''
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
    '''Creates a dictionary with a country as the key
    and a number as the value to be used as a foreign key.
    
    Parameters
    ----------
    list_of_info: list
        The complete list of dog information.
    
    Returns
    -------
    dictionary
        Country-Id in a key-value pair.
    '''
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
    '''Adds records to the countries table in the SQL database.
    
    Parameters
    ----------
    dictionary: dictionary
       A dictionary of the country and id as key-value pairs.
    
    Returns
    -------
    None
    '''
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
    '''Creates a dictionary with a breed group as the key
    and a number as the value to be used as a foreign key.
    
    Parameters
    ----------
    list_of_info: list
        The complete list of dog information.
    
    Returns
    -------
    dictionary
        Breed group-Id in a key-value pair.
    '''
    groups = {}
    counter = 0
    for list_item in list_of_info:
        group = list_item[4]
        if group not in groups:
            if group == 'Working Dog':
                group = 'Working'
                # working dog was listed as one of the 
                # groups. this sets it as just 'working'
            counter += 1
            groups[group] = counter
        else:
            group = groups[group]
    return groups

def group_table(dictionary):
    '''Adds records to the groups table in the SQL database.
    
    Parameters
    ----------
    dictionary: dictionary
       A dictionary of the breed group and id as key-value pairs.
    
    Returns
    -------
    None
    '''
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

def load_cache(): 
    ''' Opens the cache file if it exists and loads the JSON into
    the CACHE_DICT dictionary.
    if the cache file doesn't exist, creates a new cache dictionary
    
    Parameters
    ----------
    None
    
    Returns
    -------
    The opened cache: dict
    '''
    try:
        cache_file = open(CACHE_FILE_NAME, 'r')
        cache_file_contents = cache_file.read()
        cache = json.loads(cache_file_contents)
        cache_file.close()
    except:
        cache = {}
    return cache


def save_cache(cache):
    ''' Saves the current state of the cache to disk
    
    Parameters
    ----------
    cache_dict: dict
        The dictionary to save
    
    Returns
    -------
    None
    '''
    cache_file = open(CACHE_FILE_NAME, 'w')
    contents_to_write = json.dumps(cache)
    cache_file.write(contents_to_write)
    cache_file.close()


def make_url_request_using_cache(url, cache):
    '''Check the cache for a saved result for the unique key for a url scrape. 
    If the result is found, return it. Otherwise send a new 
    request, save it, then return it.
    
    Parameters
    ----------
    url: string
        The URL for the scrape.
    cache:
        The json file used to save searches.
    
    Returns
    -------
    dict
        the results of the query as a dictionary loaded from cache
        JSON
    '''
    if (url in cache.keys()): 
        print("Retrieving stick")
        return cache[url]    
    else:
        print("Throwing stick")
        response = requests.get(url) 
        cache[url] = response.text
        save_cache(cache)          
        return cache[url] 

def get_group_results_sql(group_by, sort_order, sort_by):
    '''Constructs a SQL query when the user searches by grouping.
    
    Parameters
    ----------
    group_by: string
        String representation of html form. What the user
        wants to group by.
    sort_order: string
        String representation of html form. How the user
        wants to sort data.
    sort_by: string
        String representation of html form. Sort by
        numerical data.

    Returns
    -------
    tuple
        the results of the query as a nested tuple.
    '''
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    limit = 'LIMIT 10'
    if group_by == 'breed group':
        join_and_group = 'JOIN Groups AS G ON D.BreedGroupId=G.Id GROUP BY G.BreedGroup'
        select = 'G.BreedGroup'
    elif group_by == 'origin':
        join_and_group = 'JOIN Countries AS C ON D.CountryId=C.Id GROUP BY C.Country'
        select = 'C.Country'
    elif group_by == 'size':
        select = 'D.Size'
        join_and_group = 'GROUP BY Size'
    elif group_by == 'barkiness':
        select = 'D.Barkiness'
        join_and_group = 'GROUP BY Barkiness'
    if sort_by == 'rank':
        sort_by = 'Rank'
    elif sort_by == 'max_life':
        sort_by = 'MaxLifeSpan'
    elif sort_by == 'min_life':
        sort_by = 'MinLifeSpan'
    elif sort_by == 'number':
        sort_by = 'Number'
    query = f'''
    SELECT {select},  COUNT(DISTINCT Name) AS Number, ROUND(AVG(Rank), 2) AS Rank, ROUND(AVG(MinLifeSpan), 2) AS MinLifeSpan, 
    ROUND(AVG(MaxLifeSpan), 2) AS MaxLifeSpan FROM Dogs AS D
    {join_and_group} HAVING {sort_by} <> 'n/a' ORDER BY {sort_by} {sort_order} {limit}
    '''
    results = cur.execute(query).fetchall()
    conn.close()
    return results

def get_dog_results_sql(sort_by, sort_order, region, size, breed_group, bark, limit):
    '''Constructs a SQL query when the user searches by dog.
    
    Parameters
    ----------
    sort_by: string
        String representation of html form. Sort by
        numerical data.
    sort_order: string
        String representation of html form. How the user
        wants to sort data.
    region: string
        String representation of html form. Filter by
        region of origion.
    size: string
        String representation of html form. Filter by dog size.
    breed_group: string
        String representation of html form. Filter by breed group.
    bark: string
        String representation of html form. Filter by noise level.
    limit: int
        Integer representing number of rows that the user requests.

    Returns
    -------
    tuple
        the results of the query as a nested tuple.
    '''
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    if sort_by == 'rank':
        sort_by = 'Rank'
    elif sort_by == 'max_life':
        sort_by = 'MaxLifeSpan'
    elif sort_by == 'min_life':
        sort_by = 'MinLifeSpan'
    if region == 'All':
        region = ''
        if size == 'All':
            size = ''
            if breed_group == 'All':
                breed_group = ''
                if bark == 'All bark levels':
                    bark = ''
                else:
                    bark = f"WHERE Barkiness='{bark}'"
            else:
                breed_group = f"WHERE BreedGroup='{breed_group}'"
                if bark == 'All bark levels':
                    bark = ''
                else:
                    bark = f"AND Barkiness='{bark}'"
        else:
            size = f"WHERE Size='{size}'"
            if breed_group == 'All':
                breed_group = ''
            else:
                breed_group = f"AND BreedGroup='{breed_group}'"
            if bark == 'All bark levels':
                bark = ''
            else:
                bark = f"AND Barkiness='{bark}'"
    else:
        region = f"WHERE Country='{region}'"
        if size == 'All':
            size = ''
        else:
            size = f"AND Size='{size}'"
        if breed_group == 'All':
            breed_group = ''
        else:
            breed_group = f" AND BreedGroup='{breed_group}'"
        if bark == 'All bark levels':
            bark = ''
        else:
            bark = f" AND Barkiness='{bark}'"
    if limit:
        limit = limit
    else:
        limit = 10
    query = f'''
    SELECT Name, Rank, C.Country, G.BreedGroup, Size, Barkiness, MinLifeSpan, MaxLifeSpan FROM DOGS AS D
    JOIN Countries AS C ON D.CountryId=C.Id JOIN Groups as G on D.BreedGroupId=G.Id
    {region} {size} {breed_group} {bark} ORDER BY {sort_by} {sort_order} LIMIT {limit}
    '''
    results = cur.execute(query).fetchall()
    conn.close()
    return results

def get_barkiness():
    '''Uses SQL database to find all unique instances
    in a column.
    
    Parameters
    ----------
    None

    Returns
    -------
    list
        list of all bark levels.
    '''
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    query = '''
    SELECT DISTINCT(Barkiness) FROM Dogs
    '''
    results = cur.execute(query).fetchall()
    conn.close()
    bark_list = []
    for item in results:
        for x in item:
            bark_list.append(x)
    bark_list.sort()
    return bark_list

def get_sizes():
    '''Uses SQL database to find all unique instances
    in a column.
    
    Parameters
    ----------
    None

    Returns
    -------
    list
        list of all dog sizes.
    '''
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    query = '''
    SELECT DISTINCT(Size) FROM Dogs
    '''
    results = cur.execute(query).fetchall()
    conn.close()
    size_list = []
    for item in results:
        for x in item:
            size_list.append(x)
    size_list.sort()
    return size_list

def get_breedgroups():
    '''Uses SQL database to find all unique instances
    in a column.
    
    Parameters
    ----------
    None

    Returns
    -------
    list
        list of all breed groups.
    '''
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    query = '''
    SELECT DISTINCT(BreedGroup) FROM Groups
    '''
    results = cur.execute(query).fetchall()
    conn.close()
    group_list = []
    for item in results:
        for x in item:
            group_list.append(x)
    group_list.sort()
    return group_list

def get_countries():
    '''Uses SQL database to find all unique instances
    in a column.
    
    Parameters
    ----------
    None

    Returns
    -------
    list
        list of all origins.
    '''
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    query = '''
    SELECT DISTINCT(Country) FROM Countries
    '''
    results = cur.execute(query).fetchall()
    conn.close()
    country_list = []
    for item in results:
        for x in item:
            country_list.append(x)
    country_list.sort()
    return country_list

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/dogs')
def dogs():
    bark_list = get_barkiness()
    size_list = get_sizes()
    group_list = get_breedgroups()
    country_list = get_countries()
    return render_template('dogs.html', bark_list=bark_list, size_list=size_list,
            group_list=group_list, country_list=country_list)

@app.route('/groupings')
def groupings():
    return render_template('groupings.html')

@app.route('/doggos', methods=['POST'])
def doggos():
    sort_by = request.form['sort']
    sort_order = request.form['dir']
    region = request.form['region']
    size = request.form['size']
    breed_group = request.form['breed_group']
    bark = request.form['barkiness']
    limit = request.form['limit']
    results = get_dog_results_sql(sort_by, sort_order, region, size, breed_group, bark, limit)
    headers = ['Dog Breed', 'Rank', 'Origin', 'Breed Group', 'Size', 'Barkiness', 'Min Life Span', 'Max Life Span']

    plot_results = request.form.get('plot', False)
    if (plot_results):
        x_vals = [r[0] for r in results]
        if sort_by == 'rank':
            y_vals = [r[1] for r in results]
        elif sort_by == 'min_life':
            y_vals = [r[6] for r in results]
        else:
            y_vals = [r[7] for r in results]
        bars_data = go.Bar(
            x=x_vals,
            y=y_vals
        )
        fig = go.Figure(data=bars_data)
        div = fig.to_html(full_html=False)
        return render_template("plot.html", plot_div=div)
    elif len(results) == 0:
        return render_template('ohno.html')
    else:
        return render_template('doggos.html', results=results, headers=headers, doggydict=doggydict)

@app.route('/results', methods=['POST'])
def group_results():
    group_by = request.form['group']
    sort_order = request.form['dir']
    sort_by = request.form['sort']
    results = get_group_results_sql(group_by, sort_order, sort_by)
    headers = [f'{group_by}'.capitalize(), 'Number of Dogs', 'AKC Rank', 'Min Life Span', 'Max Life Span']

    plot_results = request.form.get('plot', False)
    if (plot_results):
        x_vals = [r[0] for r in results]
        if sort_by == 'rank':
            y_vals = [r[2] for r in results]
        elif sort_by == 'number':
            y_vals = [r[1] for r in results]
        elif sort_by == 'min_life':
            y_vals = [r[3] for r in results]
        else:
            y_vals = [r[4] for r in results]
        bars_data = go.Bar(
            x=x_vals,
            y=y_vals
        )
        fig = go.Figure(data=bars_data)
        div = fig.to_html(full_html=False)
        return render_template("plot.html", plot_div=div)
    else:
        return render_template('results.html', results=results, headers=headers)


if __name__ == '__main__':
    CACHE_DICT = load_cache()
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
    add_info(combined_info) 
    app.run(debug=True)