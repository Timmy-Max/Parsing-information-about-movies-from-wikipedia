import wikipediaapi
from datetime import datetime

from data import clear_data
from parser import extract_film_data, create_and_save_dataset

# Wiki object
wiki_wiki = wikipediaapi.Wikipedia(
    user_agent="MyProjectName (merlin@example.com)",
    language="ru",
    extract_format=wikipediaapi.ExtractFormat.WIKI,
)

# Category names
categories = [
    "Категория:Фильмы_по_алфавиту",
    "Категория:Мультфильмы_по_алфавиту",
    "Категория:Телефильмы_по_алфавиту",
    "Категория:Телесериалы_по_алфавиту",
    "Категория:Мультсериалы_по_алфавиту",
]

# Type Names
type_names = [
    "film",
    "animated film",
    "tv film",
    "tv series",
    "animated series",
]

# Defining variables for data storage
all_titles = []
all_types_names = []
all_urls = []
all_summaries = []
all_plots = []

# Sorting through the categories and extracting data from each film contained in them

# Starting the timer
start_time = datetime.now()

for ind, category in enumerate(categories):
    cat = wiki_wiki.page(category)
    new_film_data = extract_film_data(cat.categorymembers, type_names[ind])
    all_titles += new_film_data[0]
    all_types_names += new_film_data[1]
    all_urls += new_film_data[2]
    all_summaries += new_film_data[3]
    all_plots += new_film_data[4]

elapsed_time = (datetime.now() - start_time).seconds
mins, secs = divmod(elapsed_time, 60)
hours, mins = divmod(mins, 60)
print(f"Elapsed time: {hours} h {mins} min {secs} sec")

# Creating dataset
data = create_and_save_dataset(
    all_titles,
    all_types_names,
    all_urls,
    all_summaries,
    all_plots,
)

# Clearing data
data = clear_data(data)

# Exporting a data frame to csv format
data.to_csv("films_wiki_data_in_russian.csv", index=False)
