from typing import Union, Tuple, List, Any

import wikipediaapi
import pandas as pd
import re

from tqdm import tqdm
from imdbparser import IMDb
from pandas.io.html import read_html


def extract_infobox(url: str) -> pd.DataFrame:
    """
    Extracts the infobox from the wiki page and presents it in Pandas dataframe format.

    Parameters
    ----------
    url : str
        Link to wikipedia page.

    Returns
    -------
    Pandas DataFrame
        Pandas DataFrame with wiki infobox.
    """
    # If the exception is triggered, it will return "not found"
    infobox = "not found"
    try:
        infobox = read_html(url, attrs={"class": "infobox"})[0]
    except ValueError:
        pass

    return infobox


def extract_from_infobox(infobox) -> str:
    """
    Retrieves the IMDb ID and genres in infobox.

    Parameters
    ----------
    infobox : pd.DataFrame
        Pandas DataFrame with wiki infobox.

    Returns
    -------
    genre, imdb_id : str, str
        Strings with genres and id or empty strings.
    """
    genre = ""
    imdb_id = ""
    if infobox.shape[1] == 2:
        col_1, col_2 = infobox.columns

        try:
            genre = infobox.loc["Жанр" == infobox[col_1], col_2].values[0]
        except (ValueError, IndexError):
            pass

        try:
            imdb_id = infobox.loc["IMDb" == infobox[col_1], col_2].values[0]
        except (ValueError, IndexError):
            pass

        # Extracting IMDb id
        if imdb_id != "":
            matches = re.findall(r"\d+", imdb_id)
            imdb_id = matches[0] if len(imdb_id) > 0 else ""

    return genre, imdb_id


def extract_imdb_rating(imdb_id) -> Union[int, str]:
    """
    Extracts the rating from the movie page on IMDb.

    Parameters
    ----------
    imdb_id : str
        String with IMDb id.

    Returns
    -------
    rating : str, str
        Float value of rating or "no info".
    """
    imdb = IMDb()
    try:
        # Extracting movie rating
        movie = imdb.get_movie(imdb_id)
        movie.fetch()
        rating = movie.__dict__.get("rating", "no info")
        rating = "no info" if rating == 0 else rating
    except IndexError:
        rating = "no info"

    try:
        return float(rating)
    except ValueError:
        return "no info"


def extract_film_data(category_members: Any, type_name: str) -> Tuple[List[str], ...]:
    """
    Extracts films titles, types_names, urls, summaries, plots from wiki.

    Parameters
    ----------
    category_members : Any
        String with IMDb id.
    type_name : str
        The name of the category that will be parsed.

    Returns
    -------
    tuple
        Tuple with lists of titles, types_names, urls, summaries, plots.
    """
    titles = []
    types_names = []
    urls = []
    summaries = []
    plots = []

    i = 0
    cat_members_values = list(category_members.values())
    n = len(cat_members_values)
    with tqdm(total=n) as pbar:
        while i < n:
            page = cat_members_values[i]
            # If page is not a subcategory
            if page.ns != wikipediaapi.Namespace.CATEGORY:
                exception_flag = False
            try:
                title = page.title
                url = page.fullurl
                summary = page.summary

                # Looking for a section with a plot
                plot_section = page.section_by_title("Сюжет")

                # "no info", if not found
                if plot_section is None:
                    plot = "no info"
                else:
                    plot = plot_section.text
            except RuntimeError:
                exception_flag = True

            if exception_flag:
                continue
            else:
                titles.append(title)
                types_names.append(type_name)
                urls.append(url)
                summaries.append(summary)
                plots.append(plot)
                i += 1
                pbar.update(1)

    return titles, types_names, urls, summaries, plots


def extract_info_by_url(url: str) -> Tuple[List[str], ...]:
    """
    Extracts genre and rating from wiki page.

    Parameters
    ----------
    url : str
        Link to wikipedia page.

    Returns
    -------
    tuple
        Tuple with lists of genres and ratings.
    """
    # Getting an infobox
    infobox = extract_infobox(url)

    # If the table exists, then imdb_id and genres are extracted
    if type(infobox) != pd.core.frame.DataFrame:
        genre, imdb_id = "no info", ""
    else:
        genre, imdb_id = extract_from_infobox(infobox)
        # Removing numbers and characters from a string
        genre = "".join(char for char in genre if char.isalpha() or char.isspace())

    rating = extract_imdb_rating(imdb_id)
    return genre, rating


def create_and_save_dataset(
    titles: List[str],
    types_names: List[str],
    urls: List[str],
    summaries: List[str],
    plots: List[str],
    path: str = "films_data_in_russian.csv",
):
    """
    Extracts genre and rating from wiki page.

    Parameters
    ----------
    titles : list
        List with titles.
    types_names : list
        List with type names like film, tv series, etc.
    urls : list
        List with links to Wikipedia pages.
    summaries : list
        List with summaries.
    plots : list
        List with plots.
    path : str
        The path to save data.

    Returns
    -------
    Pandas DataFrame
        DataFrame with films data.
    """
    data = pd.DataFrame(
        {
            "title": titles,
            "type": types_names,
            "url": urls,
            "summary": summaries,
            "plot": plots,
        }
    )
    genres = []
    ratings = []
    # Extracting genres and ratings
    with tqdm(total=data.shape[0]) as pbar:
        for url in data["url"].values:
            genre, rating = extract_info_by_url(url)
            genres.append(genre)
            ratings.append(rating)
            pbar.update(1)

    # Adding additional columns to the DataFrame
    data.insert(1, "genre", genres)
    data.insert(2, "imdb_rating", ratings)

    # Exporting a data frame to csv format
    data.to_csv(path, index=False)

    return data
