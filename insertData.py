import numpy as np
import pandas as pd
from py2neo import Graph
from tqdm import tqdm
from connector import connect
import os
import sys
import re
import traceback
import configparser
from datetime import datetime



class DataTransfer:

    def __init__(self):
        self.driver = connect()
        self.config = configparser.ConfigParser()
        self.config.read("config.ini")


    def createNode(self, data, label):

        """
        -------------------------------------------------
        Description : Function to create Nodes to GraphDb

        Parameters : 
        @param data -- a python list containing data as dict
        @param label -- a string value

        Return :
        None
        ---------------------------------------------------
        """

        create = [self.config["NODES"]["CREATE"]]
        create = [i == "TRUE" for i in create][0]

        if create:
            print("Creating Nodes in Neo4j 🛠️")

            try:
                with tqdm(total=len(data)) as pbar:
                    for i, datum in enumerate(data):
                        keys = datum.keys()

                        _query = f"CREATE ({label[0]}{i}:{label}) SET "
                        for k in keys:
                            if type(datum[k]) == str:
                                _query += f'{label[0]}{i}.{k} = "{datum[k]}",'
                            else:
                                _query += f'{label[0]}{i}.{k} = {datum[k]},'
                        
                        _query = _query.strip(",")
                        self.driver.run(_query)
                        pbar.update(1)
            
            except Exception:
                print("Error Occured ⛔ ⛔")
                print(traceback.print_exc())

        else:
            print("Forbidden ❌ for creating nodes")


    def createRelationship(self, data, label1, label2, relationship, towards=True):

        """
        -------------------------------------------------------
        Description : Function to create relationships in Neo4j

        Parameters :
        @param data -- a python list containing pair of relationships
        @param label1 -- a string representing label in Neo4j for entity1
        @param label2 -- a string representing label in Neo4j for entity2 
        @param relationship -- a string that will used to create the name of the relationship
        @param towards -- a boolean value

        Return:
        None
        --------------------------------------------------------
        """

        create = [self.config["RELATIONSHIPS"]["CREATE"]]
        create = [i == "TRUE" for i in create][0]

        if create:

            print("Creating Relationships in Neo4j 🛠️")

            try : 
                with tqdm(total = len(data)) as pbar:

                    for d in data:
                        
                        if towards:
                            _query = f'MATCH (N1:{label1}), (N2:{label2}) WHERE N1.name="{d[0]}" and N2.name="{d[1]}" CREATE (N1)-[:{relationship}]->(N2)'
                        else:
                            _query = f'MATCH (N1:{label1}), (N2:{label2}) WHERE N1.name="{d[0]}" and N2.name="{d[1]}" CREATE (N1)<-[:{relationship}]-(N2)'

                        self.driver.run(_query)

                        pbar.update(1)

            except Exception:
                print("Error Occured 🚫 🚫")
                print(traceback.print_exc())

        else:
            print("Forbidden ❌ for creating relationships")



        


def dealDuplicates(df):
    """
    -----------------------------------------------------------
    Description : Function to deal with duplicate movie titles

    Parameters : 
    @param df -- the pandas Dataframe containing the whole data

    Return :
    None 
    -----------------------------------------------------------
    """

    duplicates = df[df.duplicated(subset="showName")]
    
    showNames = duplicates["showName"].values

    for names in showNames:
        
        releaseYear = df.loc[df["showName"] == names, "releaseYear"].values
        recentYear = max(releaseYear)

        for year in releaseYear:
            if year != recentYear:
                df.loc[(df["showName"] == names) & (df["releaseYear"] == year), "showName"] = names + "-" + str(year)



def buildNodes(df, fieldName, showNames):
    """
    ------------------------------------------------------
    Description : Function to build the node data for Neo4j

    Parameters :
    @param df -- pandas Dataframe containing the complete data
    @param fieldName -- name of column for which the node data is to be made
    @param showNames -- list of show present in the dataset

    Return:
    @ret ret -- a python set
    -------------------------------------------------------
    """
    ret = set()
    for show in showNames:

        people = df.loc[df["showName"] == show, fieldName].values[0]
        people = people.split(",")
        
        for person in people:
            person = person.replace("\"", "")
            person = person.strip()
            ret.add(person)

    return ret

        

def buildRelationship(df, showList, fieldName):
    """
    ----------------------------------------------------------
    Description : Function to prepare relationship pair for Neo4j

    Parameters :
    @param df -- pandas dataframe containing the whole data
    @param showList -- list of show present in the dataset
    @param fieldName -- string representing the column name 

    Return:
    @ret ret -- a python list containing pair of relationships for entities
    ------------------------------------------------------------

    """

    ret = list()

    for show in showList:

        finders_keepers = df.loc[df["showName"] == show, fieldName].values.tolist()

        for f in finders_keepers:
            f = f.split(",")
            for _f in f:
                _f = _f.replace("\"", "")
                _f = _f.strip()

                if (_f, show) not in ret:
                    ret.append((_f, show))


    return ret






if __name__ == "__main__":

    df = pd.read_csv("./DATA/imdbData_movies.csv")
    print(df.head(5))
    

    print(df.info())

    print("Counting the duplicate values in showName 👬 ....")
    print(df.duplicated(subset="showName").sum())

    dealDuplicates(df)

    print("Counting duplicates 👬 after processing 🛠️ showName ... ")
    print(df.duplicated(subset="showName").sum())



    showList = df["showName"].unique()

    showDirectors = buildNodes(df, "director", showList)
    showCast = buildNodes(df, "cast", showList)
    showGenre = buildNodes(df, "genre", showList)

    

    # Prepare Node data for Directors
    nodeDirectors = []

    for i, directors in enumerate(showDirectors):
        nodeDirectors.append({"name": directors, "id":f"D{i}"})
    
    # Prepare Node data for cast
    nodeCast = []

    for i, cast in enumerate(showCast):
        nodeCast.append({"name":cast, "id":f"A{i}"})


    # Prepare Node data for Genre
    nodeGenre = []
    for i, genre in enumerate(showGenre):
        nodeGenre.append({"name":genre, "id":f"G{i}"})


    # Prepare Node data for Movie
    nodeMovies = []

    for i, movies in enumerate(showList):

        rating = df.loc[df["showName"] == movies, "imdbRating"].values[0]
        metascore = df.loc[df["showName"] == movies, "metascore"].values[0]
        releaseYear = df.loc[df["showName"] == movies, "releaseYear"].values[0]

        nodeMovies.append({"name":movies, "imdbRating":rating, "metascore":metascore, "releaseYear":releaseYear, "id":f"M{i}"})


    



    DataTransfer().createNode(nodeDirectors, "Director")
    DataTransfer().createNode(nodeCast, "Actor")
    DataTransfer().createNode(nodeMovies, "Movie")
    DataTransfer().createNode(nodeGenre, "Genre")

    movieCast = buildRelationship(df, showList, "cast")
    movieDirector = buildRelationship(df, showList, "director")
    movieGenre = buildRelationship(df, showList, "genre")

    # checkCast = list(filter(lambda x: "," in x[0], movieCast))
    # checkDirector = list(filter(lambda x: "," in x[0], movieDirector))
    checkGenre = list(filter(lambda x: "," in x[0], movieGenre))


    # assert(checkCast == [])
    # assert(checkDirector == [])

    # assert(checkGenre == [])


    DataTransfer().createRelationship(movieCast, "Actor", "Movie", "Acted_In")
    DataTransfer().createRelationship(movieDirector, "Director", "Movie", "Directed_by")
    DataTransfer().createRelationship(movieGenre, "Genre", "Movie", "type_of", towards=False)




    


    


    




