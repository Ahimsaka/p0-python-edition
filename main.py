import mysql.connector
from pyspark.sql import SparkSession, dataframe
from pyspark.sql.functions import col, max as max_


def get_dataframe(path: str):
    return spark.read.option("header", True)\
        .csv(path)


def get_max(df: dataframe, category: str):
    df.select("date", category).orderBy(col(category).asc()).limit(1).show()


def get_min(df: dataframe, category: str):
    df.select("date", category).orderBy(col(category).desc()).limit(1).show()


def show_all(df: dataframe, category: str):
    df.select("date", category).orderBy(col("date").asc()).show(df.size, False)


def select_type(datapoint: str, df: dataframe):
    cont = True

    while cont:
        print("\033[92mWould you like to see min, max, or all?")

        type = input("min, max, or all (or back to go back):\033[95m")

        if type == "max":
            get_max(df, datapoint)
            cont = False
        elif type == "min":
            get_min(df, datapoint)
            cont = False
        elif type == "all":
            show_all(df, datapoint)
            cont = False
        elif type == "back":
            cont = False
        else:
            print("\033[91mI did not recognize that input. Please try again.")


def select_datapoint(city: str, df: dataframe):
    cont = True

    while cont:
        print(f"\033[0mExploring data for {city}!\033[96m")

        for column in df.schema.names:
            if column != "date":
                print(column)

        datapoint = input("\033[92mSelect the data you would like to explore (or type back to go back):")

        if datapoint in df.schema.names:
            select_type(datapoint, df)
        elif datapoint == "back":
            cont = False
        else:
            print("\033[91mI did not recognize that input. Please try again.")


def select_city(cities: dict):
    cont = True

    while cont:
        print("\033[92mWelcome to the weather app situation.\nThe app that just don't quit.\n(Unless you type quit)\033[96m")

        for city in cities:
            print(city)

        selection = input("\033[92mChoose a city (or quit to quit):")

        if selection == "quit":
            cont = False
        elif selection in cities:
            select_datapoint(selection, cities[selection])
        else:
            print("\033[91mI did not recognize that input. Please try again.")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    spark = SparkSession.builder.master("local[1]")\
        .appName("p0")\
        .getOrCreate()

    cities = {
        "charlotte": get_dataframe("C:/Users/Devin/PycharmProjects/p0_redux/static/kclt.csv"),
        "los angeles": get_dataframe("C:/Users/Devin/PycharmProjects/p0_redux/static/kcqt.csv"),
        "houston": get_dataframe("C:/Users/Devin/PycharmProjects/p0_redux/static/khou.csv"),
        "indianapolis": get_dataframe("C:/Users/Devin/PycharmProjects/p0_redux/static/kind.csv"),
        "jacksonville": get_dataframe("C:/Users/Devin/PycharmProjects/p0_redux/static/kjax.csv"),
        "chicago": get_dataframe("C:/Users/Devin/PycharmProjects/p0_redux/static/kmdw.csv"),
        "new york": get_dataframe("C:/Users/Devin/PycharmProjects/p0_redux/static/knyc.csv"),
        "philadelphia": get_dataframe("C:/Users/Devin/PycharmProjects/p0_redux/static/kphl.csv"),
        "phoenix": get_dataframe("C:/Users/Devin/PycharmProjects/p0_redux/static/kphx.csv"),
        "seattle": get_dataframe("C:/Users/Devin/PycharmProjects/p0_redux/static/ksea.csv")
    }

    select_city(cities)


    print("\033[0mGoodbye, nerd.")
