# Manual

## Requirement of this Subject

The exercise involves developing a Python program that reads a .csv file and inserts its contents into a local PostgreSQL database.

Remove the content from a possible previous import before inserting it into the database

Well-written code and good development practices

Performance and resource consumption

Can use any libraries and the reason choose (and also the ones you discarded).

Cannot use COPY or any bulk import mechanism that is native to Postgres.

Accompany the project with a suitable README.md.

Complete this exercise in 5 continuous days (deadline: Saturday, November 25 at midnight your local time).

Upload the code to a public GitHub repository.

## Manual of this Subject

the upload program support two ways of uploading. pandas and dask. pandas engine is more reliable, and dask engine is more fast.

I useing my own laptop do the test:
pandas engine can complete the whole upload process 1n 190 seconds
dask engine can complete the whole upload process in 121 seconds

in some of my sel-test, using dask engine upload may be stuck halfway. 
it's happend pretty rare, but when you occur this problem, please switch to pandas engine and try again.

### environement

the project using python 3.8 (No big reason, I just have 3.8 in my hand)

the project have two dependencies, pandas and psycopg2, I will try using dask in next version but for this first version I choose to use pandas.

You need a PostgreSQL for connection.

### How to Start

put the `Stock.zip` in the root path of the project.

choose the upload engine in config.py -> input_conf['engine'], you can choose between dask and pandas.

config the connection message of the postgres in the config.py

if the machine running the script has large memory, change the input_conf['batch'] value for bigger to make it run faster.

checkout new virtual env or conda env

use `pip install -r requirements.txt` to install requirements.

type `python main.py` in the root path of the project to run it.

the log fill is in `./upload.log`

## release:

### Ver 1.0

complete the function using pandas

### Ver 2.0

using dask as another way of uploading engine, dask cause 60% less time than pandas.

change the way to Transfer parameters, to avoid hard code.


## TODO

add multi threads upload

add test case


