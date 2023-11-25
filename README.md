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

file struct:

- config.py           I put all the config params here.
- basic_uploader.py   The file contain most of my code.
- main.py             Entry of the whole program.
- utils.py            tool box to store some functions are not just for file upload.
- test_basic_uploader.py  test file for the whole project.

the upload program support two ways of uploading. pandas and dask. pandas engine is more reliable, and dask engine is more fast.

I useing my own laptop do the test:
pandas engine can complete the whole upload process 1n 190 seconds
dask engine can complete the whole upload process in 121 seconds

in some of my sel-test, using dask engine upload may be stuck halfway. 
it's happend pretty rare, but when you occur this problem, please switch to pandas engine and try again.

I use chunk read and upload in my pandas method because pandas always have some memory issues.
I didn't use chunk for dask because dask native support for partitioning.

I intend to use multiprocessing pool for uploading and then give up, because the amount of the data is not so large,
and dask to_csv func support parallel upload.

### environement

the project using python 3.8 (No big reason, I just have 3.8 in my hand)

the project have few dependencies, psycopg2, pandas, dask, SQLAlchemy and pytest 

You need a PostgreSQL server for connection.

### How to Start

put the `Stock.zip` in the root path of the project.

change the connection config of the postgres in the config.py -> db_conf
recommand to create a new db and thend drop it after demo.

choose the upload engine in config.py -> input_conf['engine'], you can choose between dask and pandas. 
the default engine is dask 

if the machine running the script has large memory, you can change the input_conf['batch'] value for bigger number to make it run faster.

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

### Ver 2.5

add test case for the whole program


