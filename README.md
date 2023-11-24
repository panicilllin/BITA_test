# Manual

## Requirement of this Subject

The exercise involves developing a Python program that reads a .csv file and inserts its contents into a local PostgreSQL database.

Before inserting it into the database, you must remove the content from a possible previous import.

well-written code and good development practices

performance and resource consumption

You can use any libraries and the reason choose (and also the ones you discarded).

you cannot use COPY or any bulk import mechanism that is native to Postgres.

If you accompany the project with a suitable README.md, we will appreciate it.

complete this exercise in 5 continuous days (deadline: Saturday, November 25 at midnight your local time).

Lastly, you need to upload the code to a public GitHub repository.

## Manual of this Subject

### environement

the project using python 3.8 (No big reason, I just have 3.8 in my hand)
the project have two dependencies, pandas and psycopg2, I will try using dask in next version but for this first version I choose to use pandas.
You need a PostgreSQL for connection.

### How to Start

put the Stock.zip in the root path of the project.
config the connection message of the postgres in the config.py
if the machine running the script has large memory, change the input_conf['batch'] value for bigger to make it run faster.
checkout new virtual env or conda env
use `pip install -r requirements.txt` to install requirements.
type `python main.py` in the root path of the project to run it.
the log fill is in ./upload.log

## TODO

I didn't use multi threads for uploading because it may cause the table loss order. I will add it as a choice in next version.
I will add summaries function and test in next version




