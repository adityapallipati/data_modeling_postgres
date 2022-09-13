# Sparkify Startup Database
## ETL Process 

The following project was created in order to extract data from two seperate files, our song file and log file, in order to analyze habits of users who use Sparkify. With the data that's been extracted, transformed, and loaded into multiple tables, we are are able to see what users listen to, what artists are most listened to, when the songs are played and so on. This can be useful in order to create various services for users. If the data can be properly analyzed for what is popular among Sparkify's users things like playlists of popular artists by location can be created and promotions based on popular songs can be made as well.

The schema was designed with 5 tables in total. The songplays table being our fact table in our star schema design. The users, songs, artists, and time tables being our deminsion tables.

The SQL querires were written in the sql_queries.py file and the tables were created with the create_tables.py. The two jupyter notebooks were used to test our code to ensure we can retrieve the neccessary information for analysis through SQL queries. The etl.py file contains the contents of the etl.ipynb jupyter notebook in a condensed, reusable format.  