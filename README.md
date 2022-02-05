# Big data architectures project - seismic activity around the world

## Description
This project uses historical data about seismic activity in the world in order to show if and how such activity can influence society in terms of demographics and tourism. The project also uses realtime data, specifically Tweets with relevant keywords, and provides insight into how often people tweet about earthquakes.

## Data sources
- Historical data:
  1. Seismographic data - https://www.kaggle.com/danielpe/earthquakes
  2. Demographics - https://www.kaggle.com/census/international-data?select=birth_death_growth_rates.csv (files "birth_death_growth_rates.csv" and "country_names_area.csv")
  3. Tourism - https://www.kaggle.com/ayushggarg/international-tourism-demographics (files "API_ST.INT.ARVL_DS2_en_csv_v2_1927083.csv" and "API_ST.INT.DPRT_DS2_en_csv_v2_1929304.csv")
  4. US states' names and abbreviations - http://goodcsv.com/geography/us-states-territories/

- Realtime data:
  1. Twitter API with filters

## Running the application
- Download the .csv files for historical data from the links listed above and place them inside the "./local_data" directory. Change the names of the downloaded files as follows:
  1. Seismographic data - change from "consolidated_data.csv" to "seismic_activity_info.csv"
  2. Demographics - no change (keep the names "birth_death_growth_rates.csv" and "country_names_area.csv")
  3. Tourism - change from "API_ST.INT.ARVL_DS2_en_csv_v2_1927083.csv" to "tourism_arrivals.csv"; change from "API_ST.INT.DPRT_DS2_en_csv_v2_1929304.csv" to "tourism_departures.csv")
  4. US states' names and abbreviations - change from "us-states-territories.csv" to "us_states.csv"

- Set the environment variables containing your Twitter API credentials. For this, you need to create a Twitter developer account and then use it to create a project and an application (https://developer.twitter.com/en/docs/twitter-api/getting-started/getting-access-to-the-twitter-api). To successfully run this program, you will need to apply for elevated access on your project (https://developer.twitter.com/en/portal/products/elevated). After this, update the ./spark/spark_stream/spark_stream.env file by inserting your Twitter API credentials.

- Enter the /docker_project directory (using "cd ./docker_project") and then run the application using the "docker-compose up --build" command. This will immediately execute all batch jobs, and will also run the realtime producer. The batch jobs might take some time (possibly more than half an hour), since the file containing seismic info is quite large.

- To run one of the realtime data consumers, enter the /spark/spark_streaming_run directory, and run one the bash scripts inside ("spark_run_consumer.sh" will provide realtime information about the number of tweets about earthquakes tweeted every 30 seconds; "spark_run_consumer2.sh" will provide information about the number of tweets tweeted from an account from a set of relevant accounts, every 30 seconds). You will be able to see the results in the console, although it is also stored in a mongodb collection.

## View batch data
- To see the results obtained by executing batch jobs, open your browser and go to "localhost:3000". This will open the metabase page. In the "Add your data section", do the following:
  1. Choose MongoDB as your database type
  2. Enter any name you like 
  3. Enter "mongodb" as your host
  4. Enter 27017 as the port
  5. Enter "currated-data" as the database name
  6. Leave the username and the password blank, and proceed
- After entering all the other info required on the first page, you will be able to proceed and view data from the "seismic" collection, which contains the results obtained by executing the batch jobs. 
- You can view the realtime processing results in the same way, just change the host to "mongodb2", and enter "streaming_data" as the database name

## Stopping the application
- Enter the ./docker_project directory (using "cd ./docker_project) and run the "docker-compose down" command.
