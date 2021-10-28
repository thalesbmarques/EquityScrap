# Equity Scrap

Equity Scrap is a project which main goal is to get data from different sources to use as input for equity valuation
process.

This first version will start with the full process of scrap and store the rental car prices from Movida (MOVI3), a
brazilian equity listed on B3.

You can expect for further versions new sources (via APIs and Web Scraping).

For analytics, you can Check my FinStrats module.

## Examples

### Movida

We have two options to get prices from Movida.

### Massive Scrap

When you want to scrap a range of dates/places/times, this will be your best way to do. To tune your parameters, go
to [movida_mass_request.yaml](Cars/movida_mass_request.yaml) and replace the values from the variables:

* date_range (int): the rolling number of days to be retrieved.
* start_intervals (int): n-days from now to start the scrap
* end_intervals (int): n-days from start_intervals
* time (str):
* places (str): Movida Stores'codes.  Codes are available in [json](db/movida_stores.json') or [csv](db/movida_stores.csv)

Let's try an example:

```yaml
date_range: 10  # Will look 10 periods forward the start_intervals

date_interval:
  start_intervals:
    - 1  # Start consulting 1 day from now
  end_intervals:
    - 10 # Will create a combination 1-day/11-day, 2-day/12-day, and so on...
    - 15 # Same but with 15 days interval
    - 20 # Same but with 20 days interval

time:
  - "08:00"  # Pick up and delivery time

places:
  - "CGH"  # Congonhas Airport
  - "GRU"  # Guarulhos Airport
``` 

Now, we'll run [movida.py](Cars/movida.py). Just to illustrate, the last lines of the code:

```python3
if __name__ == '__main__':
    params_ = get_mass_params()
    df_list = set_up_threads(params_)
    df = pd.concat(df_list)
    # Connecting to sql
    conn = sqlite3.connect('db/data.db3')
    # Exporting to sql
    df.to_sql('movida', con=conn, if_exists='append', index=False)
    # Closing connection
    conn.close()
    end = timer()
    print(timedelta(seconds=end - start))
```

The code above will read the yaml file with the inputs, will access movida's website (4 threads at time) and will store
the output appending the values in [data.db3](db/data.db3), specifically on movida table.

The log will be printed in the screen, but it could be also checked in [data.log](data.log). The snipped is an example
of a logging output:

```
28/10/2021 12:25:19 - __main__ - INFO - [CLEANING HTML] Cleaning html object and creating Data Frame Place:CGH, Start:06/11/2021 - 08:00, End:26/11/2021 - 08:00, 
28/10/2021 12:25:19 - __main__ - INFO - [CLEANING PASSED] Cleaning Passed Place:CGH, Start:06/11/2021 - 08:00, End:26/11/2021 - 08:00, 
28/10/2021 12:25:19 - __main__ - INFO - [CREATING DATAFRAME] Cleaning Passed Place:CGH, Start:06/11/2021 - 08:00, End:26/11/2021 - 08:00, 
28/10/2021 12:25:19 - __main__ - INFO - [DATAFRAME PASSED] Cleaning Passed Place:CGH, Start:06/11/2021 - 08:00, End:26/11/2021 - 08:00, 
28/10/2021 12:25:23 - __main__ - INFO - [DRIVER CLOSED] Place:GRU, Start:02/11/2021 - 08:00, End:12/11/2021 - 08:00, 
28/10/2021 12:25:23 - __main__ - INFO - [SCRAP PASSED] Place:GRU, Start:02/11/2021 - 08:00, End:12/11/2021 - 08:00, 
28/10/2021 12:25:23 - __main__ - INFO - [CLEANING HTML] Cleaning html object and creating Data Frame Place:GRU, Start:02/11/2021 - 08:00, End:12/11/2021 - 08:00, 
28/10/2021 12:25:23 - __main__ - INFO - [CLEANING PASSED] Cleaning Passed Place:GRU, Start:02/11/2021 - 08:00, End:12/11/2021 - 08:00, 
28/10/2021 12:25:23 - __main__ - INFO - [CREATING DATAFRAME] Cleaning Passed Place:GRU, Start:02/11/2021 - 08:00, End:12/11/2021 - 08:00, 
28/10/2021 12:25:23 - __main__ - INFO - [DATAFRAME PASSED] Cleaning Passed Place:GRU, Start:02/11/2021 - 08:00, End:12/11/2021 - 08:00, 
```

This example will generate 60 unique combinations, which took 9 minutes to run entirely.

On section Consulting Data, we'll show the output of this run

### Regular Scrap

TBD

### Consulting Data

Here we will get the stored values on [data.db3](db/data.db3) with a simple query.

The example could be found on [read_movida.py](Cars/read_movida.py), the code is pretty simple. It creates a connector
to SQLITE database and reads all the values on movida's table
```python3
import pandas as pd
import sqlite3

# Creating connection
conn = sqlite3.connect('db/data.db3')
movida = pd.read_sql('SELECT * FROM movida', con=conn)
conn.close()
```

The output head will be:

|    | place   | extraction_dt              | car_group                  | start_date   | end_date   | start_time   | end_time   | cars                                |   prices |
|---:|:--------|:---------------------------|:---------------------------|:-------------|:-----------|:-------------|:-----------|:------------------------------------|---------:|
|  0 | CGH     | 2021-10-27 20:21:29.284320 | GRUPO AX - MOVIDA ON       | 01/11/2021   | 20/11/2021 | 08:00        | 08:00      | Mobi Like ou similar                |   211.56 |
|  1 | CGH     | 2021-10-27 20:21:29.284320 | GRUPO B - ECONÔMICO        | 01/11/2021   | 20/11/2021 | 08:00        | 08:00      | Uno, Onix, Gol, Argo, ou similar    |   233.43 |
|  2 | CGH     | 2021-10-27 20:21:29.284320 | GRUPO BS - ECONÔMICO SEDAN | 01/11/2021   | 20/11/2021 | 08:00        | 08:00      | Logan, Versa, Onix Plus ou Similar  |   253.13 |
|  3 | CGH     | 2021-10-27 20:21:29.284320 | GRUPO BX - ECONÔMICO PLUS  | 01/11/2021   | 20/11/2021 | 08:00        | 08:00      | HB20 ou Similar                     |   235.17 |
|  4 | CGH     | 2021-10-27 20:21:29.284320 | GRUPO C - COMPACTO         | 01/11/2021   | 20/11/2021 | 08:00        | 08:00      | Gol, Sandero, 208, HB20, ou similar |   251.68 |



