# Question 1. Load January 2020 data

From the logs:
06:54:27.930 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 447770

The correct answer is 447770 rows.

# Question 2. Scheduling with cron

The correct cron syntax for 5AM on the first day of each month is 0 5 1 * *

# Question 3. Loading data to BigQuery

The correct answer is 14,851,920.
Logs:
01:52:41.773 | INFO    | Flow run 'cuddly-mushroom' - Finished in state Completed('All states completed.')
Prosessed 7019375 rows.
Prosessed 7832545 rows.
01:52:42.108 | INFO    | prefect.infrastructure.process - Process 'cuddly-mushroom' exited cleanly.

# Question 4. Github Storage Block

