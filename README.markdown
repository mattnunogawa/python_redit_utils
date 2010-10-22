Redis Counter class

Used to measure events over spans of time (such as last 30 days or last 24 hours, or 2 months ago, or this past week)

Essentially, you just create the RedisCounter object 

    counter = RedisCounter("site_hits", some_redis_instance)
    
Then use `increment_counter()` to add a hit and the various count functions to get the counts:

    current_count()
    counts_in_last_five_seconds()
    counts_in_last_hour()
    counts_in_last_day()
    counts_in_last_week()
    counts_in_last_month()
    
These take a timestamp argument of the hour/day/whatever you want to get counts for

    counts_for_hour(seconds_since_epoch)
    counts_for_day(seconds_since_epoch)
    counts_for_week(seconds_since_epoch)
    counts_for_month(seconds_since_epoch)
 
There are two maintenance functions as well:

    reset_counter()
    delete_counter()
