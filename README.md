postgresql-time-series-table-partitions
=======================================

Originally for monthly table partitions, more info at [imperialwicket.com](http://imperialwicket.com/postgresql-automating-monthly-table-partitions).

This currently duplicates a [similar gist](https://gist.github.com/imperialwicket/2720074), but revision maintenance and change requests were becoming burdensome.

Eventually I'll copy over data from the blog post to this readme (PR requests welcome for this and any other changes!).


Note that `update_partitions.sql` and `update_partitions_no_unknown_table.sql` are mutually exclusive. `update_partitions.sql` creates an 'unknown' table, and any dated inserts that don't have an appropriate child table automatically go here. If you have dirty data, or want to pay close attention to inserts for reporting, this could work well for you. `update_partitions_no_unknown_table.sql` has an alternate trigger that will dynamically create a missing child table for the appropriate date interval when an 'unknown' insert occurs. If you have unpredictable data that should always be well-organized, this alternative could be more productive than managing the unknown table (thanks @sandinosaso for this!). The trigger in `update_partitions_no_unknown_table.sql` relies on syntax available in Postgres 9.1 and newer.
