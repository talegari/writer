# writer 0.2.1
* internal change: temp table/view on some databases does accept catalog.schema
spec.
* internal change: `copy_inline` uses transaction, so sql is extracted run via 
`dbExecute`.

# writer 0.2.0
* New argument `use_transaction` (default: `TRUE`) not allows to run operations
when connection does not permit transactions.

# writer 0.1.0
* Initial CRAN submission.
