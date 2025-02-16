# This file is part of 'writer' R package
# worker functions
#
# Notes to developers:
#
# All core work happens here
#
# write_tbl_*
# writes from 'source_tbl' to dest_table_name
# source_tbl as the name says is a tbl
# dest_table_name is a string in 'a.b.c' format
# con is the connection to used to write to 'dest_table_name'
# Typically this is same as connection object held by 'source_tbl'
#
# All workers either return TRUE (when job is done successfully)
# or return the 'try-error' object
#
# circle of hell:
#
# 1. rows_* call uses transaction when inplace, but does the ops correctly.
# Some databases dont allow transaction. So this is limiting.
# 2. rows_* call without inplace returns a overall query which can only be used
# in overwrite sense which is impractical for large tables.
# 3. Correct sql can be generated from sql_query_* functions which is tricky
# due to unclear documentation. `patch` needs separate write here. Pretty much
# this is the only route for seperate implementations for with or without
# transaction.


#' @keywords internal
#' @title write_tbl_overwrite
#' @description write_tbl_overwrite
write_tbl_overwrite = function(source_tbl,
                               dest_table_name,
                               con,
                               transaction_flag,
                               ...
                               ){

  # 1. begin transaction
  # 2. truncate dest table
  # 3. append rows from source tbl
  # 4. Commit/Rollback based on error

  if (transaction_flag) DBI::dbBegin(con)
  ops_delete = try({
    DBI::dbExecute(con, glue::glue("DELETE FROM {dest_table_name}"))
  }, silent = TRUE)

  if (inherits(ops_delete, "try-error")){
    if (transaction_flag) DBI::dbRollback(con)
    return(ops_delete)
  } else {
    if (transaction_flag) DBI::dbCommit(con)
  }

  ops_append = try({
    write_tbl_append(source_tbl,
                     dest_table_name,
                     con,
                     transaction_flag,
                     ...
                     )
  }, silent = TRUE)

  if (inherits(ops_append, "try-error")){
    return(ops_append)
  } else {
    return(TRUE)
  }
}

#' @keywords internal
#' @title write_tbl_overwrite_schema
#' @description write_tbl_overwrite_schema
write_tbl_overwrite_schema = function(source_tbl,
                                      dest_table_name,
                                      con,
                                      transaction_flag,
                                      ...
                                      ){

  # 1. Write source_tbl to some new table
  # 2. delete the dest table, if it exists.
  # 3. Rename new table to the dest table.

  create_random_name = function(len = 20){
    paste0(sample(letters, len), collapse = "")
  }

  dest_table_name_split = strsplit(dest_table_name, "\\.")[[1]]
  temp_table_split = dest_table_name_split

  while (TRUE){
    temp_table_split[length(temp_table_split)] = create_random_name()

    table_exists_flag =
      DBI::dbExistsTable(con,
                         string_to_id(paste0(temp_table_split, collapse = "."))
                         )
    if (!table_exists_flag){
      break
    }
  }

  temp_table_name = paste0(temp_table_split, collapse = ".")

  if (transaction_flag) DBI::dbBegin(con)
  ops = try({
    stmt =
      dbplyr::sql_query_save(con = con,
                             sql = dbplyr::sql_render(source_tbl),
                             name = wrap_by_I(temp_table_name),
                             temporary = FALSE
                             )
    DBI::dbExecute(con, stmt)
    DBI::dbRemoveTable(con, string_to_id(dest_table_name))
    DBI::dbExecute(
      con,
      glue::glue("ALTER TABLE {temp_table_name} RENAME TO {dest_table_name}")
      )
    }, silent = TRUE
  )

    if (inherits(ops, "try-error")){
      if (transaction_flag) DBI::dbRollback(con)
      return(ops)
    } else {
      if (transaction_flag) DBI::dbCommit(con)
      return(TRUE)
    }
}

#' @keywords internal
#' @title write_tbl_create
#' @description write_tbl_create
write_tbl_create = function(source_tbl,
                            dest_table_name,
                            con,
                            transaction_flag,
                            ...
                            ){

  # 1. begin transaction
  # 2. create table
  # 4. Commit/Rollback based on error

  if (transaction_flag) DBI::dbBegin(con)
  ops = try({
    stmt =
      dbplyr::sql_query_save(con = con,
                             sql = dbplyr::sql_render(source_tbl),
                             name = wrap_by_I(dest_table_name),
                             temporary = FALSE
                             )
    DBI::dbExecute(con, stmt)
  }, silent = TRUE)

  if (inherits(ops, "try-error")){
    if (transaction_flag) DBI::dbRollback(con)
    return(ops)
  } else {
    if (transaction_flag) DBI::dbCommit(con)
    return(TRUE)
  }
}

#' @keywords internal
#' @title write_tbl_append
#' @description write_tbl_append
write_tbl_append = function(source_tbl,
                            dest_table_name,
                            con,
                            transaction_flag,
                            ...
                            ){

  # 1. begin transaction
  # 2. append table
  # 4. Commit/Rollback based on error

  if (!transaction_flag){
    ops = try({
      stmt =
        dbplyr::sql_query_append(con = con,
                                 table = wrap_by_I(dest_table_name),
                                 from = dbplyr::sql_render(source_tbl),
                                 insert_cols = colnames(source_tbl),
                                 ...
                                 )
      DBI::dbExecute(con, stmt)
    }, silent = TRUE)

  } else {

    ops = try({
      dplyr::rows_append(x = dplyr::tbl(con, wrap_by_I(dest_table_name)),
                         y = source_tbl,
                         copy = TRUE,
                         in_place = TRUE,
                         ...
                         )
      }, silent = TRUE)
  }

  if (inherits(ops, "try-error")){
    return(ops)
  } else {
    return(TRUE)
  }
}

#' @keywords internal
#' @title write_tbl_insert
#' @description write_tbl_insert
write_tbl_insert = function(source_tbl,
                            dest_table_name,
                            con,
                            transaction_flag,
                            ...
                            ){

  # 1. begin transaction
  # 2. insert table
  # 4. Commit/Rollback based on error

  if (!transaction_flag){
    ops = try({
      stmt =
        dbplyr::sql_query_insert(con = con,
                                 table = wrap_by_I(dest_table_name),
                                 from = dbplyr::sql_render(source_tbl),
                                 insert_cols = colnames(source_tbl),
                                 ...
                                 )
      DBI::dbExecute(con, stmt)
    }, silent = TRUE)

  } else {

    ops = try({
      dplyr::rows_insert(x = dplyr::tbl(con, wrap_by_I(dest_table_name)),
                         y = source_tbl,
                         copy = TRUE,
                         in_place = TRUE,
                         ...
                         )
      }, silent = TRUE)
  }

  if (inherits(ops, "try-error")){
    return(ops)
  } else {
    return(TRUE)
  }
}

#' @keywords internal
#' @title write_tbl_update
#' @description write_tbl_update
write_tbl_update = function(source_tbl,
                            dest_table_name,
                            con,
                            transaction_flag,
                            ...
                            ){

  # 1. begin transaction
  # 2. update table
  # 4. Commit/Rollback based on error

  args = list(...)
  if (!("by" %in% names(args))){
    cli::cli_abort("Arg {.field by} should be provided.")
  }

  if (!transaction_flag){
    # validate by
    dest_names = colnames(dplyr::tbl(con, I(dest_table_name)))
    if (!(is.character(args$by) & all(args$by %in% dest_names))){
      cli::cli_abort("Arg {.field by} should be subset of column names.")
    }

    # this chunk was taken from `dbplyr::row_update.tbl_lazy` from `dbplyr`
    update_cols = setdiff(colnames(source_tbl), args$by)
    update_values = rlang::set_names(
      sql_table_prefix(con, update_cols, "...y"),
      update_cols
      )

    ops = try({
      stmt =
        do.call(dbplyr::sql_query_update_from,
                c(list(con = con,
                       table = wrap_by_I(dest_table_name),
                       from = dbplyr::sql_render(source_tbl),
                       update_values = update_values
                       ),
                  args # arg 'by' is here
                  )
              )
      DBI::dbExecute(con, stmt)
    }, silent = TRUE)

  } else {

    ops = try({
      do.call(dplyr::rows_update,
              c(list(x = dplyr::tbl(con, wrap_by_I(dest_table_name)),
                     y = source_tbl,
                     copy = TRUE,
                     in_place = TRUE
                     ),
                args # arg 'by' is here
                )
              )
      }, silent = TRUE)
  }

  if (inherits(ops, "try-error")){
    return(ops)
  } else {
    return(TRUE)
  }
}

#' @keywords internal
#' @title write_tbl_upsert
#' @description write_tbl_upsert
write_tbl_upsert = function(source_tbl,
                            dest_table_name,
                            con,
                            transaction_flag,
                            ...
                            ){

  # 1. begin transaction
  # 2. upsert table
  # 4. Commit/Rollback based on error

  arguments = list(...)
  if (!("by" %in% names(arguments))){
    cli::cli_abort("Arg {.field by} should be provided.")
  }

  if (!transaction_flag){
    # validate by
    dest_names = colnames(dplyr::tbl(con, I(dest_table_name)))
    if (!(is.character(arguments$by) && all(arguments$by %in% dest_names))){
      cli::cli_abort("Arg {.field by} should be subset of column names.")
    }

    if (!("update_cols" %in% names(arguments))) {
      arguments$update_cols = setdiff(colnames(source_tbl), arguments$by)
    }

    ops = try({
      stmt =
        do.call(dbplyr::sql_query_upsert,
                c(list(con = con,
                       table = dest_table_name, # should not wrap
                       from = dbplyr::sql_render(source_tbl)
                       ),
                  arguments # arguments 'by' and 'update_cols' are here
                  )
                )
      DBI::dbExecute(con, stmt)
    }, silent = TRUE)

  } else {

    ops = try({
      do.call(dplyr::rows_upsert,
              c(list(x = dplyr::tbl(con, wrap_by_I(dest_table_name)),
                     y = source_tbl,
                     copy = TRUE,
                     in_place = TRUE
                     ),
                arguments # arguments 'by' and 'update_cols' are here
                )
              )
      }, silent = TRUE)
  }

  if (inherits(ops, "try-error")){
    return(ops)
  } else {
    return(TRUE)
  }
}

#' @keywords internal
#' @title write_tbl_patch
#' @description write_tbl_patch
write_tbl_patch = function(source_tbl,
                            dest_table_name,
                            con,
                            transaction_flag,
                            ...
                            ){

  # 1. begin transaction
  # 2. patch rows from table
  # 4. Commit/Rollback based on error

  args = list(...)
  if (!("by" %in% names(args))){
    cli::cli_abort("Arg {.field by} should be provided.")
  }

  if (!transaction_flag){

    cli::cli_abort("Yet to implement transaction free patch",
                   class = "error_not_implemented"
                   )

  } else {

    ops = try({
      do.call(dplyr::rows_patch,
              c(list(x = dplyr::tbl(con, wrap_by_I(dest_table_name)),
                     y = source_tbl,
                     copy = TRUE,
                     in_place = TRUE
                     ),
                args # arg 'by'
                )
              )
      }, silent = TRUE)
  }

  if (inherits(ops, "try-error")){
    return(ops)
  } else {
    return(TRUE)
  }
}

#' @keywords internal
#' @title write_tbl_delete
#' @description write_tbl_delete
write_tbl_delete = function(source_tbl,
                            dest_table_name,
                            con,
                            transaction_flag,
                            ...
                            ){

  # 1. begin transaction
  # 2. delete rows from table
  # 4. Commit/Rollback based on error

  args = list(...)
  if (!("by" %in% names(args))){
    cli::cli_abort("Arg {.field by} should be provided.")
  }

  if (!transaction_flag){

    # validate by
    dest_names = colnames(dplyr::tbl(con, I(dest_table_name)))
    if (!(is.character(args$by) & all(args$by %in% dest_names))){
      cli::cli_abort("Arg {.field by} should be subset of column names.")
    }

    ops = try({
      stmt =
        do.call(dbplyr::sql_query_delete,
                c(list(con = con,
                       table = wrap_by_I(dest_table_name),
                       from = dbplyr::sql_render(source_tbl)
                       ),
                  args # arg 'by'
                  )
                )
      DBI::dbExecute(con, stmt)
    }, silent = TRUE)

  } else {

    ops = try({
      do.call(dplyr::rows_delete,
              c(list(x = dplyr::tbl(con, wrap_by_I(dest_table_name)),
                     y = source_tbl,
                     copy = TRUE,
                     in_place = TRUE
                     ),
                args # arg 'by'
                )
              )
      }, silent = TRUE)
  }

  if (inherits(ops, "try-error")){
    return(ops)
  } else {
    return(TRUE)
  }
}
