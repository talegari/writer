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
# cross connection writes are possible
#
# modes covered by write_tbl_*: overwrite, overwrite_schema, create
#
# write_df_*
# writes from data.frame to dest_table_name
# modes covered by write_df_*: overwrite, overwrite_schema, create
#
# write_rows writes from a 'source' to 'dest_table_name'
# 'source' can be either a dataframe or a tbl
# modes covered by write_rows: append, insert, update, upsert, patch, delete
#
# All workers either return TRUE (when job is done successfully)
# or return the 'try-error' object

#' @keywords internal
#' @title write_tbl_overwrite
#' @description write_tbl_overwrite
write_tbl_overwrite = function(source_tbl, dest_table_name, con, ...){

  # 1. begin transaction
  # 2. truncate dest table
  # 3. append rows from source tbl
  # 4. Commit/Rollback based on error

  DBI::dbBegin(con)
  ops_delete = try({
    DBI::dbExecute(con, glue::glue("DELETE FROM {dest_table_name}"))
  }, silent = TRUE)

  if (inherits(ops_delete, "try-error")){
    DBI::dbRollback(con)
    return(ops_delete)
  } else {
    DBI::dbCommit(con)
  }

  ops_append = try({
    dplyr::rows_append(x = dplyr::tbl(con, wrap_by_I(dest_table_name)),
                       y = source_tbl,
                       in_place = TRUE,
                       copy = TRUE,
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
write_tbl_overwrite_schema = function(source_tbl, dest_table_name, con, ...){

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

  DBI::dbBegin(con)
  ops = try({
    create_query =
      dbplyr::sql_query_save(con = con,
                             sql = dbplyr::sql_render(source_tbl),
                             name = wrap_by_I(temp_table_name),
                             temporary = FALSE
                             )
    DBI::dbExecute(con, create_query)
    DBI::dbRemoveTable(con, string_to_id(dest_table_name))
    DBI::dbExecute(
      con,
      glue::glue("ALTER TABLE {temp_table_name} RENAME TO {dest_table_name}")
      )
    }, silent = TRUE
  )

    if (inherits(ops, "try-error")){
      DBI::dbRollback(con)
      return(ops)
    } else {
      DBI::dbCommit(con)
      return(TRUE)
    }
}

#' @keywords internal
#' @title write_tbl_create
#' @description write_tbl_create
write_tbl_create = function(source_tbl, dest_table_name, con, ...){

  # 1. begin transaction
  # 2. create table
  # 4. Commit/Rollback based on error

  DBI::dbBegin(con)
  ops = try({
    create_query =
      dbplyr::sql_query_save(con = con,
                             sql = dbplyr::sql_render(source_tbl),
                             name = wrap_by_I(dest_table_name),
                             temporary = FALSE
                             )
    DBI::dbExecute(con, create_query)
  }, silent = TRUE)

  if (inherits(ops, "try-error")){
    DBI::dbRollback(con)
    return(ops)
  } else {
    DBI::dbCommit(con)
    return(TRUE)
  }
}

#' @keywords internal
#' @title write_df_overwrite
#' @description write_df_overwrite
write_df_overwrite = function(df, dest_table_name, con, ...){

  # 1. begin transaction
  # 2. truncate dest table
  # 3. append rows from source tbl
  # 4. Commit/Rollback based on error

  ops = try({
    DBI::dbBegin(con)
    DBI::dbExecute(con, glue::glue("DELETE FROM {dest_table_name}"))
    DBI::dbWriteTable(con, string_to_id(dest_table_name), df, append = TRUE)
  }, silent = TRUE)

  if (inherits(ops, "try-error")){
    DBI::dbRollback(con)
    return(ops)
  } else {
    DBI::dbCommit(con)
    return(TRUE)
  }
}

#' @keywords internal
#' @title write_df_overwrite_schema
#' @description write_df_overwrite_schema
write_df_overwrite_schema = function(df, dest_table_name, con, ...){

  ops = try({
    DBI::dbWriteTable(con,
                      string_to_id(dest_table_name),
                      df,
                      overwrite = TRUE
                      )
    }, silent = TRUE)

  if (inherits(ops, "try-error")){
    return(ops)
  } else {
    return(TRUE)
  }
}

#' @keywords internal
#' @title write_df_create
#' @description write_df_create
write_df_create = function(df, dest_table_name, con, ...){

  ops = try({
    DBI::dbWriteTable(con,
                      string_to_id(dest_table_name),
                      df
                      )
    }, silent = TRUE)

  if (!inherits(ops, "try-error")){
    return(ops)
  } else {
    return(TRUE)
  }
}

#' @keywords internal
#' @title write_df_create
#' @description write_df_create
write_df_append = function(df, dest_table_name, con, ...){

  ops = try({
    DBI::dbWriteTable(con,
                      string_to_id(dest_table_name),
                      df,
                      append = TRUE,
                      ...
                      )
    }, silent = TRUE)

  if (!inherits(ops, "try-error")){
    return(ops)
  } else {
    return(TRUE)
  }
}

#' @keywords internal
#' @title write_rows
#' @description write_rows
write_rows = function(source,
                      dest_table_name,
                      con,
                      mode = c("append",
                               "insert",
                               "update",
                               "upsert",
                               "patch",
                               "delete"
                               ),
                      ...
                      ){
  # source is either a tbl or dataframe
  fun = switch(mode,
    append = dplyr::rows_append,
    insert = dplyr::rows_insert,
    update = dplyr::rows_update,
    upsert = dplyr::rows_upsert,
    delete = dplyr::rows_delete,
    patch  = dplyr::rows_patch
  )

  dest_tbl = dplyr::tbl(con, wrap_by_I(dest_table_name))

  ops = try({
    fun(x = dest_tbl,
        y = source,
        copy = TRUE,
        in_place = TRUE,
        ...
        )
  }, silent = TRUE)

  if (inherits(ops, "try-error")){
    return(ops)
  } else {
    return(TRUE)
  }
}
