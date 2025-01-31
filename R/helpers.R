# This file is part of 'writer' R package
# helper functions
# None of these functions are exported

#' @keywords internal
#' @title id_to_string
#' @description DBI id to simple string
#' @noRd
id_to_string = function(x){
  res = paste(x@name, collapse = ".")
  return(res)
}

#' @keywords internal
#' @title string to_id
#' @description simple string to DBI id
string_to_id = function(x){
  split = strsplit(x, "\\.")
  return(do.call(DBI::Id, as.list(split)))
}

#' @keywords internal
#' @title wrap by `I()`
#' @description wraps using a separate function as cannot be done literally
wrap_by_I = function(string){
  I(string)
}

#' @keywords internal
#' @title Validate table name
#' @description Handles string, `I()` or `Id` objects and returns a string
validate_table_name = function(name){

  if (!is.character(name) &&
      length(name) == 1 &&
      !inherits(name, c("AsIs", "Id"))
      ){
    msg = paste0("'table_name' should be one among these: a string, AsIs",
                 " (created by `I()`) or `Id` (created by `DBI::Id`)"
                 )
    cli::cli_abort(msg, class = "error_input")
  }

  if (inherits(name, "AsIs")){
    res = as.character(name)
  } else if (inherits(name, "Id")){
    res = id_to_string(name)
  } else {
    res = name
  }

  return(res)
}
