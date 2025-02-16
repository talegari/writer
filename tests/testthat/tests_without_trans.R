# This file is part of 'writer' R package
library("testthat")

## append ----------------------------------------------------------------------
# Test for appending data to an existing table with matching schema
test_that("Append data to existing table", {
  con = DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))

  df = data.frame(id = 1:3, name = c("Alice", "Bob", "Charlie"))
  DBI::dbWriteTable(con, "new_table", df, overwrite = TRUE)

  append_df = data.frame(id = 4:5, name = c("Dave", "Eve"))
  write_table(append_df, "new_table", mode = "append", con = con,
              use_transaction = F
              )

  expect_equal(dplyr::tbl(con, "new_table") |> dplyr::collect(),
               rbind(df, append_df),
               ignore_attr = TRUE
               )
})

# Test for appending data to a non-existing table with matching schema
test_that("Append data to non-existing table", {
  con = DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))

  # Data for appending
  append_df = data.frame(id = 4:5, name = c("Dave", "Eve"))

  # Write the data
  expect_error(write_table(append_df, "new_table", mode = "append", con = con,
                           use_transaction = F
                           )
               )
})

## create ----------------------------------------------------------------------

test_that("create new table", {
  con = DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))

  df = data.frame(id = 1:3, name = c("Alice", "Bob", "Charlie"))
  write_table(df, "new_table", mode = "create", con = con, use_transaction = F)

  expect_equal(df,
               dplyr::tbl(con, "new_table") |>
                 dplyr::collect() |>
                 as.data.frame(),
               ignore_attr = FALSE
               )
})

test_that("create fails with existing table (append mode)", {
  con = DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))

  df = data.frame(id = 1:3, name = c("Alice", "Bob", "Charlie"))
  write_table(df, "new_table", mode = "create", con = con, use_transaction = F)

  # Expected error message
  expect_error(write_table(df, "new_table", mode = "create", con = con,
                           use_transaction = F))
})

## insert ----------------------------------------------------------------------
test_that("should not insert into non existing table", {
  con = DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))

  df = data.frame(id = 1:3, name = c("Alice", "Bob", "Charlie"))
  expect_error(write_table(df, "new_table", mode = "insert", con = con))
})

test_that("insert without conflicts", {
  con = DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))

  df = data.frame(id = 1:3, name = c("Alice", "Bob", "Charlie"))
  DBI::dbWriteTable(con, "new_table", df, overwrite = TRUE)

  insert_df = data.frame(id = 3:4, name = c("Dave", "Eve"))

  insert_df |>
    write_table("new_table",
                mode = "insert",
                con = con,
                by = "id",
                conflict = "ignore",
                use_transaction = F
                )

  after_insert_df =
    dplyr::tbl(con, "new_table") |>
    dplyr::collect() |>
    as.data.frame()

  expected_df = dplyr::rows_insert(df, insert_df,
                                   by = "id",
                                   conflict = "ignore"
                                   )
  expect_equal(after_insert_df, expected_df)
})

test_that("insert handles conflicts (ignore)", {
  con = DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))

  df = data.frame(id = 1:3, name = c("Alice", "Bob", "Charlie"))
  DBI::dbWriteTable(con, "new_table", df, overwrite = TRUE)

  insert_df = data.frame(id = 3, name = c("Dave"))

  insert_df |>
    write_table("new_table",
                mode = "insert",
                con = con,
                by = "id",
                conflict = "ignore",
                use_transaction = F
                )

  after_insert_df =
    dplyr::tbl(con, "new_table") |>
    dplyr::collect() |>
    as.data.frame()

  expect_equal(after_insert_df, df)
})

## update ----------------------------------------------------------------------
test_that("update", {
  con = DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))

  df = data.frame(id = 1:3, name = c("Alice", "Bob", "Charlie"))
  write_table(df, "new_table", mode = "create", con = con)

  # Update existing rows
  update_df = data.frame(id = c(1, 3), name = c("Alicia", "Charles"))
  write_table(update_df,
              "new_table",
              mode = "update",
              con = con,
              by = "id",
              use_transaction = F
              )

  updated_df = dplyr::tbl(con, "new_table") |>
    dplyr::collect()

  expect_equal(updated_df$name[updated_df$id == 1], "Alicia")
  expect_equal(updated_df$name[updated_df$id == 3], "Charles")

  # Update with unmatched rows ignored
  DBI::dbRemoveTable(con, "new_table")
  write_table(df, "new_table", mode = "create", con = con, use_transaction = F)

  update_df_unmatched = data.frame(id = c(1, 4), name = c("Alicia", "David"))
  write_table(update_df_unmatched,
              "new_table",
              mode = "update",
              con = con,
              by = "id",
              use_transaction = F
              )
  updated_df =
    dplyr::tbl(con, "new_table") |>
    dplyr::collect()

  expect_equal(updated_df$name[updated_df$id == 1], "Alicia")
  expect_false("David" %in% updated_df$name)

  # Update from tbl_lazy
  DBI::dbRemoveTable(con, "new_table")
  write_table(df, "new_table", mode = "create", con = con, use_transaction = F)

  update_tbl_name =
    df |>
    dplyr::filter(id %in% c(1,3)) |>
    dplyr::mutate(name = c("Alicia", "Charles")) |>
    write_table("update_tbl", mode = "create", con = con, use_transaction = F)

  update_tbl = dplyr::tbl(con, update_tbl_name)

  write_table(update_tbl,
              "new_table",
              mode = "update",
              con = con,
              by = "id",
              use_transaction = F
              )

  updated_df =
    dplyr::tbl(con, "new_table") |>
    dplyr::collect()
  expect_equal(updated_df$name[updated_df$id == 1], "Alicia")
  expect_equal(updated_df$name[updated_df$id == 3], "Charles")

  # Update with no matches
  update_df_no_match = data.frame(id = c(4, 5), name = c("David", "Eve"))
  write_table(update_df_no_match,
              "new_table",
              mode = "update",
              con = con,
              by = "id",
              use_transaction = F
              )
  updated_df =
    dplyr::tbl(con, "new_table") |>
    dplyr::collect()
  expect_equal(nrow(updated_df), 3) # No rows should be updated

  # Update with empty data frame
  empty_df = data.frame(id = integer(), name = character())
  write_table(empty_df,
              "new_table",
              mode = "update",
              con = con,
              by = "id",
              use_transaction = F
              )
  updated_df =
    dplyr::tbl(con, "new_table") |>
    dplyr::collect()
  expect_equal(nrow(updated_df), 3) # No rows should be updated
})

## upsert ----------------------------------------------------------------------
test_that("upsert", {
  con = DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))

  # with
  df_initial = data.frame(id = 1:3, name = c("Alice", "Bob", "Charlie"))
  DBI::dbWriteTable(con, "test_table", df_initial, overwrite = TRUE)
  DBI::dbExecute(con,
                 'CREATE UNIQUE INDEX unique_id_index ON test_table ("id");'
                 )

  # Upsert with new and existing IDs
  df_upsert = data.frame(id = c(2, 4), name = c("Bobby", "David"))
  write_table(df_upsert,
              "test_table",
              mode = "upsert",
              con = con,
              use_transaction = F,
              by = "id"
              )

  expect_equal(
    dplyr::tbl(con, "test_table") |>
      dplyr::arrange(id) |>
      dplyr::collect() |>
      as.data.frame(),
    data.frame(id = 1:4, name = c("Alice", "Bobby", "Charlie", "David"))
    )

  # with tbl
  DBI::dbRemoveTable(con, "test_table")
  DBI::dbWriteTable(con, "test_table", df_initial, overwrite = TRUE)
  DBI::dbExecute(con,
                 'CREATE UNIQUE INDEX unique_id_index ON test_table ("id");'
                 )
  write_table(df_upsert,
              "upsert_table",
              mode = "create",
              con = con,
              use_transaction = F
              )

  write_table(dplyr::tbl(con, "upsert_table"),
              "test_table",
              mode = "upsert",
              by = "id",
              use_transaction = F
              )

  expect_equal(
    dplyr::tbl(con, "test_table") |>
      dplyr::arrange(id) |>
      dplyr::collect() |>
      as.data.frame(),
    data.frame(id = 1:4, name = c("Alice", "Bobby", "Charlie", "David"))
    )
})

## patch -----------------------------------------------------------------------
test_that("patch", {
  con = DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))

  # df
  df = data.frame(id = 1:3,
                  name = c("Alice", "Bob", "Charlie"),
                  value = c(10, 20, 30)
                  )
  DBI::dbWriteTable(con, "new_table", df)

  df_patch = data.frame(id = c(1, 2),
                        name = c("alice", NA),
                        value = c(NA, 25)
                        )
  DBI::dbWriteTable(con, "patch_table", df_patch)



  # expected_df = data.frame(id = 1:2,
  #                          name = c("alice", "Bob"),
  #                          value = c(10, 25)
  #                          )

  # expect_equal(
  #   dplyr::tbl(con, "patch_table") |>
  #   dplyr::collect() |>
  #   as.data.frame(),
  #   expected_df
  # )

  expect_error(
    write_table(df,
              "patch_table",
              mode = "patch",
              con = con,
              by = "id",
              unmatched = "ignore",
              use_transaction = F
              )
  )

  # tbl
  DBI::dbRemoveTable(con, "patch_table")
  DBI::dbWriteTable(con, "patch_table", df_patch)

  expect_error(
    write_table(dplyr::tbl(con, "new_table"),
                "patch_table",
                mode = "patch",
                con = con,
                by = "id",
                unmatched = "ignore",
                use_transaction = F
                )
  )
  # expect_equal(
  #   dplyr::tbl(con, "patch_table") |>
  #   dplyr::collect() |>
  #   as.data.frame(),
  #   expected_df
  # )

})

## delete ----------------------------------------------------------------------
test_that("delete mode works correctly", {
  con = DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))

  df = data.frame(id = 1:3, name = c("Alice", "Bob", "Charlie"))
  DBI::dbWriteTable(con, "test_table", df, overwrite = TRUE)

  # df
  delete_df = data.frame(id = c(2, 3))
  write_table(delete_df,
              "test_table",
              mode = "delete",
              con = con,
              by = "id",
              use_transaction = F
              )

  # Check the result
  result_df =
    dplyr::tbl(con, "test_table") |>
    dplyr::collect() |>
    as.data.frame()

  expected_df = data.frame(id = 1, name = "Alice")
  expect_equal(result_df, expected_df)

  # tbl
  DBI::dbWriteTable(con, "delete_table", delete_df)
  write_table(dplyr::tbl(con, "delete_table"),
              "test_table",
              mode = "delete",
              con = con,
              by = "id",
              use_transaction = F
              )

  # Check the result
  result_df =
    dplyr::tbl(con, "test_table") |>
    dplyr::collect() |>
    as.data.frame()

  expected_df = data.frame(id = 1, name = "Alice")
  expect_equal(result_df, expected_df)
})

## overwrite -------------------------------------------------------------------
test_that("overwrite", {
  con = DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))

  df = data.frame(id = 1:3, name = c("Alice", "Bob", "Charlie"))
  DBI::dbWriteTable(con, "test_table", df)

  # df
  overwrite_df = data.frame(id = c(2, 6), name = c("Bobby", "Frank"))
  write_table(overwrite_df,
              "test_table",
              mode = "overwrite",
              con = con,
              use_transaction = F
              )

  expect_equal(dplyr::tbl(con, "test_table") |>
                 dplyr::collect() |>
                 as.data.frame(),
               overwrite_df
               )

  # tbl
  DBI::dbWriteTable(con, "test_table", df, overwrite = TRUE)
  DBI::dbWriteTable(con, "or_table", overwrite_df)

  write_table(dplyr::tbl(con, "or_table"),
              "test_table",
              mode = "overwrite",
              con = con,
              use_transaction = F
              )
  expect_equal(dplyr::tbl(con, "test_table") |>
                 dplyr::collect() |>
                 as.data.frame(),
               overwrite_df
               )


  # append fails when table does not exist
  expect_error(write_table(overwrite_df,
                           "non_existent_table",
                           mode = "overwrite",
                           con = con,
                           use_transaction = F
                           )
               )
})

## overwrite_schema ------------------------------------------------------------
test_that("overwrite_schema", {
  con = DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))

  # df with different schema
  df = data.frame(id = 1:3,
                  name = c("Alice", "Bob", "Charlie")
                  )
  DBI::dbWriteTable(con, "test_table", df)

  os_df = data.frame(id = 4:5,
                     name = c("Dave", "Eve"),
                     age = c(25, 30)
                     )
  write_table(os_df,
              "test_table",
              mode = "overwrite_schema",
              con = con,
              use_transaction = F
              )

  expect_true(DBI::dbExistsTable(con, "test_table"))
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) as c FROM test_table")$c,
               2
               )
  expect_equal(colnames(dplyr::tbl(con, "test_table")),
               c("id", "name", "age")
               )
  expect_equal(DBI::dbGetQuery(con, "SELECT * FROM test_table"),
               os_df,
               ignore_attr = TRUE
               )

  # tbl with diff schema
  DBI::dbWriteTable(con, "test_table", df, overwrite = TRUE)

  DBI::dbWriteTable(con, "new_table", os_df)
  write_table(dplyr::tbl(con, "new_table"),
              "test_table",
              mode = "overwrite_schema",
              con = con,
              use_transaction = F
              )

  expect_true(DBI::dbExistsTable(con, "test_table"))
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) as c FROM test_table")$c,
               2
               )
  expect_equal(colnames(dplyr::tbl(con, "test_table")),
               c("id", "name", "age")
               )
  expect_equal(DBI::dbGetQuery(con, "SELECT * FROM test_table"),
               os_df,
               ignore_attr = TRUE
               )

  # write empty data with a diff schema

  # df
  DBI::dbWriteTable(con, "test_table", head(df, 0), overwrite = TRUE)

  os_df = data.frame(id = 4:5,
                     name = c("Dave", "Eve"),
                     age = c(25, 30)
                     )
  write_table(head(os_df, 0),
              "test_table",
              mode = "overwrite_schema",
              con = con,
              use_transaction = F
              )

  expect_true(DBI::dbExistsTable(con, "test_table"))
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) as c FROM test_table")$c,
               0
               )
  expect_equal(colnames(dplyr::tbl(con, "test_table")),
               c("id", "name", "age")
               )
  expect_equal(DBI::dbGetQuery(con, "SELECT * FROM test_table"),
               head(os_df, 0),
               ignore_attr = TRUE
               )

  # tbl with diff schema
  DBI::dbWriteTable(con, "test_table", df, overwrite = TRUE)

  DBI::dbWriteTable(con, "new_table", head(os_df, 0), overwrite = TRUE)
  write_table(dplyr::tbl(con, "new_table"),
              "test_table",
              mode = "overwrite_schema",
              con = con,
              use_transaction = F
              )

  expect_true(DBI::dbExistsTable(con, "test_table"))
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) as c FROM test_table")$c,
               0
               )
  expect_equal(colnames(dplyr::tbl(con, "test_table")),
               c("id", "name", "age")
               )
  expect_equal(DBI::dbGetQuery(con, "SELECT * FROM test_table"),
               head(os_df, 0),
               ignore_attr = TRUE
               )
})

