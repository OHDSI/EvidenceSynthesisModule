library(testthat)

connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "sqlite",
  server = "tests/results.sqlite"
)

workFolder <- tempfile("work")
dir.create(workFolder)
resultsFolder <- tempfile("results")
dir.create(resultsFolder)
jobContext <- readRDS("tests/testJobContext.rds")
jobContext$moduleExecutionSettings$workSubFolder <- workFolder
jobContext$moduleExecutionSettings$resultsSubFolder  <- resultsFolder
jobContext$moduleExecutionSettings$resultsConnectionDetails <- connectionDetails

test_that("Run module", {
  source("Main.R")
  execute(jobContext)
  resultsFiles <- list.files(resultsFolder)
  expect_true("es_analysis.csv" %in% resultsFiles)
  expect_true("es_cm_result.csv" %in% resultsFiles)
  expect_true("es_cm_diagnostics_summary.csv" %in% resultsFiles)
})

test_that("Skipped analyses as specified", {
  # We specified we didn't want cohort method analysis ID 2 in evidence synthesis ID 2:
  results <- CohortGenerator::readCsv(file.path(resultsFolder, "es_cm_result.csv"))
  expect_false(any(results$evidenceSynthesisAnalysisId == 2 & results$analysisId == 2))
})

test_that("Output conforms to results model", {
  model <-  CohortGenerator::readCsv(file.path(resultsFolder, "resultsDataModelSpecification.csv"))
  tables <- unique(model$tableName)
  for (table in tables) {
    data <- readr::read_csv(file.path(resultsFolder, sprintf("%s.csv", table)), show_col_types = FALSE)
    observed <- colnames(data)
    observed <- sort(observed)
    expected <- model$columnName[model$tableName == table]
    expected <- sort(expected)
    expect_equal(observed, expected)
  }
})

test_that("Throw warning if no unblinded estimates found", {
  tempJobContext <- jobContext
  tempJobContext$settings <- list(tempJobContext$settings[[1]])
  tempJobContext$settings[[1]]$evidenceSynthesisSource$databaseIds <- c(-999, -998, -997)
  expect_warning(execute(tempJobContext), "No unblinded estimates found")
})

test_that("Don't error when no negative controls present", {
  # Create dataset without negative controls
  tempFile <- tempfile(fileext = ".sqlite")
  file.copy("tests/results.sqlite", tempFile)
  on.exit(unlink(tempFile))
  tempConnectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "sqlite",
    server = tempFile
  )
  connection <- DatabaseConnector::connect(tempConnectionDetails)
  DatabaseConnector::renderTranslateExecuteSql(connection, "UPDATE cm_target_comparator_outcome SET true_effect_size = NULL;")
  DatabaseConnector::disconnect(connection)

  tempJobContext <- jobContext
  tempJobContext$settings <- list(tempJobContext$settings[[1]])
  tempJobContext$moduleExecutionSettings$resultsConnectionDetails <- tempConnectionDetails
  execute(tempJobContext)

  estimates <- readr::read_csv(file.path(resultsFolder, "es_cm_result.csv"), show_col_types = FALSE)
  expect_gt(nrow(estimates), 0)
  expect_true(all(is.na(estimates$calibrated_rr)))
})


# readr::write_csv(OhdsiRTools::createResultsSchemaStub(resultsFolder), "resultsDataModelSpecification.csv")

unlink(workFolder)
unlink(resultsFolder)
