library(testthat)

connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "sqlite",
  server = "tests/results.sqlite"
)

workFolder <- tempfile("work")
dir.create(workFolder)
resultsfolder <- tempfile("results")
dir.create(resultsfolder)
jobContext <- readRDS("tests/testJobContext.rds")
jobContext$moduleExecutionSettings$workSubFolder <- workFolder
jobContext$moduleExecutionSettings$resultsSubFolder  <- resultsfolder
jobContext$moduleExecutionSettings$resultsConnectionDetails <- connectionDetails

test_that("Run module", {
  source("Main.R")
  execute(jobContext)
  resultsFiles <- list.files(resultsfolder)
  expect_true("es_analysis.csv" %in% resultsFiles)
  expect_true("es_cm_result.csv" %in% resultsFiles)
  expect_true("es_cm_diagnostics_summary.csv" %in% resultsFiles)
})

test_that("Skipped analyses as specified", {
  # We specified we didn't want cohort method analysis ID 2 in evidence synthesis ID 2:
  results <- CohortGenerator::readCsv(file.path(resultsfolder, "es_cm_result.csv"))
  expect_false(any(results$evidenceSynthesisAnalysisId == 1 & results$analysisId == 2))
})

test_that("Output conforms to results model", {
  model <-  CohortGenerator::readCsv(file.path(resultsfolder, "resultsDataModelSpecification.csv"))
  tables <- unique(model$tableName)
  for (table in tables) {
    data <- readr::read_csv(file.path(resultsfolder, sprintf("%s.csv", table)), show_col_types = FALSE)
    observed <- colnames(data)
    observed <- sort(observed)
    expected <- model$columnName[model$tableName == table]
    expected <- sort(expected)
    expect_equal(observed, expected)
  }
})

# readr::write_csv(OhdsiRTools::createResultsSchemaStub(resultsfolder), "resultsDataModelSpecification.csv")

unlink(workFolder)
unlink(resultsfolder)
