# Use this profile when testing
# Sys.setenv(RENV_PROFILE = "dev")
# renv::restore(prompt = FALSE)
library(testthat)
source("SettingsFunctions.R")

test_that("Throw error when combining non-normal ll approximation with random-effects model", {
  esSource <- createEvidenceSynthesisSource(likelihoodApproximation = "adaptive grid")
  expect_error(
    createRandomEffectsMetaAnalysis(evidenceSynthesisSource = esSource),
    "only supports normal approximation"
  )
})
