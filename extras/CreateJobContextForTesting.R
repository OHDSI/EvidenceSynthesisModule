# Create a job context for testing purposes

library(Strategus)
library(dplyr)
source("SettingsFunctions.R")

# Generic Helpers ----------------------------
getModuleInfo <- function() {
  checkmate::assert_file_exists("MetaData.json")
  return(ParallelLogger::loadSettingsFromJson("MetaData.json"))
}

# Create EvidenceSynthesisModule settings ---------------------------------------

evidenceSynthesisSource1 <- createEvidenceSynthesisSource(
  sourceMethod = "CohortMethod",
  likelihoodApproximation = "adaptive grid"
)

evidenceSynthesisSource2 <- createEvidenceSynthesisSource(
  sourceMethod = "CohortMethod",
  databaseIds = c(1,2,4),
  analysisIds = c(1,3),
  likelihoodApproximation = "normal"
)

fixedEffectsMetaAnalysis <- createFixedEffectsMetaAnalysis(
  evidenceSynthesisAnalysisId = 1,
  evidenceSynthesisSource = evidenceSynthesisSource2)

randomEffectsMetaAnalysis <- createRandomEffectsMetaAnalysis(
  evidenceSynthesisAnalysisId = 2,
  evidenceSynthesisSource = evidenceSynthesisSource2)

bayesianMetaAnalysis <- createBayesianMetaAnalysis(
  evidenceSynthesisAnalysisId = 3,
  evidenceSynthesisSource = evidenceSynthesisSource1)

evidenceSynthesisModuleSpecs <- createEvidenceSynthesisModuleSpecifications(
  evidenceSynthesisAnalysisList = list(fixedEffectsMetaAnalysis,
                                       randomEffectsMetaAnalysis,
                                       bayesianMetaAnalysis))

# Module Settings Spec ----------------------------
analysisSpecifications <- createEmptyAnalysisSpecificiations() %>%
  addModuleSpecifications(evidenceSynthesisModuleSpecs)

ParallelLogger::saveSettingsToJson(analysisSpecifications, "extras/analysisSpecifications.json")

executionSettings <- Strategus::createExecutionSettings(
  connectionDetailsReference = "dummy",
  workDatabaseSchema = "main",
  cdmDatabaseSchema = "main",
  cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable = "cohort"),
  workFolder = "dummy",
  resultsFolder = "dummy",
  minCellCount = 5
)

# Job Context ----------------------------
module <- "EvidenceSynthesisModule"
moduleIndex <- 1
moduleExecutionSettings <- executionSettings
moduleExecutionSettings$workSubFolder <- "dummy"
moduleExecutionSettings$resultsSubFolder <- "dummy"
moduleExecutionSettings$resultsConnectionDetailsReference = "dummy"
moduleExecutionSettings$resultsDatabaseSchema = "main"
jobContext <- list(sharedResources = analysisSpecifications$sharedResources,
                   settings = analysisSpecifications$moduleSpecifications[[moduleIndex]]$settings,
                   moduleExecutionSettings = moduleExecutionSettings)
saveRDS(jobContext, "tests/testJobContext.rds")

