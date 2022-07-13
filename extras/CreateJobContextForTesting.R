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

evidenceSynthesisSource <- createEvidenceSynthesisSource(
  sourceMethod = "CohortMethod",
  likelihoodApproximation = "adaptive grid"
)

fixedEffectsMetaAnalysis <- createFixedEffectsMetaAnalysis(
  evidenceSynthesisAnalysisId = 1,
  evidenceSynthesisSource = evidenceSynthesisSource)


bayesianMetaAnalysis <- createBayesianMetaAnalysis(
  evidenceSynthesisAnalysisId = 1,
  evidenceSynthesisSource = evidenceSynthesisSource)

evidenceSynthesisModuleSpecs <- createEvidenceSynthesisModuleSpecifications(
  evidenceSynthesisAnalysisList = list(fixedEffectsMetaAnalysis,
                                       bayesianMetaAnalysis))

# Module Settings Spec ----------------------------
analysisSpecifications <- createEmptyAnalysisSpecificiations() %>%
  addModuleSpecifications(evidenceSynthesisModuleSpecs)

ParallelLogger::saveSettingsToJson(analysisSpecifications, "extras/analysisSpecifications.json")

executionSettings <- Strategus::createExecutionSettings(connectionDetailsReference = "dummy",
                                                        workDatabaseSchema = "main",
                                                        cdmDatabaseSchema = "main",
                                                        cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable = "cohort"),
                                                        workFolder = "dummy",
                                                        resultsFolder = "dummy",
                                                        minCellCount = 5)

# Job Context ----------------------------
module <- "EvidenceSynthesisModule"
moduleIndex <- 1
moduleExecutionSettings <- executionSettings
moduleExecutionSettings$workSubFolder <- "dummy"
moduleExecutionSettings$resultsSubFolder <- "dummy"
moduleExecutionSettings$databaseId <- 123
jobContext <- list(sharedResources = analysisSpecifications$sharedResources,
                   settings = analysisSpecifications$moduleSpecifications[[moduleIndex]]$settings,
                   moduleExecutionSettings = moduleExecutionSettings)
saveRDS(jobContext, "tests/testJobContext.rds")

