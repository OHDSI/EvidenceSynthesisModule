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

evidenceSynthesisSourceCmGrid <- createEvidenceSynthesisSource(
  sourceMethod = "CohortMethod",
  likelihoodApproximation = "adaptive grid"
)

evidenceSynthesisSourceCmNormal <- createEvidenceSynthesisSource(
  sourceMethod = "CohortMethod",
  databaseIds = c(1, 2, 4),
  analysisIds = c(1, 3),
  likelihoodApproximation = "normal"
)

evidenceSynthesisSourceSccsGrid <- createEvidenceSynthesisSource(
  sourceMethod = "SelfControlledCaseSeries",
  likelihoodApproximation = "adaptive grid"
)

evidenceSynthesisSourceSccsNormal <- createEvidenceSynthesisSource(
  sourceMethod = "SelfControlledCaseSeries",
  databaseIds = c(1, 2, 4),
  analysisIds = c(1, 3),
  likelihoodApproximation = "normal"
)

fixedEffectsMetaAnalysisCm <- createFixedEffectsMetaAnalysis(
  evidenceSynthesisAnalysisId = 1,
  evidenceSynthesisSource = evidenceSynthesisSourceCmNormal
)

randomEffectsMetaAnalysisCm <- createRandomEffectsMetaAnalysis(
  evidenceSynthesisAnalysisId = 2,
  evidenceSynthesisSource = evidenceSynthesisSourceCmNormal
)

bayesianMetaAnalysisCm <- createBayesianMetaAnalysis(
  evidenceSynthesisAnalysisId = 3,
  evidenceSynthesisSource = evidenceSynthesisSourceCmGrid
)

fixedEffectsMetaAnalysisSccs <- createFixedEffectsMetaAnalysis(
  evidenceSynthesisAnalysisId = 4,
  evidenceSynthesisSource = evidenceSynthesisSourceSccsNormal
)

randomEffectsMetaAnalysisSccs <- createRandomEffectsMetaAnalysis(
  evidenceSynthesisAnalysisId = 5,
  evidenceSynthesisSource = evidenceSynthesisSourceSccsNormal
)

bayesianMetaAnalysisSccs <- createBayesianMetaAnalysis(
  evidenceSynthesisAnalysisId = 6,
  evidenceSynthesisSource = evidenceSynthesisSourceSccsGrid
)

evidenceSynthesisModuleSpecs <- createEvidenceSynthesisModuleSpecifications(
  evidenceSynthesisAnalysisList = list(
    fixedEffectsMetaAnalysisCm,
    randomEffectsMetaAnalysisCm,
    bayesianMetaAnalysisCm,
    fixedEffectsMetaAnalysisSccs,
    randomEffectsMetaAnalysisSccs,
    bayesianMetaAnalysisSccs
  ),
  esDiagnosticThresholds = createEsDiagnosticThresholds()
)

# Module Settings Spec ----------------------------
analysisSpecifications <- createEmptyAnalysisSpecificiations() %>%
  addModuleSpecifications(evidenceSynthesisModuleSpecs)

ParallelLogger::saveSettingsToJson(analysisSpecifications, "extras/analysisSpecifications.json")

executionSettings <- Strategus::createResultsExecutionSettings(
  resultsConnectionDetailsReference = "dummy",
  resultsDatabaseSchema = "dummy",
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
moduleExecutionSettings$resultsConnectionDetailsReference <- "dummy"
moduleExecutionSettings$resultsDatabaseSchema <- "main"
jobContext <- list(
  sharedResources = analysisSpecifications$sharedResources,
  settings = analysisSpecifications$moduleSpecifications[[moduleIndex]]$settings,
  moduleExecutionSettings = moduleExecutionSettings
)
saveRDS(jobContext, "tests/testJobContext.rds")
