connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                                server = paste(keyring::key_get("assureServer"),
                                                                               keyring::key_get("assureDatabase"),
                                                                               sep = "/"),
                                                                user = keyring::key_get("assureUser"),
                                                                password = keyring::key_get("assurePassword"))
resultsDatabaseSchema <- "sena_test"
workSubFolder <- "d:/temp/workSubFolder"
resultsSubFolder <- "d:/temp/resultsSubFolder"
# dir.create(workSubFolder)
# dir.create(resultsSubFolder)

# connection <- DatabaseConnector::connect(connectionDetails)
querySql(connection, "SELECT DISTINCT analysis_id FROM sena_test.sccs_result;")

# Create analysis specifications -----------------------------------------------
library(Strategus)
library(dplyr)
source("SettingsFunctions.R")

getModuleInfo <- function() {
  checkmate::assert_file_exists("MetaData.json")
  return(ParallelLogger::loadSettingsFromJson("MetaData.json"))
}

# evidenceSynthesisSourceCmGrid <- createEvidenceSynthesisSource(
#   sourceMethod = "CohortMethod",
#   likelihoodApproximation = "adaptive grid"
# )
#
# evidenceSynthesisSourceCmNormal <- createEvidenceSynthesisSource(
#   sourceMethod = "CohortMethod",
#   databaseIds = c(1,2,4),
#   analysisIds = c(1,3),
#   likelihoodApproximation = "normal"
# )

evidenceSynthesisSourceSccsGrid <- createEvidenceSynthesisSource(
  sourceMethod = "SelfControlledCaseSeries",
  likelihoodApproximation = "adaptive grid"
)

evidenceSynthesisSourceSccsNormal <- createEvidenceSynthesisSource(
  sourceMethod = "SelfControlledCaseSeries",
  databaseIds = c(-948018521),
  analysisIds = c(1,3),
  likelihoodApproximation = "normal"
)

# fixedEffectsMetaAnalysisCm <- createFixedEffectsMetaAnalysis(
#   evidenceSynthesisAnalysisId = 1,
#   evidenceSynthesisSource = evidenceSynthesisSourceCmNormal)
#
# randomEffectsMetaAnalysisCm <- createRandomEffectsMetaAnalysis(
#   evidenceSynthesisAnalysisId = 2,
#   evidenceSynthesisSource = evidenceSynthesisSourceCmNormal)
#
# bayesianMetaAnalysisCm <- createBayesianMetaAnalysis(
#   evidenceSynthesisAnalysisId = 3,
#   evidenceSynthesisSource = evidenceSynthesisSourceCmGrid)

fixedEffectsMetaAnalysisSccs <- createFixedEffectsMetaAnalysis(
  evidenceSynthesisAnalysisId = 4,
  evidenceSynthesisSource = evidenceSynthesisSourceSccsNormal)

randomEffectsMetaAnalysisSccs <- createRandomEffectsMetaAnalysis(
  evidenceSynthesisAnalysisId = 5,
  evidenceSynthesisSource = evidenceSynthesisSourceSccsNormal)

bayesianMetaAnalysisSccs <- createBayesianMetaAnalysis(
  evidenceSynthesisAnalysisId = 6,
  evidenceSynthesisSource = evidenceSynthesisSourceSccsGrid)

evidenceSynthesisModuleSpecs <- createEvidenceSynthesisModuleSpecifications(
  evidenceSynthesisAnalysisList = list(#fixedEffectsMetaAnalysisCm,
                                       #randomEffectsMetaAnalysisCm,
                                       #bayesianMetaAnalysisCm,
                                       fixedEffectsMetaAnalysisSccs,
                                       randomEffectsMetaAnalysisSccs,
                                       bayesianMetaAnalysisSccs))

analysisSpecifications <- createEmptyAnalysisSpecificiations() %>%
  addModuleSpecifications(evidenceSynthesisModuleSpecs)

ParallelLogger::saveSettingsToJson(analysisSpecifications, "extras/LocalTest/evidenceSynthesisAnalysisSpecifications.json")

# Create job context and run ---------------------------------------------------
settings <- ParallelLogger::loadSettingsFromJson("extras/LocalTest/evidenceSynthesisAnalysisSpecifications.json")
settings <- settings$moduleSpecifications[[1]]$settings

moduleExecutionSettings <- list(
  workSubFolder = workSubFolder,
  resultsSubFolder = resultsSubFolder,
  resultsConnectionDetails = connectionDetails,
  resultsDatabaseSchema = resultsDatabaseSchema,
  minCellCount = 5
)

jobContext <- list(
  sharedResources = list(),
  settings = settings,
  moduleExecutionSettings = moduleExecutionSettings
)

source("Main.R")
execute(jobContext)
