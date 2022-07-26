connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                                server = paste(keyring::key_get("assureServer"),
                                                                               keyring::key_get("assureDatabase"),
                                                                               sep = "/"),
                                                                user = keyring::key_get("assureUser"),
                                                                password = keyring::key_get("assurePassword"))
resultsDatabaseSchema <- "poc"
workSubFolder <- "s:/temp/workSubFolder"
resultsSubFolder <- "s:/temp/resultsSubFolder"
# dir.create(workSubFolder)
# dir.create(resultsSubFolder)

# connection <- DatabaseConnector::connect(connectionDetails)

moduleExecutionSettings <- list(
  workSubFolder = workSubFolder,
  resultsSubFolder = resultsSubFolder,
  resultsConnectionDetails = connectionDetails,
  resultsDatabaseSchema = resultsDatabaseSchema,
  minCellCount = 5
)

settings <- ParallelLogger::loadSettingsFromJson("extras/LocalTest/evidenceSynthesisAnalysisSpecifications.json")
settings <- settings$moduleSpecifications[[1]]$settings

jobContext <- list(
  sharedResources = list(),
  settings = settings,
  moduleExecutionSettings = moduleExecutionSettings
)

source("Main.R")
execute(jobContext)
