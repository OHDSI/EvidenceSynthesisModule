# Copyright 2024 Observational Health Data Sciences and Informatics
#
# This file is part of EvidenceSynthesisModule
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Adding library references that are required for Strategus
library(CohortGenerator)
library(DatabaseConnector)
library(keyring)
library(ParallelLogger)
library(SqlRender)

# Adding RSQLite so that we can test modules with Eunomia
library(RSQLite)

# Load helper functions
source("EvidenceSynthesisFunctions.R")

# Module methods -------------------------
execute <- function(jobContext) {
  checkmate::assertList(x = jobContext)
  if (is.null(jobContext$settings)) {
    stop("Analysis settings not found in job context")
  }
  if (is.null(jobContext$moduleExecutionSettings)) {
    stop("Execution settings not found in job context")
  }

  writeAnalysisSpecs(
    analysisSpecs = jobContext$settings$evidenceSynthesisAnalysisList,
    resultsFolder = jobContext$moduleExecutionSettings$resultsSubFolder
  )

  executeEvidenceSynthesis(
    connectionDetails = jobContext$moduleExecutionSettings$resultsConnectionDetails,
    databaseSchema = jobContext$moduleExecutionSettings$resultsDatabaseSchema,
    settings = jobContext$settings$evidenceSynthesisAnalysisList,
    esDiagnosticThresholds = jobContext$settings$esDiagnosticThresholds,
    resultsFolder = jobContext$moduleExecutionSettings$resultsSubFolder,
    minCellCount = jobContext$moduleExecutionSettings$minCellCount
  )

  file.copy("resultsDataModelSpecification.csv", file.path(jobContext$moduleExecutionSettings$resultsSubFolder, "resultsDataModelSpecification.csv"))
  invisible(NULL)
}
