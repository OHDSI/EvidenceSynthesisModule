# Copyright 2022 Observational Health Data Sciences and Informatics
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

databaseFile <- "tests/results.sqlite"
if (file.exists(databaseFile)) {
  unlink(databaseFile)
}
connection <- DatabaseConnector::connect(dbms = "sqlite", server = databaseFile)

library(EvidenceSynthesis)
library(dplyr)

# targetId = 1; comparatorId = 2; outcomeId = 1; analysisId = 1; hazardRatio = 1; nSites = 10
simulateTco <- function(targetId, comparatorId, outcomeId, analysisId, hazardRatio = 1, nSites = 10) {
  simulationSettings <- createSimulationSettings(nSites = nSites,
                                                 n = 2500,
                                                 treatedFraction = 0.25,
                                                 hazardRatio = hazardRatio,
                                                 randomEffectSd = if_else(hazardRatio == 1, 0, 0.5))
  cmDiagnosticsSummary <- tibble(targetId = targetId,
                                 comparatorId = comparatorId,
                                 outcomeId = outcomeId,
                                 analysisId = analysisId,
                                 databaseId = seq_len(nSites),
                                 unblind = runif(nSites) < 0.9)

  populations <- simulatePopulations(simulationSettings)
  cmResult <- list()
  cmLikelihoodProfile <- list()
  # i = 1
  for (i in seq_along(populations)) {
    population <- populations[[i]]
    cyclopsData <- Cyclops::createCyclopsData(Surv(time, y) ~ x + strata(stratumId),
                                              data = population,
                                              modelType = "cox")
    cyclopsFit <- Cyclops::fitCyclopsModel(cyclopsData)
    ci <- tryCatch(
      {
        confint(cyclopsFit, parm = 1, includePenalty = TRUE)
      },
      error = function(e) {
        c(0, -Inf, Inf)
      }
    )
    normal <- approximateLikelihood(cyclopsFit, "x", approximation = "normal")
    adaptiveGrid <- approximateLikelihood(cyclopsFit, "x", approximation = "adaptive grid")
    z <- normal$logRr / normal$seLogRr
    p <- 2 * pmin(pnorm(z), 1 - pnorm(z))
    cmResult[[i]] <- tibble(targetId = targetId,
                            comparatorId = comparatorId,
                            outcomeId = outcomeId,
                            analysisId = analysisId,
                            databaseId = i,
                            targetSubjects = sum(population$x == 1),
                            comparatorSubjects = sum(population$x == 0),
                            targetDays = sum(population$time[population$x == 1]),
                            comparatorDays = sum(population$time[population$x == 0]),
                            targetOutcomes = sum(population$y[population$x == 1]),
                            comparatorOutcomes = sum(population$y[population$x == 0]),
                            rr = exp(normal$logRr),
                            ci95Lb = exp(ci[2]),
                            ci95Ub = exp(ci[3]),
                            p = p,
                            logRr = normal$logRr,
                            seLogRr = normal$seLogRr)
    cmLikelihoodProfile[[i]] <- adaptiveGrid %>%
      rename(logRr = .data$point,
             logLikelihood = .data$value) %>%
      mutate(targetId = targetId,
             comparatorId = comparatorId,
             outcomeId = outcomeId,
             analysisId = analysisId,
             databaseId = i)
  }
  cmResult <- bind_rows(cmResult)
  cmLikelihoodProfile <- bind_rows(cmLikelihoodProfile)
  tablesExist <- DatabaseConnector::existsTable(
    connection = connection,
    databaseSchema = "main",
    tableName = "cm_diagnostics_summary"
  )

  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = "main",
    tableName = "cm_diagnostics_summary",
    data = cmDiagnosticsSummary,
    createTable = !tablesExist,
    dropTableIfExists = FALSE,
    camelCaseToSnakeCase = TRUE
  )
  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = "main",
    tableName = "cm_result",
    data = cmResult,
    createTable = !tablesExist,
    dropTableIfExists = FALSE,
    camelCaseToSnakeCase = TRUE
  )
  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = "main",
    tableName = "cm_likelihood_profile",
    data = cmLikelihoodProfile,
    createTable = !tablesExist,
    dropTableIfExists = FALSE,
    camelCaseToSnakeCase = TRUE
  )
}

targetId <- 1
comparatorId <- 2
# outcomeId <- 1
for (outcomeId in 1:26) {
  message(sprintf("Simulating outcome %d", outcomeId))
  outcomeOfInterest <- outcomeId == 1
  trueEffectSize <- if_else(outcomeOfInterest, 2, 1)
  cmTargetComparatorOutcome <- tibble(targetId = targetId,
                                      comparatorId = comparatorId,
                                      outcomeId = outcomeId,
                                      trueEffectSize = trueEffectSize,
                                      outcomeOfInterest = outcomeOfInterest)
  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = "main",
    tableName = "cm_target_comparator_outcome",
    data = cmTargetComparatorOutcome,
    createTable = outcomeId == 1,
    dropTableIfExists = FALSE,
    camelCaseToSnakeCase = TRUE
  )
  for (analysisId in 1:4) {
    simulateTco(targetId, comparatorId, outcomeId, analysisId, hazardRatio = trueEffectSize)
  }
}
DatabaseConnector::disconnect(connection)
