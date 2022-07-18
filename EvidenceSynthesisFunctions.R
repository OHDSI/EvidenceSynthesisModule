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

library(dplyr)

writeAnalysisSpecs <- function(analysisSpecs, resultsFolder) {
  message("Writing evidence synthesis analysis specifications")
  tempFileName <- tempfile()
  evidenceSynthesisAnalysis <- tibble()
  for (analysisSettings in analysisSpecs) {
    ParallelLogger::saveSettingsToJson(analysisSettings, tempFileName)
    analysis <- tibble(
      evidenceSynthesisAnalysisId = analysisSettings$evidenceSynthesisAnalysisId,
      evidenceSynthesisDescription =  analysisSettings$evidenceSynthesisDescription,
      sourceMethod = analysisSettings$evidenceSynthesisSource$sourceMethod,
    ) %>%
      mutate(definition = readChar(tempFileName, file.info(tempFileName)$size))
    evidenceSynthesisAnalysis <- bind_rows(evidenceSynthesisAnalysis, analysis)
  }
  unlink(tempFileName)
  fileName <- file.path(resultsFolder, "es_analysis.csv")
  CohortGenerator::writeCsv(evidenceSynthesisAnalysis, fileName)
}

executeEvidenceSynthesis <- function(connectionDetails, databaseSchema, settings, resultsFolder, minCellCount) {
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  if (file.exists(file.path(resultsFolder, "es_cm_result.csv"))){
    unlink(file.path(resultsFolder, "es_cm_result.csv"))
  }
  if (file.exists(file.path(resultsFolder, "es_cm_diagnostics_summary.csv"))){
    unlink(file.path(resultsFolder, "es_cm_diagnostics_summary.csv"))
  }

  message("Performing evidence synthesis across databases")
  invisible(lapply(
    X = settings,
    FUN = doAnalysis,
    connection = connection,
    databaseSchema = databaseSchema,
    resultsFolder = resultsFolder,
    minCellCount = minCellCount)
  )
}

# analysisSettings = settings[[1]]
doAnalysis <- function(analysisSettings, connection, databaseSchema, resultsFolder, minCellCount) {
  perDbEstimates <- getPerDatabaseEstimates(
    connection = connection,
    databaseSchema = databaseSchema,
    evidenceSynthesisSource = analysisSettings$evidenceSynthesisSource
  )
  fullKeys <- perDbEstimates$estimates[, c(perDbEstimates$key, "analysisId")] %>%
    distinct()

  cluster <- ParallelLogger::makeCluster(min(10, parallel::detectCores()))
  ParallelLogger::clusterRequire(cluster, "dplyr")
  on.exit(ParallelLogger::stopCluster(cluster))

  message(sprintf("Performing analysis %s (%s)", analysisSettings$evidenceSynthesisAnalysisId, analysisSettings$evidenceSynthesisDescription))
  estimates <- ParallelLogger::clusterApply(
    cluster = cluster,
    x = split(fullKeys, seq_len(nrow(fullKeys))),
    fun = doSingleEvidenceSynthesis,
    perDbEstimates = perDbEstimates,
    analysisSettings = analysisSettings,
    minCellCount = minCellCount)
  estimates <- bind_rows(estimates)

  message("- Calibrating estimates")
  estimates <- estimates %>%
    inner_join(perDbEstimates$trueEffectSizes, by = perDbEstimates$key)
  if (analysisSettings$controlType == "outcome") {
    controlKey <- c(perDbEstimates$key, "analysisId")
    controlKey <- controlKey[controlKey != "outcomeId"]
    groupKeys <- estimates[, controlKey]
    groupKeys <- apply(groupKeys, 1, paste, collapse = "_")
  } else if (analysisSettings$controlType == "exposure") {
    controlKey <- c("outcomeId", "analysisId")
    groupKeys <- estimates[, controlKey]
    groupKeys <- apply(groupKeys, 1, paste, collapse = "_")
  } else {
    stop(sprintf("Unknown control type '%s'", analysisSettings$controlType))
  }
  estimates <- ParallelLogger::clusterApply(
    cluster = cluster,
    x = split(estimates, groupKeys),
    fun = calibrateEstimates)
  estimates <- bind_rows(estimates) %>%
    mutate(evidenceSynthesisAnalysisId = analysisSettings$evidenceSynthesisAnalysisId)

  # Save diagnostics
  diagnostics <- estimates[, c(perDbEstimates$key, "analysisId", "i2", "tau", "ease")] %>%
    mutate(i2Diagnostic = ifelse(!is.na(i2) & i2 > 0.4, "FAIL", "PASS")) %>%
    mutate(tauDiagnostic = ifelse(!is.na(tau) & tau > log(2), "FAIL", "PASS")) %>%
    mutate(easeDiagnostic = case_when(
      abs(.data$ease) < 0.1 ~ "PASS",
      abs(.data$ease) < 0.25 ~ "WARNING",
      TRUE ~ "FAIL"
    )) %>%
    mutate(unblind = ifelse(.data$i2Diagnostic != "FAIL" &
                              .data$tauDiagnostic != "FAIL" &
                              .data$easeDiagnostic != "FAIL", 1, 0))
  if (analysisSettings$evidenceSynthesisSource$sourceMethod == "CohortMethod") {
    fileName <- file.path(resultsfolder, "es_cm_diagnostics_summary.csv")
  } else {
    stop(sprintf("Saving diagnostics summary not implemented for source method '%s'", analysisSettings$evidenceSynthesisSource$sourceMethod))
  }
  writeToCsv(data = diagnostics, fileName = fileName, append = file.exists(fileName))

  # Save estimates
  estimates <- estimates  %>%
    select(-.data$trueEffectSize, -.data$outcomeOfInterest, -.data$ease, -.data$i2, -.data$tau)

  if (analysisSettings$evidenceSynthesisSource$sourceMethod == "CohortMethod") {
    fileName <- file.path(resultsfolder, "es_cm_result.csv")
  } else {
    stop(sprintf("Saving results not implemented for source method '%s'", analysisSettings$evidenceSynthesisSource$sourceMethod))
  }
  writeToCsv(data = estimates, fileName = fileName, append = file.exists(fileName))
}

# group = split(estimates, groupKeys)[[1]]
calibrateEstimates <- function(group) {
  ncs <- group[group$trueEffectSize == 1 & !is.na(group$seLogRr), ]
  pcs <- group[!is.na(group$trueEffectSize) & group$trueEffectSize != 1 & !is.na(group$seLogRr), ]
  if (nrow(ncs) >= 5) {
    null <- EmpiricalCalibration::fitMcmcNull(logRr = ncs$logRr, seLogRr = ncs$seLogRr)
    ease <- EmpiricalCalibration::computeExpectedAbsoluteSystematicError(null)
    calibratedP <- EmpiricalCalibration::calibrateP(null = null, logRr = group$logRr, seLogRr = group$seLogRr)
    if (nrow(pcs) >= 5) {
      model <- EmpiricalCalibration::fitSystematicErrorModel(
        logRr = c(ncs$logRr, pcs$logRr),
        seLogRr = c(ncs$seLogRr, pcs$seLogRr),
        trueLogRr = log(c(ncs$trueEffectSize, pcs$trueEffectSize)),
        estimateCovarianceMatrix = FALSE
      )
    } else {
      model <- EmpiricalCalibration::convertNullToErrorModel(null)
    }
    calibratedCi <- EmpiricalCalibration::calibrateConfidenceInterval(model = model, logRr = group$logRr, seLogRr = group$seLogRr)
    group$calibratedRr <- exp(calibratedCi$logRr)
    group$calibratedCi95Lb <- exp(calibratedCi$logLb95Rr)
    group$calibratedCi95Ub <- exp(calibratedCi$logUb95Rr)
    group$calibratedP <- calibratedP$p
    group$calibratedLogRr <- calibratedCi$logRr
    group$calibratedSeLogRr <- calibratedCi$seLogRr
    group$ease <- ease$ease
  } else {
    group$calibratedRr <- NA
    group$calibratedCi95Lb <- NA
    group$calibratedCi95Ub <- NA
    group$calibratedP <- NA
    group$calibratedLogRr <- NA
    group$calibratedSeLogRr <- NA
    group$ease <- NA
  }
  return(group)
}

# row <- split(fullKeys, seq_len(nrow(fullKeys)))[[1]]
doSingleEvidenceSynthesis <- function(row, perDbEstimates, analysisSettings, minCellCount) {
  sumMinCellCount <- function(counts, minCellCount) {
    if (length(counts) == 0) {
      return(NA)
    }
    hasNegative <- any(counts < 0)
    sumCount <- sum(abs(counts))
    if (sumCount == 0) {
      return(sumCount)
    }
    if (hasNegative) {
      if (sumCount < minCellCount) {
        sumCount <- -minCellCount
      } else {
        sumCount <- -sumCount
      }
    } else {
      if (sumCount < minCellCount) {
        sumCount <- -minCellCount
      }
    }
    return(sumCount)
  }

  subset <- perDbEstimates$estimates %>%
    inner_join(row, by = c(perDbEstimates$key, "analysisId"))
  llApproximations <- perDbEstimates$llApproximations %>%
    inner_join(row, by = c(perDbEstimates$key, "analysisId"))
  if (analysisSettings$evidenceSynthesisSource$likelihoodApproximation == "normal") {
    llApproximations <- llApproximations %>%
      filter(!is.na(.data$logRr) & !is.na(.data$seLogRr))
    includedDbs <- llApproximations$databaseId
  } else if (analysisSettings$evidenceSynthesisSource$likelihoodApproximation == "adaptive grid") {
    includedDbs <- llApproximations$databaseId
    llApproximations <- llApproximations %>%
      select(point = .data$logRr,
             value = .data$logLikelihood,
             .data$databaseId) %>%
      group_by(.data$databaseId) %>%
      group_split()
  }
  nDatabases <- length(includedDbs)
  subset <- subset %>%
    filter(.data$databaseId %in% includedDbs)
  if (analysisSettings$evidenceSynthesisSource$sourceMethod == "CohortMethod") {
    counts <- tibble(
      targetSubjects = sumMinCellCount(subset$targetSubjects, minCellCount),
      comparatorSubjects = sumMinCellCount(subset$comparatorSubjects, minCellCount),
      targetDays = sumMinCellCount(subset$targetDays, 0),
      comparatorDays = sumMinCellCount(subset$comparatorDays, 0),
      targetOutcomes = sumMinCellCount(subset$targetOutcomes, minCellCount),
      comparatorOutcomes = sumMinCellCount(subset$comparatorOutcomes, minCellCount),
    )
  } else {
    stop(sprintf("Aggregating counts not implemented for source method '%s'", analysisSettings$evidenceSynthesisSource$sourceMethod))
  }

  if (nDatabases == 0) {
    estimate <- tibble(
      rr = NA,
      ci95Lb = NA,
      ci95Ub = NA,
      p = NA,
      logRr = NA,
      seLogRr = NA,
      i2 = NA,
      tau = NA
    )
  } else if (nDatabases == 1) {
    estimate <- tibble(
      rr = exp(subset$logRr),
      ci95Lb = subset$ci95Lb,
      ci95Ub = subset$ci95Ub,
      p = subset$p,
      logRr = subset$logRr,
      seLogRr = subset$seLogRr,
      i2 = NA,
      tau = NA
    )
  } else {
    if (is(analysisSettings, "FixedEffectsMetaAnalysis")) {
      args <- analysisSettings
      args$evidenceSynthesisAnalysisId <- NULL
      args$evidenceSynthesisDescription <- NULL
      args$evidenceSynthesisSource <- NULL
      args$controlType <- NULL
      args$data <- as.data.frame(llApproximations)
      estimate <- do.call(EvidenceSynthesis::computeFixedEffectMetaAnalysis, args)
      z <- estimate$logRr / estimate$seLogRr
      p <- 2 * pmin(pnorm(z), 1 - pnorm(z))
      estimate <- estimate %>%
        as_tibble() %>%
        rename(ci95Lb = lb,
               ci95Ub = ub) %>%
        mutate(i2 = NA,
               tau = NA,
               p = !!p)
    } else if (is(analysisSettings, "RandomEffectsMetaAnalysis")) {
      m <- meta::metagen(TE = llApproximations$logRr,
                         seTE = llApproximations$seLogRr,
                         studlab = rep("", nrow(llApproximations)),
                         byvar = NULL,
                         sm = "RR",
                         level.comb = 1 - analysisSettings$alpha)
      rfx <- summary(m)$random
      estimate <- tibble(
        rr = exp(rfx$TE),
        ci95Lb = exp(rfx$lower),
        ci95Ub = exp(rfx$upper),
        p = rfx$p,
        logRr = rfx$TE,
        seLogRr = rfx$seTE,
        i2 = m$I2,
        tau = NA
      )
    } else if (is(analysisSettings, "BayesianMetaAnalysis")) {
      args <- analysisSettings
      args$evidenceSynthesisAnalysisId <- NULL
      args$evidenceSynthesisDescription <- NULL
      args$evidenceSynthesisSource <- NULL
      args$controlType <- NULL
      args$data <- llApproximations
      estimate <- do.call(EvidenceSynthesis::computeBayesianMetaAnalysis, args)
      z <- estimate$logRr / estimate$seLogRr
      p <- 2 * pmin(pnorm(z), 1 - pnorm(z))
      estimate <- estimate %>%
        as_tibble() %>%
        transmute(rr = exp(.data$mu),
                  ci95Lb = exp(.data$mu95Lb),
                  ci95Ub = exp(.data$mu95Ub),
                  p = !!p,
                  logRr = .data$mu,
                  seLogRr = .data$muSe,
                  tau = .data$tau,
                  i2 = NA)
    }
  }
  estimate <- bind_cols(row, estimate, counts) %>%
    mutate(nDatabases = nDatabases)
  return(estimate)
}

getPerDatabaseEstimates <- function(connection, databaseSchema, evidenceSynthesisSource) {
  if (evidenceSynthesisSource$sourceMethod == "CohortMethod") {
    key <- c("targetId", "comparatorId", "outcomeId")
    databaseIds <- evidenceSynthesisSource$databaseIds
    analysisIds <- evidenceSynthesisSource$analysisIds
    toInclude <- "SELECT target_id,
      comparator_id,
      outcome_id,
      analysis_id,
      database_id
    FROM @database_schema.cm_diagnostics_summary
    WHERE unblind = 1
    {@database_ids != ''} ? {  AND database_id IN (@database_ids)}
    {@analysis_ids != ''} ? {  AND analysis_id IN (@analysis_ids)}"
    toInclude <- SqlRender::render(
      sql = toInclude,
      database_schema = databaseSchema,
      database_ids = if (is.null(databaseIds)) "" else databaseIds,
      analysis_ids = if (is.null(analysisIds)) "" else analysisIds
    )

    sql <- "SELECT cm_result.*
      FROM @database_schema.cm_result
      INNER JOIN (
        @to_include
      ) to_include
      ON cm_result.target_id = to_include.target_id
        AND cm_result.comparator_id = to_include.comparator_id
        AND cm_result.outcome_id = to_include.outcome_id
        AND cm_result.analysis_id = to_include.analysis_id
        AND cm_result.database_id = to_include.database_id;
      "
    estimates <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = sql,
      database_schema = databaseSchema,
      to_include = toInclude,
      snakeCaseToCamelCase = TRUE
    ) %>%
      as_tibble()

    if (evidenceSynthesisSource$likelihoodApproximation == "normal") {
      llApproximations <- estimates %>%
        select(
          .data$targetId,
          .data$comparatorId,
          .data$outcomeId,
          .data$analysisId,
          .data$databaseId,
          .data$logRr,
          .data$seLogRr
        )
    } else if (evidenceSynthesisSource$likelihoodApproximation == "adaptive grid") {
      sql <- "SELECT cm_likelihood_profile.*
      FROM @database_schema.cm_likelihood_profile
      INNER JOIN (
        @to_include
      ) to_include
      ON cm_likelihood_profile.target_id = to_include.target_id
        AND cm_likelihood_profile.comparator_id = to_include.comparator_id
        AND cm_likelihood_profile.outcome_id = to_include.outcome_id
        AND cm_likelihood_profile.analysis_id = to_include.analysis_id
        AND cm_likelihood_profile.database_id = to_include.database_id;
      "
      llApproximations <- DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = sql,
        database_schema = databaseSchema,
        to_include = toInclude,
        snakeCaseToCamelCase = TRUE
      )
    } else {
      stop(sprintf("Unknown likelihood approximation '%s'.", evidenceSynthesisSource$likelihoodApproximation))
    }
    sql <- "SELECT *
      FROM @database_schema.cm_target_comparator_outcome;
    "
    trueEffectSizes <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = sql,
      database_schema = databaseSchema,
      snakeCaseToCamelCase = TRUE
    )
  } else {
    stop(sprintf("Evidence synthesis for source method '%s' hasn't been implemented yet.", evidenceSynthesisSource$sourceMethod))
  }
  return(list(
    key = key,
    estimates = estimates,
    llApproximations = llApproximations,
    trueEffectSizes = trueEffectSizes
  ))
}

writeToCsv <- function(data, fileName, append) {
  data <- SqlRender::camelCaseToSnakeCaseNames(data)
  readr::write_csv(data, fileName, append = append)
}
