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

library(dplyr)

writeAnalysisSpecs <- function(analysisSpecs, resultsFolder) {
  message("Writing evidence synthesis analysis specifications")
  tempFileName <- tempfile()
  evidenceSynthesisAnalysis <- tibble()
  for (analysisSettings in analysisSpecs) {
    ParallelLogger::saveSettingsToJson(analysisSettings, tempFileName)
    analysis <- tibble(
      evidenceSynthesisAnalysisId = analysisSettings$evidenceSynthesisAnalysisId,
      evidenceSynthesisDescription = analysisSettings$evidenceSynthesisDescription,
      sourceMethod = analysisSettings$evidenceSynthesisSource$sourceMethod,
    ) %>%
      mutate(definition = readChar(tempFileName, file.info(tempFileName)$size))
    evidenceSynthesisAnalysis <- bind_rows(evidenceSynthesisAnalysis, analysis)
  }
  unlink(tempFileName)
  fileName <- file.path(resultsFolder, "es_analysis.csv")
  CohortGenerator::writeCsv(evidenceSynthesisAnalysis, fileName)
}

ensureEmptyAndExists <- function(outputTable, resultsFolder) {
  diagnostics <- createEmptyResult(outputTable)
  fileName <- file.path(resultsFolder, paste0(outputTable, ".csv"))
  writeToCsv(data = diagnostics, fileName = fileName, append = FALSE)
}

executeEvidenceSynthesis <- function(connectionDetails, databaseSchema, settings, esDiagnosticThresholds, resultsFolder, minCellCount) {
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  outputTables <- c(
    "es_cm_result",
    "es_cm_diagnostics_summary",
    "es_sccs_result",
    "es_sccs_diagnostics_summary"
  )
  invisible(lapply(outputTables, function(x) {
    ensureEmptyAndExists(x, resultsFolder)
  }))

  message("Performing evidence synthesis across databases")
  invisible(lapply(
    X = settings,
    FUN = doAnalysis,
    connection = connection,
    databaseSchema = databaseSchema,
    resultsFolder = resultsFolder,
    minCellCount = minCellCount,
    esDiagnosticThresholds = esDiagnosticThresholds
  ))
}

# analysisSettings = settings[[4]]
doAnalysis <- function(analysisSettings, connection, databaseSchema, resultsFolder, minCellCount, esDiagnosticThresholds) {
  perDbEstimates <- getPerDatabaseEstimates(
    connection = connection,
    databaseSchema = databaseSchema,
    evidenceSynthesisSource = analysisSettings$evidenceSynthesisSource
  )
  if (nrow(perDbEstimates$estimates) == 0) {
    message <- sprintf(
      "No unblinded estimates found for source method '%s'",
      analysisSettings$evidenceSynthesisSource$sourceMethod
    )
    if (!is.null(analysisSettings$evidenceSynthesisSource$databaseIds)) {
      message <- sprintf(
        "%s restricting to database IDs %s",
        message,
        paste(analysisSettings$evidenceSynthesisSource$databaseIds, collapse = ", ")
      )
    }
    if (!is.null(analysisSettings$evidenceSynthesisSource$analysisIds)) {
      message <- sprintf(
        "%s restricting to analysis IDs %s",
        message,
        paste(analysisSettings$evidenceSynthesisSource$analysisIds, collapse = ", ")
      )
    }
    warning(message)
    return()
  }

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
    minCellCount = minCellCount
  )
  estimates <- bind_rows(estimates)

  message("- Calibrating estimates")
  estimates <- estimates %>%
    inner_join(perDbEstimates$trueEffectSizes, by = intersect(names(estimates), names(perDbEstimates$trueEffectSizes)))
  if (analysisSettings$controlType == "outcome") {
    if (analysisSettings$evidenceSynthesisSource$sourceMethod == "CohortMethod") {
      controlKey <- c("targetId", "comparatorId", "analysisId")
    } else if (analysisSettings$evidenceSynthesisSource$sourceMethod == "SelfControlledCaseSeries") {
      controlKey <- c("covariateId", "analysisId")
    }
  } else if (analysisSettings$controlType == "exposure") {
    if (analysisSettings$evidenceSynthesisSource$sourceMethod == "CohortMethod") {
      controlKey <- c("outcomeId", "analysisId")
    } else if (analysisSettings$evidenceSynthesisSource$sourceMethod == "SelfControlledCaseSeries") {
      controlKey <- c("exposuresOutcomeSetId", "analysisId")
    }
  } else {
    stop(sprintf("Unknown control type '%s'", analysisSettings$controlType))
  }
  groupKeys <- estimates[, controlKey]
  groupKeys <- apply(groupKeys, 1, paste, collapse = "_")

  estimates <- ParallelLogger::clusterApply(
    cluster = cluster,
    x = split(estimates, groupKeys),
    fun = calibrateEstimates
  )
  estimates <- bind_rows(estimates) %>%
    mutate(evidenceSynthesisAnalysisId = analysisSettings$evidenceSynthesisAnalysisId)

  # Save diagnostics
  diagnostics <- estimates[, c(perDbEstimates$key, "analysisId", "evidenceSynthesisAnalysisId", "mdrr", "ease", "i2", "tau")] %>%
    mutate(mdrrDiagnostic = case_when(
      is.na(.data$mdrr) ~ "NOT EVALUATED",
      .data$mdrr < esDiagnosticThresholds$mdrrThreshold ~ "PASS",
      TRUE ~ "FAIL"
    )) %>%
    mutate(easeDiagnostic = case_when(
      is.na(.data$ease) ~ "NOT EVALUATED",
      abs(.data$ease) < esDiagnosticThresholds$easeThreshold ~ "PASS",
      TRUE ~ "FAIL"
    )) %>%
    mutate(i2Diagnostic = case_when(
      is.na(.data$i2) ~ "NOT EVALUATED",
      abs(.data$i2) < esDiagnosticThresholds$i2Threshold ~ "PASS",
      TRUE ~ "FAIL"
    )) %>%
    mutate(tauDiagnostic = case_when(
      is.na(.data$tau) ~ "NOT EVALUATED",
      abs(.data$tau) < esDiagnosticThresholds$tauThreshold ~ "PASS",
      TRUE ~ "FAIL"
    )) %>%
    mutate(unblind = ifelse(.data$mdrrDiagnostic != "FAIL" &
      .data$easeDiagnostic != "FAIL" &
      .data$i2Diagnostic != "FAIL" &
      .data$tauDiagnostic != "FAIL", 1, 0))
  if (analysisSettings$evidenceSynthesisSource$sourceMethod == "CohortMethod") {
    fileName <- file.path(resultsFolder, "es_cm_diagnostics_summary.csv")
  } else if (analysisSettings$evidenceSynthesisSource$sourceMethod == "SelfControlledCaseSeries") {
    fileName <- file.path(resultsFolder, "es_sccs_diagnostics_summary.csv")
  } else {
    stop(sprintf("Saving diagnostics summary not implemented for source method '%s'", analysisSettings$evidenceSynthesisSource$sourceMethod))
  }
  writeToCsv(data = diagnostics, fileName = fileName, append = TRUE)

  # Save estimates
  estimates <- estimates %>%
    select(-"trueEffectSize", -"ease", -"i2", -"tau", -"mdrr")
  if (analysisSettings$evidenceSynthesisSource$sourceMethod == "CohortMethod") {
    estimates <- estimates %>%
      select(-"outcomeOfInterest")
    fileName <- file.path(resultsFolder, "es_cm_result.csv")
  } else if (analysisSettings$evidenceSynthesisSource$sourceMethod == "SelfControlledCaseSeries") {
    fileName <- file.path(resultsFolder, "es_sccs_result.csv")
  } else {
    stop(sprintf("Saving results not implemented for source method '%s'", analysisSettings$evidenceSynthesisSource$sourceMethod))
  }
  writeToCsv(data = estimates, fileName = fileName, append = TRUE)
}

# group = split(estimates, groupKeys)[[1]]
calibrateEstimates <- function(group) {
  ncs <- group[group$trueEffectSize == 1 & !is.na(group$seLogRr), ]
  pcs <- group[!is.na(group$trueEffectSize) & group$trueEffectSize != 1 & !is.na(group$seLogRr), ]
  if (nrow(ncs) >= 5) {
    null <- EmpiricalCalibration::fitMcmcNull(logRr = ncs$logRr, seLogRr = ncs$seLogRr)
    ease <- EmpiricalCalibration::computeExpectedAbsoluteSystematicError(null)
    calibratedP <- EmpiricalCalibration::calibrateP(
      null = null,
      logRr = group$logRr,
      seLogRr = group$seLogRr,
      twoSided = TRUE
    )
    calibratedOneSidedP <- EmpiricalCalibration::calibrateP(
      null = null,
      logRr = group$logRr,
      seLogRr = group$seLogRr,
      twoSided = FALSE,
      upper = TRUE
    )
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
    group$calibratedOneSidedP <- calibratedOneSidedP$p
    group$calibratedLogRr <- calibratedCi$logRr
    group$calibratedSeLogRr <- calibratedCi$seLogRr
    group$ease <- ease$ease
  } else {
    group$calibratedRr <- NA
    group$calibratedCi95Lb <- NA
    group$calibratedCi95Ub <- NA
    group$calibratedP <- NA
    group$calibratedOneSidedP <- NA
    group$calibratedLogRr <- NA
    group$calibratedSeLogRr <- NA
    group$ease <- NA
  }
  return(group)
}

# row <- split(fullKeys, seq_len(nrow(fullKeys)))[[2]]
# row <- tibble(targetId = 8413, comparatorId = 8436, outcomeId = 1078, analysisId = 2)
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
  computeMdrrFromSe <- function(seLogRr, alpha = 0.05, power = 0.8) {
    # Based on the computation of a two-sided p-value, power can be computed as
    # power = 1-pnorm(qnorm(1 - alpha/2) - (log(mdrr) / seLogRr))/2
    # That can be translated in into:
    mdrr <- exp((qnorm(1 - alpha / 2) - qnorm(2 * (1 - power))) * seLogRr)
    return(mdrr)
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
    includedDbs <- unique(llApproximations$databaseId)
    llApproximations <- llApproximations %>%
      select(
        point = .data$logRr,
        value = .data$logLikelihood,
        .data$databaseId
      ) %>%
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
  } else if (analysisSettings$evidenceSynthesisSource$sourceMethod == "SelfControlledCaseSeries") {
    counts <- tibble(
      outcomeSubjects = sumMinCellCount(subset$outcomeSubjects, minCellCount),
      outcomeEvents = sumMinCellCount(subset$outcomeEvents, minCellCount),
      outcomeObservationPeriods = sumMinCellCount(subset$outcomeObservationPeriods, 0),
      observedDays = sumMinCellCount(subset$observedDays, 0),
      covariateSubjects = sumMinCellCount(subset$covariateSubjects, minCellCount),
      covariateDays = sumMinCellCount(subset$covariateDays, minCellCount),
      covariateEras = sumMinCellCount(subset$covariateEras, minCellCount),
      covariateOutcomes = sumMinCellCount(subset$covariateOutcomes, minCellCount)
    )
  } else {
    stop(sprintf("Aggregating counts not implemented for source method '%s'", analysisSettings$evidenceSynthesisSource$sourceMethod))
  }

  if (nDatabases == 0) {
    estimate <- tibble(
      rr = as.numeric(NA),
      ci95Lb = as.numeric(NA),
      ci95Ub = as.numeric(NA),
      p = as.numeric(NA),
      oneSidedP = as.numeric(NA),
      logRr = as.numeric(NA),
      seLogRr = as.numeric(NA),
      i2 = as.numeric(NA),
      tau = as.numeric(NA),
      mdrr = as.numeric(Inf)
    )
  } else if (nDatabases == 1) {
    estimate <- tibble(
      rr = exp(subset$logRr),
      ci95Lb = subset$ci95Lb,
      ci95Ub = subset$ci95Ub,
      p = subset$p,
      oneSidedP = if ("oneSidedP" %in% colnames(subset)) subset$oneSidedP else NA,
      logRr = subset$logRr,
      seLogRr = subset$seLogRr,
      i2 = NA,
      tau = NA,
      mdrr = subset$mdrr
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
      p <- EmpiricalCalibration::computeTraditionalP(
        logRr = estimate$logRr,
        seLogRr = estimate$seLogRr,
        twoSided = TRUE
      )
      oneSidedP <- EmpiricalCalibration::computeTraditionalP(
        logRr = estimate$logRr,
        seLogRr = estimate$seLogRr,
        twoSided = FALSE,
        upper = TRUE
      )
      estimate <- estimate %>%
        as_tibble() %>%
        rename(
          ci95Lb = lb,
          ci95Ub = ub
        ) %>%
        mutate(
          i2 = NA,
          tau = NA,
          mdrr = computeMdrrFromSe(estimate$seLogRr),
          p = !!p,
          oneSidedP = !!oneSidedP
        )
    } else if (is(analysisSettings, "RandomEffectsMetaAnalysis")) {
      m <- meta::metagen(
        TE = llApproximations$logRr,
        seTE = llApproximations$seLogRr,
        studlab = rep("", nrow(llApproximations)),
        byvar = NULL,
        control = list(maxiter = 1000),
        sm = "RR",
        level.comb = 1 - analysisSettings$alpha
      )
      rfx <- summary(m)$random
      oneSidedP <- EmpiricalCalibration::computeTraditionalP(
        logRr = rfx$TE,
        seLogRr = rfx$seTE,
        twoSided = FALSE,
        upper = TRUE
      )
      estimate <- tibble(
        rr = exp(rfx$TE),
        ci95Lb = exp(rfx$lower),
        ci95Ub = exp(rfx$upper),
        p = rfx$p,
        oneSidedP = !!oneSidedP,
        logRr = rfx$TE,
        seLogRr = rfx$seTE,
        i2 = m$I2,
        tau = NA,
        mdrr = computeMdrrFromSe(rfx$seTE)
      )
    } else if (is(analysisSettings, "BayesianMetaAnalysis")) {
      args <- analysisSettings
      args$evidenceSynthesisAnalysisId <- NULL
      args$evidenceSynthesisDescription <- NULL
      args$evidenceSynthesisSource <- NULL
      args$controlType <- NULL
      args$data <- llApproximations
      estimate <- do.call(EvidenceSynthesis::computeBayesianMetaAnalysis, args)
      p <- EmpiricalCalibration::computeTraditionalP(
        logRr = estimate$logRr,
        seLogRr = estimate$seLogRr,
        twoSided = TRUE
      )
      oneSidedP <- EmpiricalCalibration::computeTraditionalP(
        logRr = estimate$logRr,
        seLogRr = estimate$seLogRr,
        twoSided = FALSE,
        upper = TRUE
      )
      estimate <- estimate %>%
        as_tibble() %>%
        transmute(
          rr = exp(.data$mu),
          ci95Lb = exp(.data$mu95Lb),
          ci95Ub = exp(.data$mu95Ub),
          p = !!p,
          oneSidedP = !!oneSidedP,
          logRr = .data$mu,
          seLogRr = .data$muSe,
          tau = .data$tau,
          i2 = NA,
          mdrr = computeMdrrFromSe(estimate$seLogRr)
        )
    }
  }
  estimate <- bind_cols(row, estimate, counts) %>%
    mutate(nDatabases = nDatabases)
  return(estimate)
}

hasUnblindForEvidenceSynthesisColumn <- function(connection, databaseSchema, table) {
  row <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT TOP 1 * FROM @database_schema.@table;",
    database_schema = databaseSchema,
    table = table,
    snakeCaseToCamelCase = TRUE
  )
  return("unlindForEvidenceSynthesis" %in% colnames(row))
}

getPerDatabaseEstimates <- function(connection, databaseSchema, evidenceSynthesisSource) {
  if (evidenceSynthesisSource$sourceMethod == "CohortMethod") {
    key <- c("targetId", "comparatorId", "outcomeId")
    databaseIds <- evidenceSynthesisSource$databaseIds
    analysisIds <- evidenceSynthesisSource$analysisIds
    if (hasUnblindForEvidenceSynthesisColumn(connection, databaseSchema, "cm_diagnostics_summary")) {
      unblindColumn <- "unblind_for_evidence_synthesis"
    } else {
      unblindColumn <- "unblind"
    }
    # For backwards compatibility, when CohortMethod did not generate diagnostics
    # for negative controls: if negative control (outcome_of_interest = 0) then
    # still unblind.
    sql <- "SELECT cm_result.*,
        mdrr,
        CASE
          WHEN @unblind_column IS NULL THEN 1 - outcome_of_interest
          ELSE @unblind_column
        END AS unblind
      FROM @database_schema.cm_result
      INNER JOIN @database_schema.cm_target_comparator_outcome
        ON cm_result.target_id = cm_target_comparator_outcome.target_id
          AND cm_result.comparator_id = cm_target_comparator_outcome.comparator_id
          AND cm_result.outcome_id = cm_target_comparator_outcome.outcome_id
      LEFT JOIN @database_schema.cm_diagnostics_summary
      ON cm_result.target_id = cm_diagnostics_summary.target_id
        AND cm_result.comparator_id = cm_diagnostics_summary.comparator_id
        AND cm_result.outcome_id = cm_diagnostics_summary.outcome_id
        AND cm_result.analysis_id = cm_diagnostics_summary.analysis_id
        AND cm_result.database_id = cm_diagnostics_summary.database_id
      {@database_ids != ''| @analysis_ids != ''} ? {WHERE}
      {@database_ids != ''} ? {  cm_result.database_id IN (@database_ids)}
      {@analysis_ids != ''} ? {  {@database_ids != ''} ? {AND} cm_result.analysis_id IN (@analysis_ids)};
      "
    estimates <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = sql,
      database_schema = databaseSchema,
      unblind_column = unblindColumn,
      database_ids = if (is.null(databaseIds)) "" else quoteSql(databaseIds),
      analysis_ids = if (is.null(analysisIds)) "" else analysisIds,
      snakeCaseToCamelCase = TRUE
    ) %>%
      as_tibble()

    # Temp hack: detect NA values that have been converted to 0 in the DB:
    idx <- estimates$seLogRr == 0
    estimates$logRr[idx] <- NA
    estimates$seLogRr[idx] <- NA
    estimates$p[idx] <- NA

    if (evidenceSynthesisSource$likelihoodApproximation == "normal") {
      llApproximations <- estimates %>%
        filter(.data$unblind == 1) %>%
        select(
          "targetId",
          "comparatorId",
          "outcomeId",
          "analysisId",
          "databaseId",
          "logRr",
          "seLogRr"
        )
    } else if (evidenceSynthesisSource$likelihoodApproximation == "adaptive grid") {
      sql <- "SELECT cm_likelihood_profile.*
      FROM @database_schema.cm_likelihood_profile
      WHERE log_likelihood IS NOT NULL
      {@database_ids != ''} ? {  AND cm_likelihood_profile.database_id IN (@database_ids)}
      {@analysis_ids != ''} ? {  AND cm_likelihood_profile.analysis_id IN (@analysis_ids)};
      "
      llApproximations <- DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = sql,
        database_schema = databaseSchema,
        database_ids = if (is.null(databaseIds)) "" else quoteSql(databaseIds),
        analysis_ids = if (is.null(analysisIds)) "" else analysisIds,
        snakeCaseToCamelCase = TRUE
      ) %>%
        inner_join(
          estimates %>%
            filter(.data$unblind == 1) %>%
            select(
              "targetId",
              "comparatorId",
              "outcomeId",
              "analysisId",
              "databaseId",
            ),
          by = c("targetId", "comparatorId", "outcomeId", "analysisId", "databaseId")
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
    trueEffectSizes <- trueEffectSizes %>%
      mutate(trueEffectSize = ifelse(!is.na(.data$trueEffectSize) & .data$trueEffectSize == 0,
        NA,
        .data$trueEffectSize
      ))
  } else if (evidenceSynthesisSource$sourceMethod == "SelfControlledCaseSeries") {
    key <- c("exposuresOutcomeSetId", "covariateId")
    databaseIds <- evidenceSynthesisSource$databaseIds
    analysisIds <- evidenceSynthesisSource$analysisIds
    if (hasUnblindForEvidenceSynthesisColumn(connection, databaseSchema, "sccs_diagnostics_summary")) {
      unblindColumn <- "unblind_for_evidence_synthesis"
    } else {
      unblindColumn <- "unblind"
    }
    sql <- "SELECT sccs_result.*,
        mdrr,
        CASE
          WHEN @unblind_column IS NULL THEN CASE WHEN true_effect_size IS NULL THEN 0 ELSE 1 END
          ELSE @unblind_column
        END AS unblind
      FROM @database_schema.sccs_result
      INNER JOIN @database_schema.sccs_covariate
        ON sccs_result.database_id = sccs_covariate.database_id
          AND sccs_result.exposures_outcome_set_id = sccs_covariate.exposures_outcome_set_id
          AND sccs_result.covariate_id = sccs_covariate.covariate_id
          AND sccs_result.analysis_id = sccs_covariate.analysis_id
      INNER JOIN @database_schema.sccs_exposure
        ON sccs_result.exposures_outcome_set_id = sccs_exposure.exposures_outcome_set_id
          AND sccs_covariate.era_id = sccs_covariate.era_id
      LEFT JOIN @database_schema.sccs_diagnostics_summary
      ON sccs_result.exposures_outcome_set_id = sccs_diagnostics_summary.exposures_outcome_set_id
        AND sccs_result.covariate_id = sccs_diagnostics_summary.covariate_id
        AND sccs_result.analysis_id = sccs_diagnostics_summary.analysis_id
        AND sccs_result.database_id = sccs_diagnostics_summary.database_id
      {@database_ids != ''| @analysis_ids != ''} ? {WHERE}
      {@database_ids != ''} ? {  sccs_result.database_id IN (@database_ids)}
      {@analysis_ids != ''} ? {  {@database_ids != ''} ? {AND} sccs_result.analysis_id IN (@analysis_ids)};
      "
    estimates <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = sql,
      database_schema = databaseSchema,
      unblind_column = unblindColumn,
      database_ids = if (is.null(databaseIds)) "" else quoteSql(databaseIds),
      analysis_ids = if (is.null(analysisIds)) "" else analysisIds,
      snakeCaseToCamelCase = TRUE
    ) %>%
      as_tibble()

    # Temp hack: detect NA values that have been converted to 0 in the DB:
    idx <- estimates$seLogRr == 0
    estimates$logRr[idx] <- NA
    estimates$seLogRr[idx] <- NA
    estimates$p[idx] <- NA

    if (evidenceSynthesisSource$likelihoodApproximation == "normal") {
      llApproximations <- estimates %>%
        filter(.data$unblind == 1) %>%
        select(
          "exposuresOutcomeSetId",
          "covariateId",
          "analysisId",
          "databaseId",
          "logRr",
          "seLogRr"
        )
    } else if (evidenceSynthesisSource$likelihoodApproximation == "adaptive grid") {
      sql <- "SELECT sccs_likelihood_profile.*
      FROM @database_schema.sccs_likelihood_profile
      WHERE log_likelihood IS NOT NULL
      {@database_ids != ''} ? {  AND sccs_likelihood_profile.database_id IN (@database_ids)}
      {@analysis_ids != ''} ? {  AND sccs_likelihood_profile.analysis_id IN (@analysis_ids)};
      "
      llApproximations <- DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = sql,
        database_schema = databaseSchema,
        database_ids = if (is.null(databaseIds)) "" else quoteSql(databaseIds),
        analysis_ids = if (is.null(analysisIds)) "" else analysisIds,
        snakeCaseToCamelCase = TRUE
      ) %>%
        inner_join(
          estimates %>%
            filter(.data$unblind == 1) %>%
            select(
              "exposuresOutcomeSetId",
              "covariateId",
              "analysisId",
              "databaseId",
            ),
          by = c("exposuresOutcomeSetId", "covariateId", "analysisId", "databaseId")
        )
    } else {
      stop(sprintf("Unknown likelihood approximation '%s'.", evidenceSynthesisSource$likelihoodApproximation))
    }
    sql <- "SELECT DISTINCT sccs_covariate.analysis_id,
            sccs_covariate.exposures_outcome_set_id,
            sccs_covariate.covariate_id,
            true_effect_size
          FROM @database_schema.sccs_exposure
          INNER JOIN @database_schema.sccs_covariate
            ON sccs_exposure.era_id = sccs_covariate.era_id
              AND sccs_exposure.exposures_outcome_set_id = sccs_covariate.exposures_outcome_set_id
          INNER JOIN @database_schema.sccs_covariate_analysis
            ON sccs_covariate.analysis_id = sccs_covariate_analysis.analysis_id
              AND sccs_covariate.covariate_analysis_id = sccs_covariate_analysis.covariate_analysis_id
          WHERE sccs_covariate_analysis.variable_of_interest = 1;
    "
    trueEffectSizes <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = sql,
      database_schema = databaseSchema,
      snakeCaseToCamelCase = TRUE
    )
    trueEffectSizes <- trueEffectSizes %>%
      mutate(trueEffectSize = ifelse(!is.na(.data$trueEffectSize) & .data$trueEffectSize == 0,
        NA,
        .data$trueEffectSize
      ))
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
  tableName <- gsub(".csv$", "", basename(fileName))
  names <- colnames(createEmptyResult(tableName))
  data <- data[, names]
  data <- SqlRender::camelCaseToSnakeCaseNames(data)
  readr::write_csv(data, fileName, append = append)
}

createEmptyResult <- function(tableName = "") {
  columns <- readr::read_csv(
    file = "resultsDataModelSpecification.csv",
    show_col_types = FALSE
  ) %>%
    SqlRender::snakeCaseToCamelCaseNames() %>%
    filter(.data$tableName == !!tableName) %>%
    pull(.data$columnName) %>%
    SqlRender::snakeCaseToCamelCase()
  result <- vector(length = length(columns))
  names(result) <- columns
  result <- as_tibble(t(result), name_repair = "check_unique")
  result <- result[FALSE, ]
  return(result)
}

quoteSql <- function(values) {
  return(paste0("'", paste(values, collapse = "', '"), "'"))
}
