#' Create an evidence synthesis source
#'
#' @param sourceMethod            The source method generating the estimates to synthesize. Can be "CohortMethod" or
#'                                "SelfControlledCaseSeries"
#' @param databaseIds             The database  IDs to include. Use `databaseIds = NULL` to include all database IDs.
#' @param likelihoodApproximation The type of likelihood approximation. Can be "adaptive grid" or "normal".
#'
#' @return
#' An object of type `EvidenceSynthesisSource`.
#'
#' @export
createEvidenceSynthesisSource <- function(sourceMethod = "CohortMethod",
                                          databaseIds = NULL,
                                          likelihoodApproximation = "adaptive grid") {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertChoice(sourceMethod, c("CohortMethod", "SelfControlledCaseSeries"), add = errorMessages)
  checkmate::assertChoice(likelihoodApproximation, c("adaptive grid", "normal"), add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)

  analysis <- list()
  for (name in names(formals(createEvidenceSynthesisSource))) {
    analysis[[name]] <- get(name)
  }
  class(analysis) <- "EvidenceSynthesisSource"
  return(analysis)
}

#' Create specifications for the EvidenceSynthesisModule
#'
#' @param evidenceSynthesisAnalysisList A list of objects of type "EvidenceSynthesisAnalysis" as generated
#'                                      by either the [createFixedEffectsMetaAnalysis()] or
#'                                      [createBayesianMetaAnalysis()] function.
#'
#' @return
#' An object of type `EvidenceSynthesisModuleSpecifications`.
#'
#' @export
createEvidenceSynthesisModuleSpecifications <- function(evidenceSynthesisAnalysisList) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertList(evidenceSynthesisAnalysisList, min.len = 1, add = errorMessages)
  for (i in 1:length(evidenceSynthesisAnalysisList)) {
    checkmate::assertClass(evidenceSynthesisAnalysisList[[i]], "EvidenceSynthesisAnalysis", add = errorMessages)
  }
  checkmate::reportAssertions(collection = errorMessages)

  specifications <- list(settings = evidenceSynthesisAnalysisList,
                         module = "%module%",
                         version = "%version%",
                         remoteRepo = "github.com",
                         remoteUsername = "ohdsi")
  class(specifications) <- c("EvidenceSynthesisModuleSpecifications", "ModuleSpecifications")
  return(specifications)
}
