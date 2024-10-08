EvidenceSynthesisModule 0.6.2
=============================

1. Fixing grouping of negative outcome controls when there are multiple exposures (and/or multiple indications).

EvidenceSynthesisModule 0.6.1
=============================

1. Restore SettingsFunction.R file from v0.5.1.

EvidenceSynthesisModule 0.6.0
=============================

1. Using renv project profiles to manage core packages required for module execution vs. those that are needed for development purposes.

EvidenceSynthesisModule 0.5.1
=============================

1. Minor bugfix (increasing VARCHAR size for diagnostics to contain "NOT EVALUATED").

EvidenceSynthesisModule 0.5.0
=============================

1. Adding Minimum Detectable Relative Risk (MDRR) as a diagnostic.

2. Adding `createEsDiagnosticThresholds()` function for specifying diagnostics thresholds.

3. Removing WARNING as possible output for diagnostics, adding NOT EVALUATED.


EvidenceSynthesisModule 0.4.0
=============================

1. Include diagnostics and estimates for analyses that are blinded for all databases.

2. Respect unblind decisions for negative controls.

3. Use new `unblind_for_evidence_synthesis` column in diagnostics summary tables (when present).


EvidenceSynthesisModule 0.3.0
=============================

1. Adding one-sided (calibrated) p-values.


EvidenceSynthesisModule 0.2.1
=============================

Bugfixes

1. Fixed issue where blinded results would still be included in meta-analysis for cohort method when using adaptive grid approximation.


EvidenceSynthesisModule 0.2.0
=============================

- Updated module to use HADES wide lock file and updated to use renv v1.0.2
- Added functions and tests for creating the results data model for use by Strategus upload functionality
- Added additional GitHub Action tests to unit test the module functionality on HADES supported R version (v4.2.3) and the latest release of R

EvidenceSynthesisModule 0.1.3
=============================

Bugfixes

1. Extend size of `es_analysis.source_method` in results data model specification


EvidenceSynthesisModule 0.1.2
=============================

Bugfixes

1. Added missing `resultsFolder` argument to private function.

EvidenceSynthesisModule 0.1.1
=============================

Bugfixes

1. Added missing `renv/settings.dcf` file.

EvidenceSynthesisModule 0.1.0
=============================

Changes

1. Adding synthesis of SCCS results.

EvidenceSynthesisModule 0.0.4
=============================

Bugfixes

1. Adding `evidence_synthesis_analysis_id` to primary key of `es_cm_result` to avoid PK violations.

EvidenceSynthesisModule 0.0.3
=============================

Bugfixes

1. Also setting version in `SettingsFunctions.R`

EvidenceSynthesisModule 0.0.2
=============================

Bugfixes

1. Was not extracting negative control estimates from results database.

2. Fixing miscounting of number of databases when using adaptive grid approximation.

3. Added temporary fix for NAs set to 0 in per-database estimates in the results database.

4. Fixed error about Fisher scoring algorithm not converging.


EvidenceSynthesisModule 0.0.1
=============================

- Initial version
