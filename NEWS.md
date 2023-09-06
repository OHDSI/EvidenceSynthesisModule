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
