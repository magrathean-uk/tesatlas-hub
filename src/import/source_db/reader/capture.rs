// SPDX-License-Identifier: AGPL-3.0-only

fn discard_stage_after_error(
    stage: SourceDbStage,
    primary: SourceDbReaderError,
) -> SourceDbReaderError {
    match stage.discard() {
        Ok(()) => primary,
        Err(error) => SourceDbReaderError::StageCleanupFailure {
            primary: Box::new(primary),
            cleanup: SourceDbStageCleanupFailureKind::from(&error),
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceDbStageCleanupFailureKind {
    MissingOrChanged,
    UnsafePath,
    RemoveFailed,
    Other,
}

impl From<&SourceDbStageError> for SourceDbStageCleanupFailureKind {
    fn from(error: &SourceDbStageError) -> Self {
        match error {
            SourceDbStageError::InspectPath { .. }
            | SourceDbStageError::StagePathIdentityChanged(_)
            | SourceDbStageError::DirectoryIdentityChanged(_) => Self::MissingOrChanged,
            SourceDbStageError::UnexpectedLinkCount { actual: 0, .. } => Self::MissingOrChanged,
            SourceDbStageError::SymlinkPath(_)
            | SourceDbStageError::ExpectedDirectory(_)
            | SourceDbStageError::ExpectedFile(_)
            | SourceDbStageError::UnexpectedLinkCount { .. }
            | SourceDbStageError::UnexpectedOwner { .. }
            | SourceDbStageError::InsecurePermissions { .. }
            | SourceDbStageError::InvalidStagePath => Self::UnsafePath,
            SourceDbStageError::RemoveStage { .. } => Self::RemoveFailed,
            _ => Self::Other,
        }
    }
}

impl std::fmt::Display for SourceDbStageCleanupFailureKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::MissingOrChanged => "missing-or-changed",
            Self::UnsafePath => "unsafe-path",
            Self::RemoveFailed => "remove-failed",
            Self::Other => "other",
        })
    }
}

#[cfg(test)]
mod n1_src_import_source_db_reader_capture_rs_display_tests {
    use super::*;
    use crate::runtime::name_test_support::{ErrorExample, assert_name_free};

    impl ErrorExample for SourceDbStageCleanupFailureKind {
        fn error_example() -> Self {
            SourceDbStageCleanupFailureKind::MissingOrChanged
        }
    }

    #[test]
    fn every_reachable_error_display_omits_the_source_project_name() {
        fn check_source_db_stage_cleanup_failure_kind(error: SourceDbStageCleanupFailureKind) {
            match &error {
                SourceDbStageCleanupFailureKind::MissingOrChanged => (),
                SourceDbStageCleanupFailureKind::UnsafePath => (),
                SourceDbStageCleanupFailureKind::RemoveFailed => (),
                SourceDbStageCleanupFailureKind::Other => (),
            }
            assert_name_free(&error);
        }
        check_source_db_stage_cleanup_failure_kind(
            SourceDbStageCleanupFailureKind::MissingOrChanged,
        );
        check_source_db_stage_cleanup_failure_kind(SourceDbStageCleanupFailureKind::UnsafePath);
        check_source_db_stage_cleanup_failure_kind(SourceDbStageCleanupFailureKind::RemoveFailed);
        check_source_db_stage_cleanup_failure_kind(SourceDbStageCleanupFailureKind::Other);
    }
}
